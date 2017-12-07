#include <iostream>
#include <cstdio>
#include <cassert>
#include <ctime>

#include "driver_program.h"
#include "fast_random.h"

void Populate(pqxx::connection &conn, const DriverConfig &config) {

  size_t table_size = config.default_table_size_ * config.scale_factor_;
  
  std::cout << ">>>>> Populate table \'employee\'. " << std::endl
            << "   -- Build index? : " << config.with_index_ << std::endl
            << "   -- Table size   : " << table_size << std::endl;

  pqxx::work txn(conn);

  txn.exec("DROP TABLE IF EXISTS employee;");
  txn.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");

  // std::cout << "before create index" << std::endl;
  
  if (config.with_index_ == true) {
    // std::cout << "in create index" << std::endl;
    txn.exec("CREATE INDEX emp_index ON employee(id)");
  }

  // std::cout << "after create index" << std::endl;

  for (size_t i = 0; i < table_size; ++i) {
    txn.exec("INSERT INTO employee VALUES (" + std::to_string(i) + ", 'a');");
    // std::cout << "after insert " <<  std::to_string(i) << std::endl;
  }
  txn.commit();
}

void ProcessClient(pqxx::connection &conn, const DriverConfig &config) {
  
  srand(time(NULL));

  size_t table_size = config.default_table_size_ * config.scale_factor_;

  FastRandom fast_rand;

  ZipfDistribution zipf(table_size, config.zipf_theta_);

  std::cout << ">>>>> Process transactions via client interface. " << std::endl
            << "   -- With prepared statement? : " << config.with_prep_stmt_ << std::endl
            << "   -- Table size               : " << table_size << std::endl
            << "   -- Operation count          : " << config.operation_count_ << std::endl
            << "   -- Update ratio             : " << config.update_ratio_ << std::endl
            << "   -- Zipf theta               : " << config.zipf_theta_ << std::endl;
  
  pqxx::work txn(conn);

  if (config.with_prep_stmt_ == true) {

    conn.prepare("read", "SELECT name FROM employee WHERE id=$1");
    conn.prepare("write", "UPDATE employee SET name = 'z' WHERE id=$1");
    
    for (size_t i = 0; i < config.operation_count_; ++i) {
      
      size_t key = zipf.GetNextNumber() - 1;
      
      if (fast_rand.next_uniform() < config.update_ratio_) {
        // update
        txn.prepared("write")(key).exec();
      } else {
        // select
        pqxx::result R = txn.prepared("read")(key).exec();
        printf("key = %lu, txn result set size = %lu\n", key, R.size());
      }
    }
  } else {
    for (size_t i = 0; i < config.operation_count_; ++i) {
      
      size_t key = zipf.GetNextNumber() - 1;
      
      if (fast_rand.next_uniform() < config.update_ratio_) {
        // update
        txn.exec("UPDATE employee SET name = 'z' WHERE id=" + std::to_string(key) + ";");
      } else {
        // select
        pqxx::result R = txn.exec("SELECT name FROM employee WHERE id=" + std::to_string(key) + ";");
        printf("key = %lu, txn result set size = %lu\n", key, R.size());
      }
    }
  }
  txn.commit();
}

void ProcessProcedure(pqxx::connection &conn, const DriverConfig &config) {

  srand(time(NULL));
  
  size_t table_size = config.default_table_size_ * config.scale_factor_;

  std::cout << ">>>>> Process transactions via stored procedure. "
            << "   -- Table size      : " << table_size << std::endl
            << "   -- Operation count : " << config.operation_count_ << std::endl
            << "   -- Update ratio    : " << config.update_ratio_ << std::endl
            << "   -- Zipf theta      : " << config.zipf_theta_ << std::endl;
  
  FastRandom fast_rand;

  size_t read_count = 0;
  bool *is_update = new bool[config.operation_count_];
  for (size_t i = 0; i < config.operation_count_; ++i) {
    if (fast_rand.next_uniform() < config.update_ratio_) {
      is_update[i] = true;
    } else {
      is_update[i] = false;
      ++read_count;
    }
  }

  ZipfDistribution zipf(table_size, config.zipf_theta_);

  std::string func_str("CREATE OR REPLACE FUNCTION ycsb(");

  if (read_count == 0) {
    for (size_t i = 0; i < config.operation_count_ - 1; ++i) {
      func_str += "val" + std::to_string(i) + " integer, ";
    }
    func_str +=  "val" + std::to_string(config.operation_count_ - 1) + " integer) ";
  } else {
    for (size_t i = 0; i < config.operation_count_; ++i) {
      func_str += "val" + std::to_string(i) + " integer, ";
    }
    for (size_t i = 0; i < read_count - 1; ++i) {
      func_str += "ref" + std::to_string(i) + " refcursor, ";
    }
    func_str += "ref" + std::to_string(read_count - 1) + " refcursor) ";
  }

  func_str += "RETURNS ";
  
  if (read_count == 0) {
    func_str += "void ";
  } else if (read_count == 1) {
    func_str += "refcursor ";
  } else {
    func_str += "SETOF refcursor ";
  }
  
  func_str += "AS $$ ";

  func_str += "BEGIN ";

  size_t curr_read_count = 0;
  for (size_t i = 0; i < config.operation_count_; ++i) {
    if (is_update[i] == true) {
      // is update
      func_str += "UPDATE employee SET name = 'z' WHERE id=val" + std::to_string(i) + ";";
    } else {
      // is read
      if (read_count == 1) {
        func_str += "OPEN ref" + std::to_string(curr_read_count) + " FOR SELECT name FROM employee where id = val" + std::to_string(i) + "; RETURN ref" + std::to_string(curr_read_count) + ";";
        ++curr_read_count;
      } else {
        func_str += "OPEN ref" + std::to_string(curr_read_count) + " FOR SELECT name FROM employee where id = val" + std::to_string(i) + "; RETURN NEXT ref" + std::to_string(curr_read_count) + ";";
        ++curr_read_count;
      }
    }
  }
  func_str += " END; $$ LANGUAGE PLPGSQL;";

  std::cout << ">>>>>>>>>>>>>>>" << std::endl;
  for (size_t i = 0; i < config.operation_count_; ++i) {
    std::cout << is_update[i] << " ";
  }
  std::cout << std::endl;
  std::cout << "<<<<<<<<<<<<<<<" << std::endl;

  pqxx::nontransaction nontxn0(conn);

  nontxn0.exec(func_str.c_str());

  nontxn0.commit();

  pqxx::work txn(conn);

  std::string txn_str = "SELECT ycsb(";
  if (read_count == 0) {
    for (size_t i = 0; i < config.operation_count_ - 1; ++i) {
      size_t key = zipf.GetNextNumber() - 1;
      txn_str += std::to_string(key) + ", ";
    }
    size_t key = zipf.GetNextNumber() - 1;
    txn_str += std::to_string(key) + ");";
  } else {
    for (size_t i = 0; i < config.operation_count_; ++i) {
      size_t key = zipf.GetNextNumber() - 1;
      txn_str += std::to_string(key) + ", ";
    }
    for (size_t i = 0; i < read_count - 1; ++i) {
      txn_str += "'ref" + std::to_string(i) + "', ";
    }
    txn_str += "'ref" + std::to_string(read_count - 1) + "');";
  }

  std::cout << txn_str << std::endl;

  txn.exec(txn_str.c_str());

  for (size_t i = 0; i < read_count; ++i) {
    std::string select_str = "FETCH ALL FROM ref" + std::to_string(i) + ";";
    pqxx::result R = txn.exec(select_str.c_str());
    
    std::cout << "txn result set size = " << R.size() << std::endl;

    for (size_t i = 0; i < R.size(); ++i) {
      std::cout << R[i][0].as<std::string>() << std::endl;
    }
  }

  txn.commit();

  pqxx::nontransaction nontxn1(conn);

  nontxn1.exec("DROP FUNCTION ycsb;");

  nontxn1.commit();


  delete[] is_update;
  is_update = nullptr;

}

void Scan(pqxx::connection &conn) {
  
  pqxx::work txn(conn);

  std::cout << ">>>>> Scan table." << std::endl;

  pqxx::result R = txn.exec("SELECT * FROM employee;");
  
  printf("txn result set size = %lu\n", R.size());
  
  for (size_t i = 0; i < R.size(); ++i) {
    int id = R[i][0].as<int>();
    std::string name = R[i][1].as<std::string>();
    std::cout << "id = " << id << ", " << name << std::endl;
  }
  
  txn.commit();
}







