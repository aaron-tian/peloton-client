#pragma once

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include "client_config.h"

void Populate(pqxx::connection &conn, const ClientConfig &config);

void Process(pqxx::connection &conn, const ClientConfig &config);

void Scan(pqxx::connection &conn);
