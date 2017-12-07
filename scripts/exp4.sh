# run postgres with C++ client interface
# ycsb benchmark, each transaction contains 10 operations, R/W = 0/100%
# prepared statement is enabled

./process_client.sh 1 1 10 1 0
