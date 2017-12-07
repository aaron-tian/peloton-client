#pragma once

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include "driver_config.h"

void Populate(pqxx::connection &conn, const DriverConfig &config);

void ProcessClient(pqxx::connection &conn, const DriverConfig &config);

void ProcessProcedure(pqxx::connection &conn, const DriverConfig &config);

void Scan(pqxx::connection &conn);

/* aa_profiling begin */
extern void aa_BeginProfiling();
extern void aa_EndProfiling();
extern bool aa_IsProfiling();
extern void aa_InsertTimePoint(char* point_name);
/* aa_profiling end */

