#define DUCKDB_EXTENSION_MAIN

#include "anonymize_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <random>

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
  Connection con(instance);
  con.BeginTransaction();
  //! Register Anonymize Email Function
  auto &catalog = Catalog::GetSystemCatalog(*con.context);
  AnonymizeEmail::Register(catalog, *con.context);
  GenerateData::Register(catalog, *con.context);

  //! Register Config to setup seed for testing
  auto &config = DBConfig::GetConfig(instance);
  config.AddExtensionOption(
      "anonymize_seed", "Seed used to generate anonymous values",
      LogicalType::BIGINT,
      Value(std::chrono::system_clock::now().time_since_epoch().count()));

  con.Commit();
}

void AnonymizeExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string AnonymizeExtension::Name() { return "anonymize"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void anonymize_init(duckdb::DatabaseInstance &db) {
  LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *anonymize_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
