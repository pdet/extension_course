#define DUCKDB_EXTENSION_MAIN

#include "anonymize_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <random>

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

string AnonymizeEmail(string email) {
  // Find the position of the "@" symbol
  size_t atPosition = email.find('@');

  // If the "@" symbol is not found, return the original email
  if (atPosition == string::npos) {
    throw InternalException("This is not a valid email");
  }

  // Replace the characters before the "@" symbol with random letters
  static const char alphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  static const int alphabet_size = sizeof(alphabet) - 1;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<int> distribution(0, alphabet_size - 1);
  for (size_t i = 0; i < atPosition; i++) {
    email[i] = alphabet[distribution(generator)];
  }

  return email;
}

void AnonymizeScalarFun(duckdb::DataChunk &args, duckdb::ExpressionState &state,
                        duckdb::Vector &result) {
  D_ASSERT(args.ColumnCount() == 1);
  auto &email_vector = args.data[0];

  UnifiedVectorFormat email_data;
  email_vector.ToUnifiedFormat(args.size(), email_data);
  auto email_ptr = (const duckdb::string_t *)email_data.data;
  auto result_ptr = duckdb::FlatVector::GetData<string_t>(result);

  if (email_data.validity.AllValid()) {
    for (idx_t i = 0; i < args.size(); ++i) {
      const auto idx = email_data.sel->get_index(i);
      result_ptr[i] = AnonymizeEmail(email_ptr[idx].GetString());
    }
  } else {
    auto &result_validity = duckdb::FlatVector::Validity(result);
    for (idx_t i = 0; i < args.size(); i++) {
      if (email_data.validity.RowIsValid(i)) {
        const auto idx = email_data.sel->get_index(i);
        result_ptr[i] = AnonymizeEmail(email_ptr[idx].GetString());
      } else {
        result_validity.SetInvalid(i);
      }
    }
  }
}

static void LoadInternal(DatabaseInstance &instance) {
  Connection con(instance);
  con.BeginTransaction();

  auto &catalog = Catalog::GetSystemCatalog(*con.context);

  CreateScalarFunctionInfo anonymize_fun_info(
      ScalarFunction("anonymize", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                     AnonymizeScalarFun));
  anonymize_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
  catalog.CreateFunction(*con.context, &anonymize_fun_info);
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
