#define DUCKDB_EXTENSION_MAIN

#include "anonymize_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <random>

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

string AnonymizeEmail(string email, std::mt19937 &generator) {
  // Find the position of the "@" symbol
  size_t atPosition = email.find('@');

  // If the "@" symbol is not found, return the original email
  if (atPosition == string::npos) {
    throw InvalidInputException("This is not a valid email");
  }

  // Replace the characters before the "@" symbol with random letters
  static const char alphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  static const int alphabet_size = sizeof(alphabet) - 1;

  std::uniform_int_distribution<int> distribution(0, alphabet_size - 1);
  for (size_t i = 0; i < atPosition; i++) {
    email[i] = alphabet[distribution(generator)];
  }

  return email;
}

void AssignString(string &email, string_t *result, idx_t result_idx,
                  std::mt19937 &generator) {
  auto new_email = AnonymizeEmail(email, generator);
  auto len = new_email.size();
  auto ptr = new char[len];
  memcpy(ptr, new_email.c_str(), len);
  result[result_idx] = string_t(ptr, len);
}
void AnonymizeScalarFun(duckdb::DataChunk &args, duckdb::ExpressionState &state,
                        duckdb::Vector &result) {
  D_ASSERT(args.ColumnCount() == 1);
  Value seed;
  state.GetContext().TryGetCurrentSetting("anonymize_seed", seed);
  std::mt19937 generator(seed.GetValue<int64_t>());

  auto &email_vector = args.data[0];

  if (email_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
    if (duckdb::ConstantVector::IsNull(email_vector)) {
      duckdb::ConstantVector::SetNull(result, true);
    } else {
      auto email_ptr =
          duckdb::ConstantVector::GetData<duckdb::string_t>(email_vector);
      result.SetValue(
          0, Value(AnonymizeEmail(email_ptr[0].GetString(), generator)));
    }
  } else {
    UnifiedVectorFormat email_data;
    email_vector.ToUnifiedFormat(args.size(), email_data);
    auto email_ptr = (const duckdb::string_t *)email_data.data;
    auto result_ptr = duckdb::FlatVector::GetData<string_t>(result);
    if (email_data.validity.AllValid()) {
      for (idx_t i = 0; i < args.size(); ++i) {
        const auto idx = email_data.sel->get_index(i);
        auto email = email_ptr[idx].GetString();
        AssignString(email, result_ptr, i, generator);
      }
    } else {

      auto &result_validity = duckdb::FlatVector::Validity(result);
      for (idx_t i = 0; i < args.size(); i++) {
        if (email_data.validity.RowIsValid(i)) {
          const auto idx = email_data.sel->get_index(i);
          auto email = email_ptr[idx].GetString();
          AssignString(email, result_ptr, i, generator);
        } else {
          result_validity.SetInvalid(i);
        }
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
