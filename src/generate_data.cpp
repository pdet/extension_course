#include "anonymize_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <random>

namespace duckdb {

struct GenerateFunctionData : public TableFunctionData {
  GenerateFunctionData() = default;
  int64_t number_of_entries;
  int64_t generated_entries = 0;
  std::mt19937 generator;
};

// Returns a random integer between min and max, inclusive
int RandomInteger(int min, int max, std::mt19937 &generator) {
  std::uniform_int_distribution<int> distribution(min, max);
  return distribution(generator);
}

// Returns a random element from the given array
template <typename T, size_t N>
T RandomElement(T (&array)[N], std::mt19937 &generator) {
  return array[RandomInteger(0, N - 1, generator)];
}

// Generates a random fake name
string GenerateName(std::mt19937 &generator) {
  static const char *firstNames[] = {"John",   "Mary",   "David",   "Sarah",
                                     "Daniel", "Laura",  "Michael", "Emily",
                                     "James",  "Emma",   "William", "Ava",
                                     "Joseph", "Olivia", "Andrew",  "Sophia"};
  static const char *lastNames[] = {
      "Smith",    "Johnson", "Williams",  "Jones",    "Brown",     "Garcia",
      "Miller",   "Davis",   "Rodriguez", "Martinez", "Hernandez", "Lopez",
      "Gonzalez", "Perez",   "Taylor",    "Anderson"};
  string firstName = RandomElement(firstNames, generator);
  string lastName = RandomElement(lastNames, generator);
  return firstName + " " + lastName;
}

void AssignString(string_t *result, idx_t result_idx, std::mt19937 &generator) {
  auto random_email = GenerateName(generator);
  auto len = random_email.size();
  auto ptr = new char[len];
  memcpy(ptr, random_email.c_str(), len);
  result[result_idx] = string_t(ptr, len);
}

void GenerateFunction(ClientContext &context, TableFunctionInput &data,
                      DataChunk &output) {
  auto &func_data = ((GenerateFunctionData &)*data.bind_data);

  idx_t chunk_size = 0;
  auto result_vec = (string_t *)FlatVector::GetData(output.data[0]);
  for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
    if (func_data.generated_entries >= func_data.number_of_entries) {
      chunk_size = i;
      break;
    }
    AssignString(result_vec, i, func_data.generator);
    func_data.generated_entries++;
  }
  output.SetCardinality(chunk_size);
}

unique_ptr<FunctionData> GenerateBind(ClientContext &context,
                                      TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types,
                                      vector<string> &names) {
  auto result = make_uniq<GenerateFunctionData>();

  Value seed;
  context.TryGetCurrentSetting("anonymize_seed", seed);
  result->generator = std::mt19937(seed.GetValue<int64_t>());
  if (input.inputs[0].IsNull()) {
    throw InvalidInputException("Number of Generated Entries must be non-null");
  }
  auto num_entries = input.inputs[0].GetValue<int64_t>();

  if (num_entries <= 0) {
    throw InvalidInputException("Number of Generated Entries must be > 0");
  }
  result->number_of_entries = num_entries;
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("name");
  return std::move(result);
}

void GenerateData::Register(Catalog &catalog, ClientContext &context) {
  auto generate_data_info = CreateTableFunctionInfo(
      TableFunction("generate_data", {duckdb::LogicalType::BIGINT},
                    GenerateFunction, GenerateBind));

  catalog.CreateTableFunction(context, generate_data_info);
}
} // namespace duckdb