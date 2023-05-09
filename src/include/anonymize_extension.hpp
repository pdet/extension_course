#pragma once

#include "duckdb.hpp"

namespace duckdb {

class AnonymizeExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

struct AnonymizeEmail {
  static void Register(Catalog &catalog, ClientContext &context);
};

struct GenerateData {
  static void Register(Catalog &catalog, ClientContext &context);
};

} // namespace duckdb
