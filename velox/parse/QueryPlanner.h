/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/core/PlanNode.h"
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::velox::core {

class DuckDbQueryPlanner {
 public:
  DuckDbQueryPlanner(memory::MemoryPool* pool) : pool_{pool} {}

  void registerTable(
      const std::string& name,
      const std::vector<RowVectorPtr>& data);

  void registerScalarFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType);

  // TODO Allow replacing built-in DuckDB functions. Currently, replacing "sum"
  // causes a crash (a bug in DuckDB). Replacing existing functions is useful
  // when signatures don't match.
  void registerAggregateFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType);

  PlanNodePtr plan(const std::string& sql);

 private:
  ::duckdb::DuckDB db_;
  ::duckdb::Connection conn_{db_};
  memory::MemoryPool* pool_;
  std::unordered_map<std::string, std::vector<RowVectorPtr>> tables_;
};

PlanNodePtr parseQuery(
    const std::string& sql,
    memory::MemoryPool* pool,
    const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
        inMemoryTables = {});

} // namespace facebook::velox::core
