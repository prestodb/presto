/*
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

#include <string>
#include <vector>

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::protocol::lance {

struct LanceColumnHandle : public ColumnHandle {
  String columnName;
  Type columnType;
  bool nullable = true;

  LanceColumnHandle() noexcept;

  bool operator<(const ColumnHandle& o) const override {
    return columnName < dynamic_cast<const LanceColumnHandle&>(o).columnName;
  }
};

void to_json(json& j, const LanceColumnHandle& p);
void from_json(const json& j, LanceColumnHandle& p);

struct LanceTableHandle : public ConnectorTableHandle {
  String schemaName;
  String tableName;

  LanceTableHandle() noexcept;
};

void to_json(json& j, const LanceTableHandle& p);
void from_json(const json& j, LanceTableHandle& p);

struct LanceTableLayoutHandle : public ConnectorTableLayoutHandle {
  std::shared_ptr<LanceTableHandle> table;
  TupleDomain<std::shared_ptr<ColumnHandle>> tupleDomain;

  LanceTableLayoutHandle() noexcept;
};

void to_json(json& j, const LanceTableLayoutHandle& p);
void from_json(const json& j, LanceTableLayoutHandle& p);

struct LanceSplit : public ConnectorSplit {
  String datasetPath;
  List<int> fragments;

  LanceSplit() noexcept;
};

void to_json(json& j, const LanceSplit& p);
void from_json(const json& j, LanceSplit& p);

struct LanceTransactionHandle : public ConnectorTransactionHandle {
  LanceTransactionHandle() noexcept;
};

void to_json(json& j, const LanceTransactionHandle& p);
void from_json(const json& j, LanceTransactionHandle& p);

} // namespace facebook::presto::protocol::lance
