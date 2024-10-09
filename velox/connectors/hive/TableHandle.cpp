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

#include "velox/connectors/hive/TableHandle.h"

namespace facebook::velox::connector::hive {

namespace {
std::unordered_map<HiveColumnHandle::ColumnType, std::string>
columnTypeNames() {
  return {
      {HiveColumnHandle::ColumnType::kPartitionKey, "PartitionKey"},
      {HiveColumnHandle::ColumnType::kRegular, "Regular"},
      {HiveColumnHandle::ColumnType::kSynthesized, "Synthesized"},
      {HiveColumnHandle::ColumnType::kRowIndex, "RowIndex"}};
}

template <typename K, typename V>
std::unordered_map<V, K> invertMap(const std::unordered_map<K, V>& mapping) {
  std::unordered_map<V, K> inverted;
  for (const auto& [key, value] : mapping) {
    inverted.emplace(value, key);
  }
  return inverted;
}

} // namespace

std::string HiveColumnHandle::columnTypeName(
    HiveColumnHandle::ColumnType type) {
  static const auto ctNames = columnTypeNames();
  return ctNames.at(type);
}

HiveColumnHandle::ColumnType HiveColumnHandle::columnTypeFromName(
    const std::string& name) {
  static const auto nameColumnTypes = invertMap(columnTypeNames());
  return nameColumnTypes.at(name);
}

folly::dynamic HiveColumnHandle::serialize() const {
  folly::dynamic obj = ColumnHandle::serializeBase("HiveColumnHandle");
  obj["hiveColumnHandleName"] = name_;
  obj["columnType"] = columnTypeName(columnType_);
  obj["dataType"] = dataType_->serialize();
  obj["hiveType"] = hiveType_->serialize();
  folly::dynamic requiredSubfields = folly::dynamic::array;
  for (const auto& subfield : requiredSubfields_) {
    requiredSubfields.push_back(subfield.toString());
  }
  obj["requiredSubfields"] = requiredSubfields;
  return obj;
}

std::string HiveColumnHandle::toString() const {
  std::ostringstream out;
  out << fmt::format(
      "HiveColumnHandle [name: {}, columnType: {}, dataType: {},",
      name_,
      columnTypeName(columnType_),
      dataType_->toString());
  out << " requiredSubfields: [";
  for (const auto& subfield : requiredSubfields_) {
    out << " " << subfield.toString();
  }
  out << " ]]";
  return out.str();
}

ColumnHandlePtr HiveColumnHandle::create(const folly::dynamic& obj) {
  auto name = obj["hiveColumnHandleName"].asString();
  auto columnType = columnTypeFromName(obj["columnType"].asString());
  auto dataType = ISerializable::deserialize<Type>(obj["dataType"]);
  auto hiveType = ISerializable::deserialize<Type>(obj["hiveType"]);

  const auto& arr = obj["requiredSubfields"];
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.reserve(arr.size());
  for (auto& s : arr) {
    requiredSubfields.emplace_back(s.asString());
  }

  return std::make_shared<HiveColumnHandle>(
      name, columnType, dataType, hiveType, std::move(requiredSubfields));
}

void HiveColumnHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveColumnHandle", HiveColumnHandle::create);
}

HiveTableHandle::HiveTableHandle(
    std::string connectorId,
    const std::string& tableName,
    bool filterPushdownEnabled,
    SubfieldFilters subfieldFilters,
    const core::TypedExprPtr& remainingFilter,
    const RowTypePtr& dataColumns,
    const std::unordered_map<std::string, std::string>& tableParameters)
    : ConnectorTableHandle(std::move(connectorId)),
      tableName_(tableName),
      filterPushdownEnabled_(filterPushdownEnabled),
      subfieldFilters_(std::move(subfieldFilters)),
      remainingFilter_(remainingFilter),
      dataColumns_(dataColumns),
      tableParameters_(tableParameters) {}

std::string HiveTableHandle::toString() const {
  std::stringstream out;
  out << "table: " << tableName_;
  if (!subfieldFilters_.empty()) {
    // Sort filters by subfield for deterministic output.
    std::map<std::string, common::Filter*> orderedFilters;
    for (const auto& [field, filter] : subfieldFilters_) {
      orderedFilters[field.toString()] = filter.get();
    }
    out << ", range filters: [";
    bool notFirstFilter = false;
    for (const auto& [field, filter] : orderedFilters) {
      if (notFirstFilter) {
        out << ", ";
      }
      out << "(" << field << ", " << filter->toString() << ")";
      notFirstFilter = true;
    }
    out << "]";
  }
  if (remainingFilter_) {
    out << ", remaining filter: (" << remainingFilter_->toString() << ")";
  }
  if (dataColumns_) {
    out << ", data columns: " << dataColumns_->toString();
  }
  return out.str();
}

folly::dynamic HiveTableHandle::serialize() const {
  folly::dynamic obj = ConnectorTableHandle::serializeBase("HiveTableHandle");
  obj["tableName"] = tableName_;
  obj["filterPushdownEnabled"] = filterPushdownEnabled_;

  folly::dynamic subfieldFilters = folly::dynamic::array;
  for (const auto& [subfield, filter] : subfieldFilters_) {
    folly::dynamic pair = folly::dynamic::object;
    pair["subfield"] = subfield.toString();
    pair["filter"] = filter->serialize();
    subfieldFilters.push_back(pair);
  }

  obj["subfieldFilters"] = subfieldFilters;
  if (remainingFilter_) {
    obj["remainingFilter"] = remainingFilter_->serialize();
  }
  if (dataColumns_) {
    obj["dataColumns"] = dataColumns_->serialize();
  }

  return obj;
}

ConnectorTableHandlePtr HiveTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto connectorId = obj["connectorId"].asString();
  auto tableName = obj["tableName"].asString();
  auto filterPushdownEnabled = obj["filterPushdownEnabled"].asBool();

  core::TypedExprPtr remainingFilter;
  if (auto it = obj.find("remainingFilter"); it != obj.items().end()) {
    remainingFilter =
        ISerializable::deserialize<core::ITypedExpr>(it->second, context);
  }

  SubfieldFilters subfieldFilters;
  folly::dynamic subfieldFiltersObj = obj["subfieldFilters"];
  for (const auto& subfieldFilter : subfieldFiltersObj) {
    common::Subfield subfield(subfieldFilter["subfield"].asString());
    auto filter =
        ISerializable::deserialize<common::Filter>(subfieldFilter["filter"]);
    subfieldFilters[common::Subfield(std::move(subfield.path()))] =
        filter->clone();
  }

  RowTypePtr dataColumns;
  if (auto it = obj.find("dataColumns"); it != obj.items().end()) {
    dataColumns = ISerializable::deserialize<RowType>(it->second, context);
  }

  return std::make_shared<const HiveTableHandle>(
      connectorId,
      tableName,
      filterPushdownEnabled,
      std::move(subfieldFilters),
      remainingFilter,
      dataColumns);
}

void HiveTableHandle::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveTableHandle", create);
}

} // namespace facebook::velox::connector::hive
