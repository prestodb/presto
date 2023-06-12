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

#include "velox/connectors/Connector.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::hive {

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

class HiveColumnHandle : public ColumnHandle {
 public:
  enum class ColumnType { kPartitionKey, kRegular, kSynthesized };

  HiveColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      std::vector<common::Subfield> requiredSubfields = {})
      : name_(name),
        columnType_(columnType),
        dataType_(std::move(dataType)),
        requiredSubfields_(std::move(requiredSubfields)) {}

  const std::string& name() const {
    return name_;
  }

  ColumnType columnType() const {
    return columnType_;
  }

  const TypePtr& dataType() const {
    return dataType_;
  }

  // Applies to columns of complex types: arrays, maps and structs.  When a
  // query uses only some of the subfields, the engine provides the complete
  // list of required subfields and the connector is free to prune the rest.
  //
  // Examples:
  //  - SELECT a[1], b['x'], x.y FROM t
  //  - SELECT a FROM t WHERE b['y'] > 10
  //
  // Pruning a struct means populating some of the members with null values.
  //
  // Pruning a map means dropping keys not listed in the required subfields.
  //
  // Pruning arrays means dropping values with indices larger than maximum
  // required index.
  const std::vector<common::Subfield>& requiredSubfields() const {
    return requiredSubfields_;
  }

  bool isPartitionKey() const {
    return columnType_ == ColumnType::kPartitionKey;
  }

  std::string toString() const;

  folly::dynamic serialize() const override;

  static ColumnHandlePtr create(const folly::dynamic& obj);

  static std::string columnTypeName(HiveColumnHandle::ColumnType columnType);

  static HiveColumnHandle::ColumnType columnTypeFromName(
      const std::string& name);

  static void registerSerDe();

 private:
  const std::string name_;
  const ColumnType columnType_;
  const TypePtr dataType_;
  const std::vector<common::Subfield> requiredSubfields_;
};

class HiveTableHandle : public ConnectorTableHandle {
 public:
  HiveTableHandle(
      std::string connectorId,
      const std::string& tableName,
      bool filterPushdownEnabled,
      SubfieldFilters subfieldFilters,
      const core::TypedExprPtr& remainingFilter);

  ~HiveTableHandle() override;

  bool isFilterPushdownEnabled() const {
    return filterPushdownEnabled_;
  }

  const SubfieldFilters& subfieldFilters() const {
    return subfieldFilters_;
  }

  const core::TypedExprPtr& remainingFilter() const {
    return remainingFilter_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  const std::string tableName_;
  const bool filterPushdownEnabled_;
  const SubfieldFilters subfieldFilters_;
  const core::TypedExprPtr remainingFilter_;
};

} // namespace facebook::velox::connector::hive
