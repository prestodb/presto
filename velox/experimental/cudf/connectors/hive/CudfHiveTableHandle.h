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
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"

#include <cudf/types.hpp>

#include <string>
#include <vector>

namespace facebook::velox::cudf_velox::connector::hive {

using namespace facebook::velox::connector;

// CudfHive column handle only needs the column name (all columns are generated
// in the same way).
class CudfHiveColumnHandle : public ColumnHandle {
 public:
  explicit CudfHiveColumnHandle(
      const std::string& name,
      const TypePtr type,
      const cudf::data_type cudfDataType,
      std::vector<CudfHiveColumnHandle> children = {})
      : name_(name),
        type_(type),
        cudfDataType_(cudfDataType),
        children_(std::move(children)) {}

  const std::string& name() const {
    return name_;
  }

  const TypePtr& type() const {
    return type_;
  }

  const cudf::data_type cudfDataType() const {
    return cudfDataType_;
  }

  const std::vector<CudfHiveColumnHandle>& children() const {
    return children_;
  }

  std::string toString() const;

 private:
  const std::string name_;
  const TypePtr type_;
  const cudf::data_type cudfDataType_;
  const std::vector<CudfHiveColumnHandle> children_;
};

class CudfHiveTableHandle : public ConnectorTableHandle {
 public:
  CudfHiveTableHandle(
      std::string connectorId,
      const std::string& tableName,
      bool filterPushdownEnabled,
      const core::TypedExprPtr& subfieldFilterExpr,
      const core::TypedExprPtr& remainingFilter = nullptr,
      const RowTypePtr& dataColumns = nullptr);

  const std::string& name() const override {
    return tableName_;
  }

  bool isFilterPushdownEnabled() const {
    return filterPushdownEnabled_;
  }

  const core::TypedExprPtr& subfieldFilterExpr() const {
    return subfieldFilterExpr_;
  }

  const core::TypedExprPtr& remainingFilter() const {
    return remainingFilter_;
  }

  // Schema of the table.  Need this for reading TEXTFILE.
  const RowTypePtr& dataColumns() const {
    return dataColumns_;
  }

  std::string toString() const override;

  static ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  const std::string tableName_;
  const bool filterPushdownEnabled_;
  // This expression is used for predicate pushdown.
  const core::TypedExprPtr subfieldFilterExpr_;
  // This expression is used for post-scan filtering.
  const core::TypedExprPtr remainingFilter_;
  const RowTypePtr dataColumns_;
};

} // namespace facebook::velox::cudf_velox::connector::hive
