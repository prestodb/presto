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

#include "velox/experimental/cudf/connectors/hive/CudfHiveTableHandle.h"

#include "velox/connectors/Connector.h"
#include "velox/type/Type.h"

#include <string>

namespace facebook::velox::cudf_velox::connector::hive {

using namespace facebook::velox::connector;

std::string CudfHiveColumnHandle::toString() const {
  std::ostringstream out;
  out << fmt::format(
      "CudfHiveColumnHandle [name: {}, Type: {},", name_, type_->toString());
  return out.str();
}

CudfHiveTableHandle::CudfHiveTableHandle(
    std::string connectorId,
    const std::string& tableName,
    bool filterPushdownEnabled,
    const core::TypedExprPtr& subfieldFilterExpr,
    const core::TypedExprPtr& remainingFilter,
    const RowTypePtr& dataColumns)
    : ConnectorTableHandle(std::move(connectorId)),
      tableName_(tableName),
      filterPushdownEnabled_(filterPushdownEnabled),
      subfieldFilterExpr_(subfieldFilterExpr),
      remainingFilter_(remainingFilter),
      dataColumns_(dataColumns) {}

std::string CudfHiveTableHandle::toString() const {
  std::stringstream out;
  out << "table: " << tableName_;
  if (dataColumns_) {
    out << ", data columns: " << dataColumns_->toString();
  }
  return out.str();
}

ConnectorTableHandlePtr CudfHiveTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  VELOX_NYI("CudfHiveTableHandle::create() not yet implemented");
}

} // namespace facebook::velox::cudf_velox::connector::hive
