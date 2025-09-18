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

#include "velox/experimental/cudf/connectors/hive/CudfHiveConfig.h"

#include "velox/connectors/hive/HiveConnector.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/types.hpp>

namespace facebook::velox::cudf_velox::connector::hive {

using namespace facebook::velox::connector;
using namespace facebook::velox::config;

class CudfHiveConnector final
    : public ::facebook::velox::connector::hive::HiveConnector {
 public:
  CudfHiveConnector(
      const std::string& id,
      std::shared_ptr<const ConfigBase> config,
      folly::Executor* executor);

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override final;

  bool canAddDynamicFilter() const override {
    return false;
  }

  // TODO (dm): Re-add data sink

 protected:
  // TODO (dm): rename parquetconfig
  const std::shared_ptr<CudfHiveConfig> cudfHiveConfig_;
};

class CudfHiveConnectorFactory
    : public ::facebook::velox::connector::hive::HiveConnectorFactory {
 public:
  CudfHiveConnectorFactory()
      : ::facebook::velox::connector::hive::HiveConnectorFactory() {}

  explicit CudfHiveConnectorFactory(const char* connectorName)
      : ::facebook::velox::connector::hive::HiveConnectorFactory(
            connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override;
};

} // namespace facebook::velox::cudf_velox::connector::hive
