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

#include "velox/experimental/cudf/connectors/parquet/ParquetConnector.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetDataSource.h"

namespace facebook::velox::cudf_velox::connector::parquet {

using namespace facebook::velox::connector;

ParquetConnector::ParquetConnector(
    const std::string& id,
    std::shared_ptr<const facebook::velox::config::ConfigBase> config,
    folly::Executor* executor)
    : Connector(id),
      parquetConfig_(std::make_shared<ParquetConfig>(config)),
      executor_(executor) {
  LOG(INFO) << "cudf::Parquet connector " << connectorId() << " created.";
}

std::unique_ptr<DataSource> ParquetConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<ParquetDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      executor_,
      connectorQueryCtx,
      parquetConfig_);
}

std::unique_ptr<DataSink> ParquetConnector::createDataSink(
    RowTypePtr inputType,
    ConnectorInsertTableHandlePtr connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy /*commitStrategy*/) {
  auto parquetInsertHandle =
      std::dynamic_pointer_cast<const ParquetInsertTableHandle>(
          connectorInsertTableHandle);
  VELOX_CHECK_NOT_NULL(
      parquetInsertHandle, "Parquet connector expecting parquet write handle!");
  return std::make_unique<ParquetDataSink>(
      inputType,
      parquetInsertHandle,
      connectorQueryCtx,
      CommitStrategy::kNoCommit,
      parquetConfig_);
}

std::shared_ptr<Connector> ParquetConnectorFactory::newConnector(
    const std::string& id,
    std::shared_ptr<const facebook::velox::config::ConfigBase> config,
    folly::Executor* ioExecutor,
    folly::Executor* cpuExecutor) {
  return std::make_shared<ParquetConnector>(id, config, ioExecutor);
}

} // namespace facebook::velox::cudf_velox::connector::parquet
