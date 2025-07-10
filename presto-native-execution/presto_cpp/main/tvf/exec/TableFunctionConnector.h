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

#include "presto_cpp/main/tvf/core/TableFunctionNode.h"
#include "presto_cpp/main/tvf/exec/TableFunctionConnectorSplit.h"

#include "velox/common/config/Config.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/Connector.h"

namespace facebook::presto::tvf {

class TableFunctionConnector;

class TableFunctionColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit TableFunctionColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};

class TableFunctionTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  explicit TableFunctionTableHandle(
      std::string connectorId,
      const std::string& name,
      TableFunctionHandlePtr handle)
      : ConnectorTableHandle(std::move(connectorId)),
        functionName_(name),
        functionHandle_(handle) {}

  ~TableFunctionTableHandle() override {}

  std::string toString() const override;

  const std::string functionName() const {
    return functionName_;
  }

  TableFunctionHandlePtr handle() const {
    return functionHandle_;
  }

 private:
  const std::string functionName_;

  TableFunctionHandlePtr functionHandle_;
};

class TableFunctionDataSource : public velox::connector::DataSource {
 public:
  TableFunctionDataSource(
      const std::shared_ptr<const velox::RowType>& outputType,
      const std::shared_ptr<velox::connector::ConnectorTableHandle>&
          tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
      velox::memory::MemoryPool* pool);

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  void addDynamicFilter(
      velox::column_index_t /*outputChannel*/,
      const std::shared_ptr<velox::common::Filter>& /*filter*/) override {
    VELOX_NYI("Dynamic filters not supported by Table Function Connector.");
  }

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  std::unordered_map<std::string, velox::RuntimeCounter> runtimeStats()
      override {
    // TODO: Which stats do we want to expose here?
    return {};
  }

 private:
  velox::memory::MemoryPool* pool_;
  // HashStringAllocator required by functions that allocate out of line
  // buffers.
  velox::HashStringAllocator stringAllocator_;

  velox::RowTypePtr outputType_;

  // This should be constructed for each data source.
  std::unique_ptr<TableFunction> function_;

  std::shared_ptr<TableFunctionConnectorSplit> currentSplit_;

  size_t completedRows_{0};
  size_t completedBytes_{0};
};

class TableFunctionConnector final : public velox::connector::Connector {
 public:
  TableFunctionConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      folly::Executor* /*executor*/)
      : Connector(id) {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const std::shared_ptr<const velox::RowType>& outputType,
      const std::shared_ptr<velox::connector::ConnectorTableHandle>&
          tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override final {
    return std::make_unique<TableFunctionDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr /*inputType*/,
      std::shared_ptr<
          velox::connector::
              ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      velox::connector::CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI("TableFunctionConnector does not support data sink.");
  }
};

class TableFunctionConnectorFactory
    : public velox::connector::ConnectorFactory {
 public:
  static constexpr const char* kTableFunctionConnectorName{"table_function"};

  TableFunctionConnectorFactory()
      : ConnectorFactory(kTableFunctionConnectorName) {}

  explicit TableFunctionConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<TableFunctionConnector>(id, config, ioExecutor);
  }
};

} // namespace facebook::presto::tvf
