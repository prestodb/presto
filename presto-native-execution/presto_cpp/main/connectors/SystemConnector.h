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

#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/SystemSplit.h"

#include "velox/connectors/Connector.h"

namespace facebook::presto {
class TaskManager;

class SystemColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit SystemColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};

class SystemTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  explicit SystemTableHandle(
      std::string connectorId,
      std::string schemaName,
      std::string tableName);

  std::string toString() const override;

  const std::string& name() const override {
    return name_;
  }

  const std::string& schemaName() const {
    return schemaName_;
  }

  const std::string& tableName() const {
    return tableName_;
  }

  const velox::RowTypePtr taskSchema() const;

 private:
  const std::string name_;
  const std::string schemaName_;
  const std::string tableName_;
};

class SystemDataSource : public velox::connector::DataSource {
 public:
  SystemDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      const TaskManager* taskManager,
      velox::memory::MemoryPool* pool);

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  void addDynamicFilter(
      velox::column_index_t /*outputChannel*/,
      const std::shared_ptr<velox::common::Filter>& /*filter*/) override {
    VELOX_NYI("Dynamic filters not supported by SystemConnector.");
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

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
      override {
    return {};
  }

 private:
  enum class TaskColumnEnum {
    // Note: These values are in the same order as SystemTableHandle schema.
    kNodeId = 0,
    kTaskId,
    kStageExecutionId,
    kStageId,
    kQueryId,
    kState,
    kSplits,
    kQueuedSplits,
    kRunningSplits,
    kCompletedSplits,
    kSplitScheduledTimeMs,
    kSplitCpuTimeMs,
    kSplitBlockedTimeMs,
    kRawInputBytes,
    kRawInputRows,
    kProcessedInputBytes,
    kProcessedInputRows,
    kOutputBytes,
    kOutputRows,
    kPhysicalWrittenBytes,
    kCreated,
    kStart,
    kLastHeartBeat,
    kEnd,
  };

  velox::RowVectorPtr getTaskResults();

  // Mapping between output columns and their indices (column_index_t)
  // corresponding to the taskInfo fields for them.
  std::vector<velox::column_index_t> outputColumnMappings_;
  velox::RowTypePtr outputType_;

  const TaskManager* taskManager_;
  velox::memory::MemoryPool* pool_;

  std::shared_ptr<SystemSplit> currentSplit_;

  size_t completedRows_{0};
  size_t completedBytes_{0};
};

class SystemConnector : public velox::connector::Connector {
 public:
  SystemConnector(const std::string& id, const TaskManager* taskManager)
      : Connector(id), taskManager_(taskManager) {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override final {
    VELOX_CHECK(taskManager_);
    return std::make_unique<SystemDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        taskManager_,
        connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr /*inputType*/,
      velox::connector::
          ConnectorInsertTableHandlePtr /*connectorInsertTableHandle*/,
      velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      velox::connector::CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI("SystemConnector does not support data sink.");
  }

 private:
  const TaskManager* taskManager_;
};

class SystemPrestoToVeloxConnector final : public PrestoToVeloxConnector {
 public:
  explicit SystemPrestoToVeloxConnector(std::string connectorId)
      : PrestoToVeloxConnector(std::move(connectorId)) {}

  std::unique_ptr<velox::connector::ConnectorSplit> toVeloxSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  std::unique_ptr<velox::connector::ColumnHandle> toVeloxColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  std::unique_ptr<velox::connector::ConnectorTableHandle> toVeloxTableHandle(
      const protocol::TableHandle& tableHandle,
      const VeloxExprConverter& exprConverter,
      const TypeParser& typeParser) const final;

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;
};

class TvfNativePrestoToVeloxConnector final : public PrestoToVeloxConnector {
 public:
  explicit TvfNativePrestoToVeloxConnector(std::string connectorId)
      : PrestoToVeloxConnector(std::move(connectorId)) {}

  std::unique_ptr<velox::connector::ConnectorSplit> toVeloxSplit(
      const protocol::ConnectorId& catalogId,
      const protocol::ConnectorSplit* connectorSplit,
      const protocol::SplitContext* splitContext) const final;

  std::unique_ptr<velox::connector::ColumnHandle> toVeloxColumnHandle(
      const protocol::ColumnHandle* column,
      const TypeParser& typeParser) const final;

  std::unique_ptr<velox::connector::ConnectorTableHandle> toVeloxTableHandle(
      const protocol::TableHandle& tableHandle,
      const VeloxExprConverter& exprConverter,
      const TypeParser& typeParser)
      const final;

  std::unique_ptr<protocol::ConnectorProtocol> createConnectorProtocol()
      const final;
};

} // namespace facebook::presto
