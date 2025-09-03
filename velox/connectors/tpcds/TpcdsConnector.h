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
#include "velox/connectors/tpcds/TpcdsConnectorSplit.h"
#include "velox/tpcds/gen/TpcdsGen.h"

namespace facebook::velox::connector::tpcds {

class TpcdsConnector;

// TPC-DS column handle only needs the column name (all columns are generated in
// the same way).
class TpcdsColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit TpcdsColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};

// TPC-DS table handle uses the underlying enum to describe the target table.
class TpcdsTableHandle : public ConnectorTableHandle {
 public:
  explicit TpcdsTableHandle(
      std::string connectorId,
      velox::tpcds::Table table,
      double scaleFactor = 0.01)
      : ConnectorTableHandle(std::move(connectorId)),
        table_(table),
        name_(toTableName(table)),
        scaleFactor_(scaleFactor) {
    VELOX_CHECK_GT(scaleFactor, 0.0, "Tpcds scale factor must be non-negative");
  }

  const std::string& name() const override {
    return name_;
  }

  std::string toString() const override;

  velox::tpcds::Table getTpcdsTable() const {
    return table_;
  }

  double getScaleFactor() const {
    return scaleFactor_;
  }

 private:
  const velox::tpcds::Table table_;
  const std::string name_;
  const double scaleFactor_;
};

class TpcdsDataSource : public velox::connector::DataSource {
 public:
  TpcdsDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<const velox::connector::ConnectorTableHandle>&
          tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<const velox::connector::ColumnHandle>>& columnHandles,
      velox::memory::MemoryPool* FOLLY_NONNULL pool);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) override {
    VELOX_NYI("Dynamic filters not supported by TpcdsConnector.");
  }

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  RowVectorPtr projectOutputColumns(RowVectorPtr vector);

  velox::tpcds::Table table_;
  double scaleFactor_{0.01};
  size_t rowCount_{0};
  RowTypePtr outputType_;

  // Mapping between output columns and their indices (column_index_t) in the
  // dsdgen generated datasets.
  std::vector<column_index_t> outputColumnMappings_;

  std::shared_ptr<connector::tpcds::TpcdsConnectorSplit> currentSplit_;

  // Offset of the first row in current split.
  uint64_t splitOffset_{0};
  // Offset of the last row in current split.
  uint64_t splitEnd_{0};

  size_t completedRows_{0};
  size_t completedBytes_{0};

  memory::MemoryPool* FOLLY_NONNULL pool_;
};

class TpcdsConnector final : public velox::connector::Connector {
 public:
  TpcdsConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* FOLLY_NULLABLE /*executor*/)
      : Connector(id) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<const ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<const velox::connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    return std::make_unique<TpcdsDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          const ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI("TpcdsConnector does not support data sink.");
  }
};

class TpcdsConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kTpcdsConnectorName{"tpcds"};

  TpcdsConnectorFactory() : ConnectorFactory(kTpcdsConnectorName) {}

  explicit TpcdsConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<TpcdsConnector>(id, config, ioExecutor);
  }
};

} // namespace facebook::velox::connector::tpcds
