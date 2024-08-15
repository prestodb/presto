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

#include "velox/common/config/Config.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/fuzzer/FuzzerConnectorSplit.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::connector::fuzzer {

/// `FuzzerConnector` is a connector that generates data on-the-fly using
/// VectorFuzzer, based on the expected outputType defined by the client.
///
/// FuzzerConnector doesn't have the concept of table names, but using
/// `FuzzerTableHandle` clients can specify VectorFuzzer options and seed, which
/// are used when instantiating VectorFuzzer.
///
/// FuzzerConnectorSplit lets clients specify how many rows are expected to be
/// generated.

class FuzzerTableHandle : public ConnectorTableHandle {
 public:
  explicit FuzzerTableHandle(
      std::string connectorId,
      VectorFuzzer::Options options,
      size_t fuzzerSeed = 0)
      : ConnectorTableHandle(std::move(connectorId)),
        fuzzerOptions(options),
        fuzzerSeed(fuzzerSeed) {}

  ~FuzzerTableHandle() override {}

  std::string toString() const override {
    return "fuzzer-mock-table";
  }

  const VectorFuzzer::Options fuzzerOptions;
  size_t fuzzerSeed;
};

class FuzzerDataSource : public DataSource {
 public:
  FuzzerDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      velox::memory::MemoryPool* pool);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  void addDynamicFilter(
      column_index_t /*outputChannel*/,
      const std::shared_ptr<common::Filter>& /*filter*/) override {
    VELOX_NYI("Dynamic filters not supported by FuzzerConnector.");
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
    // TODO: Which stats do we want to expose here?
    return {};
  }

 private:
  const RowTypePtr outputType_;
  std::unique_ptr<VectorFuzzer> vectorFuzzer_;

  // The current split being processed.
  std::shared_ptr<FuzzerConnectorSplit> currentSplit_;

  // How many rows were generated for this split.
  uint64_t splitOffset_{0};
  uint64_t splitEnd_{0};

  size_t completedRows_{0};
  size_t completedBytes_{0};

  memory::MemoryPool* pool_;
};

class FuzzerConnector final : public Connector {
 public:
  FuzzerConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /*executor*/)
      : Connector(id) {}

  std::unique_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /*columnHandles*/,
      ConnectorQueryCtx* connectorQueryCtx) override final {
    return std::make_unique<FuzzerDataSource>(
        outputType, tableHandle, connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI("FuzzerConnector does not support data sink.");
  }
};

class FuzzerConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kFuzzerConnectorName{"fuzzer"};

  FuzzerConnectorFactory() : ConnectorFactory(kFuzzerConnectorName) {}

  explicit FuzzerConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<FuzzerConnector>(id, config, executor);
  }
};

} // namespace facebook::velox::connector::fuzzer
