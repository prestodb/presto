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

#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"

namespace arrow {
class RecordBatch;
namespace flight {
class FlightClientOptions;
class FlightStreamReader;
class Location;
} // namespace flight
} // namespace arrow

namespace facebook::presto {

struct ArrowFederationSplit : public velox::connector::ConnectorSplit {
  /// @param connectorId
  /// @param splitBytes Base64 Serialized Split for Arrow Federation Flight
  /// Server
  ArrowFederationSplit(
      const std::string& connectorId,
      const std::string& splitBytes)
      : ConnectorSplit(connectorId), splitBytes_(splitBytes) {}

  const std::string splitBytes_;
};

class ArrowFederationColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit ArrowFederationColumnHandle(
      const std::string& columnHandleBytes,
      const std::string& columnName)
      : columnName_(columnName), columnHandleBytes_(columnHandleBytes) {}

  const std::string& name() const {
    return columnName_;
  }

  const std::string& columnHandleBytes() const {
    return columnHandleBytes_;
  }

  const std::string columnName_;
  const std::string columnHandleBytes_;
};

class ArrowFederationTableHandle
    : public velox::connector::ConnectorTableHandle {
 public:
  explicit ArrowFederationTableHandle(const std::string& connectorId)
      : ConnectorTableHandle(connectorId), name_("arrow_federation") {}

  const std::string& name() const override {
    return name_;
  }

 private:
  const std::string name_;
};

class ArrowFederationDataSource : public ArrowFlightDataSource {
 public:
  ArrowFederationDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      std::shared_ptr<Authenticator> authenticator,
      const velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<ArrowFlightConfig>& flightConfig,
      const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts);

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& /* unused */) override;

 private:
  const velox::connector::ColumnHandleMap columnHandles_;
};

class ArrowFederationConnector : public ArrowFlightConnector {
 public:
  explicit ArrowFederationConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      const char* authenticatorName = nullptr)
      : ArrowFlightConnector(id, config, authenticatorName) {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;
};

class ArrowFederationConnectorFactory
    : public velox::connector::ConnectorFactory {
 public:
  static constexpr const char* kArrowFederationConnectorName =
      "arrow-federation";

  ArrowFederationConnectorFactory()
      : ConnectorFactory(kArrowFederationConnectorName) {}

  explicit ArrowFederationConnectorFactory(
      const char* name,
      const char* authenticatorName = nullptr)
      : ConnectorFactory(name), authenticatorName_(authenticatorName) {}

  std::shared_ptr<velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<ArrowFederationConnector>(
        id, config, authenticatorName_);
  }

 private:
  const char* authenticatorName_{nullptr};
};

} // namespace facebook::presto
