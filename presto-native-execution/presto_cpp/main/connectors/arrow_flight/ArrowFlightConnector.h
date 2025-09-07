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

#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConfig.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "velox/connectors/Connector.h"

namespace arrow {
class RecordBatch;
namespace flight {
class FlightClientOptions;
class FlightStreamReader;
class Location;
} // namespace flight
} // namespace arrow

namespace facebook::presto {

class ArrowFlightTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  explicit ArrowFlightTableHandle(const std::string& connectorId)
      : ConnectorTableHandle(connectorId), name_("arrow_flight") {}

  const std::string& name() const override {
    return name_;
  }

 private:
  const std::string name_;
};

struct ArrowFlightSplit : public velox::connector::ConnectorSplit {
  /// @param connectorId
  /// @param flightEndpointBytes Base64 Serialized `FlightEndpoint`
  ArrowFlightSplit(
      const std::string& connectorId,
      const std::string& flightEndpointBytes)
      : ConnectorSplit(connectorId),
        flightEndpointBytes_(flightEndpointBytes) {}

  const std::string flightEndpointBytes_;
};

class ArrowFlightColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit ArrowFlightColumnHandle(const std::string& columnName)
      : columnName_(columnName) {}

  const std::string& name() const {
    return columnName_;
  }

 private:
  std::string columnName_;
};

class ArrowFlightDataSource : public velox::connector::DataSource {
 public:
  ArrowFlightDataSource(
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

  void addDynamicFilter(
      velox::column_index_t outputChannel,
      const std::shared_ptr<velox::common::Filter>& filter) override {
    VELOX_UNSUPPORTED("Arrow Flight connector doesn't support dynamic filters");
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, velox::RuntimeCounter> runtimeStats()
      override {
    return {};
  }

 private:
  /// Convert an Arrow record batch to Velox RowVector.
  /// Process only those columns that are present in outputType_.
  velox::RowVectorPtr projectOutputColumns(
      const std::shared_ptr<arrow::RecordBatch>& input);

  velox::RowTypePtr outputType_;
  std::vector<std::string> columnMapping_;
  std::unique_ptr<arrow::flight::FlightStreamReader> currentReader_;
  uint64_t completedRows_ = 0;
  uint64_t completedBytes_ = 0;
  std::shared_ptr<Authenticator> authenticator_;
  const velox::connector::ConnectorQueryCtx* const connectorQueryCtx_;
  const std::shared_ptr<ArrowFlightConfig> flightConfig_;
  const std::shared_ptr<arrow::flight::FlightClientOptions> clientOpts_;
  const std::shared_ptr<arrow::flight::Location> defaultLocation_;
};

class ArrowFlightConnector : public velox::connector::Connector {
 public:
  explicit ArrowFlightConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      const char* authenticatorName = nullptr)
      : Connector(id),
        flightConfig_(std::make_shared<ArrowFlightConfig>(config)),
        clientOpts_(initClientOpts(flightConfig_)),
        authenticator_(getAuthenticatorFactory(
                           authenticatorName
                               ? authenticatorName
                               : flightConfig_->authenticatorName())
                           ->newAuthenticator(config)) {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr inputType,
      velox::connector::ConnectorInsertTableHandlePtr
          connectorInsertTableHandle,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      velox::connector::CommitStrategy commitStrategy) override {
    VELOX_NYI("The arrow-flight connector does not support a DataSink");
  }

 private:
  static std::shared_ptr<arrow::flight::FlightClientOptions> initClientOpts(
      const std::shared_ptr<ArrowFlightConfig>& config);

  const std::shared_ptr<ArrowFlightConfig> flightConfig_;
  const std::shared_ptr<arrow::flight::FlightClientOptions> clientOpts_;
  const std::shared_ptr<Authenticator> authenticator_;
};

class ArrowFlightConnectorFactory : public velox::connector::ConnectorFactory {
 public:
  static constexpr const char* kArrowFlightConnectorName = "arrow-flight";

  ArrowFlightConnectorFactory() : ConnectorFactory(kArrowFlightConnectorName) {}

  explicit ArrowFlightConnectorFactory(
      const char* name,
      const char* authenticatorName = nullptr)
      : ConnectorFactory(name), authenticatorName_(authenticatorName) {}

  std::shared_ptr<velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<ArrowFlightConnector>(
        id, config, authenticatorName_);
  }

 private:
  const char* authenticatorName_{nullptr};
};

} // namespace facebook::presto
