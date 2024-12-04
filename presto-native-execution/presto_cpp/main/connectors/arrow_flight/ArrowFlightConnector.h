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

#include "arrow/flight/api.h"
#include "presto_cpp/main/connectors/arrow_flight/FlightConfig.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "velox/connectors/Connector.h"

namespace facebook::presto::connector::arrow_flight {

class FlightTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  explicit FlightTableHandle(const std::string& connectorId)
      : ConnectorTableHandle{connectorId} {}
};

struct FlightSplit : public velox::connector::ConnectorSplit {
  /// @param connectorId
  /// @param ticket Flight Ticket obtained from `GetFlightInfo`
  /// @param locations Locations which can consume the ticket
  /// @param extraCredentials Extra credentials for authentication
  FlightSplit(
      const std::string& connectorId,
      const std::string& ticket,
      const std::vector<std::string>& locations = {},
      const std::map<std::string, std::string>& extraCredentials = {})
      : ConnectorSplit{connectorId},
        ticket{ticket},
        locations{locations},
        extraCredentials{extraCredentials} {}

  const std::string ticket;
  const std::vector<std::string> locations;
  std::map<std::string, std::string> extraCredentials;
};

class FlightColumnHandle : public velox::connector::ColumnHandle {
 public:
  FlightColumnHandle(const std::string& columnName) : columnName_{columnName} {}

  const std::string& name() {
    return columnName_;
  }

 private:
  std::string columnName_;
};

class FlightDataSource : public velox::connector::DataSource {
 public:
  FlightDataSource(
      const velox::RowTypePtr& outputType,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
      std::shared_ptr<auth::Authenticator> authenticator,
      velox::memory::MemoryPool* pool,
      const std::shared_ptr<FlightConfig>& flightConfig,
      const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts,
      const std::optional<arrow::flight::Location> defaultLocation =
          std::nullopt);

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) override;

  void addDynamicFilter(
      velox::column_index_t outputChannel,
      const std::shared_ptr<velox::common::Filter>& filter) override {
    VELOX_NYI("This connector doesn't support dynamic filters");
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
  /// Convert an arrow record batch to Velox RowVector.
  /// Process only those columns that are present in outputType_.
  velox::RowVectorPtr projectOutputColumns(
      const std::shared_ptr<arrow::RecordBatch>& input);

  velox::RowTypePtr outputType_;
  std::vector<std::string> columnMapping_;
  std::unique_ptr<arrow::flight::FlightStreamReader> currentReader_;
  uint64_t completedRows_ = 0;
  uint64_t completedBytes_ = 0;
  std::shared_ptr<auth::Authenticator> authenticator_;
  velox::memory::MemoryPool* const pool_;
  const std::shared_ptr<FlightConfig> flightConfig_;
  const std::shared_ptr<arrow::flight::FlightClientOptions> clientOpts_;
  const std::optional<arrow::flight::Location> defaultLocation_;
};

class ArrowFlightConnector : public velox::connector::Connector {
 public:
  explicit ArrowFlightConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      const char* authenticatorName = nullptr)
      : Connector{id},
        flightConfig_{std::make_shared<FlightConfig>(config)},
        clientOpts_{initClientOpts(flightConfig_)},
        defaultLocation_{getDefaultLocation(flightConfig_)},
        authenticator_{auth::getAuthenticatorFactory(
                           authenticatorName
                               ? authenticatorName
                               : flightConfig_->authenticatorName())
                           ->newAuthenticator(config)} {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const std::shared_ptr<velox::connector::ConnectorTableHandle>&
          tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
      velox::connector::ConnectorQueryCtx* ctx) override {
    return std::make_unique<FlightDataSource>(
        outputType,
        columnHandles,
        authenticator_,
        ctx->memoryPool(),
        flightConfig_,
        clientOpts_,
        defaultLocation_);
  }

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr inputType,
      std::shared_ptr<velox::connector::ConnectorInsertTableHandle>
          connectorInsertTableHandle,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      velox::connector::CommitStrategy commitStrategy) override {
    VELOX_NYI("Flight connector does not support DataSink");
  }

 private:
  // Returns the default location specified in the FlightConfig.
  // Returns nullopt if either host or port is missing.
  static std::optional<arrow::flight::Location> getDefaultLocation(
      const std::shared_ptr<FlightConfig>& config);

  static std::shared_ptr<arrow::flight::FlightClientOptions> initClientOpts(
      const std::shared_ptr<FlightConfig>& config);

  const std::shared_ptr<FlightConfig> flightConfig_;
  const std::shared_ptr<arrow::flight::FlightClientOptions> clientOpts_;
  const std::optional<arrow::flight::Location> defaultLocation_;
  const std::shared_ptr<auth::Authenticator> authenticator_;
};

class ArrowFlightConnectorFactory : public velox::connector::ConnectorFactory {
 public:
  static constexpr const char* kArrowFlightConnectorName = "arrow-flight";

  ArrowFlightConnectorFactory() : ConnectorFactory(kArrowFlightConnectorName) {}

  explicit ArrowFlightConnectorFactory(
      const char* name,
      const char* authenticatorName = nullptr)
      : ConnectorFactory{name}, authenticatorName_{authenticatorName} {}

  std::shared_ptr<velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<ArrowFlightConnector>(
        id, config, authenticatorName_);
  }

 private:
  const char* authenticatorName_{nullptr};
};

} // namespace facebook::presto::connector::arrow_flight
