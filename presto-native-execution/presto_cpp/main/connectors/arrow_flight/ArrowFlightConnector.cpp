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
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/api.h>
#include <folly/base64.h>
#include <utility>
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox::connector;

namespace facebook::presto {
namespace {
std::shared_ptr<arrow::flight::Location> getDefaultLocation(
    const std::shared_ptr<ArrowFlightConfig>& config) {
  auto defaultHost = config->defaultServerHostname();
  auto defaultPort = config->defaultServerPort();
  if (!defaultHost.has_value() || !defaultPort.has_value()) {
    return nullptr;
  }

  AFC_ASSIGN_OR_RAISE(
      auto defaultLocation,
      config->defaultServerSslEnabled()
          ? arrow::flight::Location::ForGrpcTls(
                defaultHost.value(), defaultPort.value())
          : arrow::flight::Location::ForGrpcTcp(
                defaultHost.value(), defaultPort.value()));

  return std::make_shared<arrow::flight::Location>(std::move(defaultLocation));
}
} // namespace

// Wrapper for CallOptions which does not add any member variables,
// but provides a write-only interface for adding call headers.
class CallOptionsAddHeaders : public arrow::flight::FlightCallOptions,
                              public arrow::flight::AddCallHeaders {
 public:
  void AddHeader(const std::string& key, const std::string& value) override {
    headers.emplace_back(key, value);
  }
};

std::shared_ptr<arrow::flight::FlightClientOptions>
ArrowFlightConnector::initClientOpts(
    const std::shared_ptr<ArrowFlightConfig>& config) {
  auto clientOpts = std::make_shared<arrow::flight::FlightClientOptions>();
  clientOpts->disable_server_verification = !config->serverVerify();

  auto certPath = config->serverSslCertificate();
  if (certPath.has_value()) {
    std::ifstream file(certPath.value());
    VELOX_CHECK(file.is_open(), "Could not open TLS certificate");
    std::string cert(
        (std::istreambuf_iterator<char>(file)),
        (std::istreambuf_iterator<char>()));
    clientOpts->tls_root_certs = cert;
  }

  auto clientCertPath = config->clientSslCertificate();
  if (clientCertPath.has_value()) {
    std::ifstream certFile(clientCertPath.value());
    VELOX_CHECK(
        certFile.is_open(), "Could not open client certificate at {}", clientCertPath.value());
    clientOpts->cert_chain.assign(
        (std::istreambuf_iterator<char>(certFile)),
        (std::istreambuf_iterator<char>()));
  }

  auto clientKeyPath = config->clientSslKey();
  if (clientKeyPath.has_value()) {
    std::ifstream keyFile(clientKeyPath.value());
    VELOX_CHECK(
        keyFile.is_open(), "Could not open client key at {}", clientKeyPath.value());
    clientOpts->private_key.assign(
        (std::istreambuf_iterator<char>(keyFile)),
        (std::istreambuf_iterator<char>()));
  }

  return clientOpts;
}

ArrowFlightDataSource::ArrowFlightDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    std::shared_ptr<Authenticator> authenticator,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ArrowFlightConfig>& flightConfig,
    const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts)
    : outputType_{outputType},
      authenticator_{std::move(authenticator)},
      connectorQueryCtx_{connectorQueryCtx},
      flightConfig_{flightConfig},
      clientOpts_{clientOpts},
      defaultLocation_(getDefaultLocation(flightConfig_)) {
  VELOX_CHECK_NOT_NULL(clientOpts_, "FlightClientOptions is not initialized");

  // columnMapping_ contains the real column names in the expected order.
  // This is later used by projectOutputColumns to filter out unnecessary
  // columns from the fetched chunk.
  columnMapping_.reserve(outputType_->size());

  for (const auto& columnName : outputType_->names()) {
    auto it = columnHandles.find(columnName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "missing columnHandle for column '{}'",
        columnName);

    auto handle =
        std::dynamic_pointer_cast<const ArrowFlightColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "handle for column '{}' is not an ArrowFlightColumnHandle",
        columnName);

    columnMapping_.push_back(handle->name());
  }
}

void ArrowFlightDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto flightSplit = std::dynamic_pointer_cast<ArrowFlightSplit>(split);
  VELOX_CHECK(
      flightSplit, "ArrowFlightDataSource received wrong type of split");

  auto flightEndpointStr =
      folly::base64Decode(flightSplit->flightEndpointBytes_);

  arrow::flight::FlightEndpoint flightEndpoint;
  AFC_ASSIGN_OR_RAISE(
      flightEndpoint,
      arrow::flight::FlightEndpoint::Deserialize(flightEndpointStr));

  arrow::flight::Location loc;
  if (!flightEndpoint.locations.empty()) {
    loc = flightEndpoint.locations[0];
  } else {
    VELOX_CHECK_NOT_NULL(
        defaultLocation_,
        "No location from Flight endpoint, default host or port is missing");
    loc = *defaultLocation_;
  }

  AFC_ASSIGN_OR_RAISE(
      auto client, arrow::flight::FlightClient::Connect(loc, *clientOpts_));

  CallOptionsAddHeaders callOptsAddHeaders{};
  authenticator_->authenticateClient(
      client, connectorQueryCtx_->sessionProperties(), callOptsAddHeaders);

  auto readerResult = client->DoGet(callOptsAddHeaders, flightEndpoint.ticket);
  AFC_ASSIGN_OR_RAISE(currentReader_, readerResult);
}

std::optional<velox::RowVectorPtr> ArrowFlightDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /* unused */) {
  VELOX_CHECK_NOT_NULL(currentReader_, "Missing split, call addSplit() first");

  AFC_ASSIGN_OR_RAISE(auto chunk, currentReader_->Next());

  // Null values in the chunk indicates that the Flight stream is complete.
  if (!chunk.data) {
    currentReader_ = nullptr;
    return nullptr;
  }

  // Extract only required columns from the record batch as a velox RowVector.
  auto output = projectOutputColumns(chunk.data);

  completedRows_ += output->size();
  completedBytes_ += output->inMemoryBytes();
  return output;
}

velox::RowVectorPtr ArrowFlightDataSource::projectOutputColumns(
    const std::shared_ptr<arrow::RecordBatch>& input) {
  velox::memory::MemoryPool* pool = connectorQueryCtx_->memoryPool();
  std::vector<velox::VectorPtr> children;
  children.reserve(columnMapping_.size());

  // Extract and convert desired columns in the correct order.
  for (const auto& name : columnMapping_) {
    auto column = input->GetColumnByName(name);
    VELOX_CHECK_NOT_NULL(column, "column with name '{}' not found", name);
    ArrowArray array;
    ArrowSchema schema;
    AFC_RAISE_NOT_OK(arrow::ExportArray(*column, &array, &schema));
    children.push_back(velox::importFromArrowAsOwner(schema, array, pool));
  }

  return std::make_shared<velox::RowVector>(
      pool,
      outputType_,
      velox::BufferPtr() /*nulls*/,
      input->num_rows(),
      std::move(children));
}

std::unique_ptr<velox::connector::DataSource>
ArrowFlightConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<ArrowFlightDataSource>(
      outputType,
      columnHandles,
      authenticator_,
      connectorQueryCtx,
      flightConfig_,
      clientOpts_);
}

} // namespace facebook::presto
