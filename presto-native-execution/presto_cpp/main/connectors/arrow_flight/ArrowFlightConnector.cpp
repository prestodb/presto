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
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::presto::connector::arrow_flight {

using namespace arrow::flight;
using namespace velox;
using namespace velox::connector;

// Wrapper for CallOptions which does not add any member variables,
// but provides a write-only interface for adding call headers.
class CallOptionsAddHeaders : public FlightCallOptions, public AddCallHeaders {
 public:
  void AddHeader(const std::string& key, const std::string& value) override {
    headers.emplace_back(key, value);
  }
};

std::optional<Location> ArrowFlightConnector::getDefaultLocation(
    const std::shared_ptr<FlightConfig>& config) {
  auto defaultHost = config->defaultServerHostname();
  auto defaultPort = config->defaultServerPort();
  if (!defaultHost.has_value() || !defaultPort.has_value()) {
    return std::nullopt;
  }

  bool defaultSslEnabled = config->defaultServerSslEnabled();
  AFC_RETURN_OR_RAISE(
      defaultSslEnabled
          ? Location::ForGrpcTls(defaultHost.value(), defaultPort.value())
          : Location::ForGrpcTcp(defaultHost.value(), defaultPort.value()));
}

std::shared_ptr<arrow::flight::FlightClientOptions>
ArrowFlightConnector::initClientOpts(
    const std::shared_ptr<FlightConfig>& config) {
  auto clientOpts = std::make_shared<FlightClientOptions>();
  clientOpts->disable_server_verification = !config->serverVerify();

  auto certPath = config->serverSslCertificate();
  if (certPath.hasValue()) {
    std::ifstream file(certPath.value());
    VELOX_CHECK(file.is_open(), "Could not open TLS certificate");
    std::string cert(
        (std::istreambuf_iterator<char>(file)),
        (std::istreambuf_iterator<char>()));
    clientOpts->tls_root_certs = cert;
  }

  return clientOpts;
}

FlightDataSource::FlightDataSource(
    const RowTypePtr& outputType,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    std::shared_ptr<auth::Authenticator> authenticator,
    memory::MemoryPool* pool,
    const std::shared_ptr<FlightConfig>& flightConfig,
    const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts,
    const std::optional<arrow::flight::Location> defaultLocation)
    : outputType_{outputType},
      authenticator_{authenticator},
      pool_{pool},
      flightConfig_{flightConfig},
      clientOpts_{clientOpts},
      defaultLocation_{defaultLocation} {
  // columnMapping_ contains the real column names in the expected order.
  // This is later used by projectOutputColumns to filter out unecessary
  // columns from the fetched chunk.
  columnMapping_.reserve(outputType_->size());

  for (auto columnName : outputType_->names()) {
    auto it = columnHandles.find(columnName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "missing columnHandle for column '{}'",
        columnName);

    auto handle = std::dynamic_pointer_cast<FlightColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "handle for column '{}' is not an FlightColumnHandle",
        columnName);

    columnMapping_.push_back(handle->name());
  }
}

void FlightDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto flightSplit = std::dynamic_pointer_cast<FlightSplit>(split);
  VELOX_CHECK(flightSplit, "FlightDataSource received wrong type of split");

  auto& locs = flightSplit->locations;
  Location loc;
  if (locs.size() > 0) {
    AFC_ASSIGN_OR_RAISE(loc, Location::Parse(locs[0]));
  } else {
    VELOX_CHECK(
        defaultLocation_.has_value(),
        "Split has empty Location list, but default host or port is missing");
    loc = defaultLocation_.value();
  }

  AFC_ASSIGN_OR_RAISE(
      auto client,
      FlightClient::Connect(
          loc, clientOpts_ ? *clientOpts_ : FlightClientOptions{}));

  CallOptionsAddHeaders callOptsAddHeaders{};
  FlightCallOptions& callOpts = callOptsAddHeaders;
  AddCallHeaders& headerWriter = callOptsAddHeaders;
  authenticator_->authenticateClient(
      client, flightSplit->extraCredentials, headerWriter);

  auto ticket = Ticket{flightSplit->ticket};
  auto readerResult = client->DoGet(callOpts, ticket);
  VELOX_CHECK(
      readerResult.ok(),
      "Server replied with error: {}",
      readerResult.status().message());
  currentReader_ = std::move(readerResult).ValueUnsafe();
}

std::optional<RowVectorPtr> FlightDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /* unused */) {
  VELOX_CHECK_NOT_NULL(currentReader_, "Missing split, call addSplit() first");

  AFC_ASSIGN_OR_RAISE(auto chunk, currentReader_->Next());
  auto recordBatch = std::move(chunk).data;

  // Null values in the chunk indicates that the Flight stream is complete.
  if (!recordBatch) {
    currentReader_ = nullptr;
    return nullptr;
  }

  // Extract only required columns from the record batch as a velox RowVector.
  auto output = projectOutputColumns(recordBatch);

  completedRows_ += output->size();
  completedBytes_ += output->inMemoryBytes();
  return output;
}

RowVectorPtr FlightDataSource::projectOutputColumns(
    const std::shared_ptr<arrow::RecordBatch>& input) {
  std::vector<VectorPtr> children;
  children.reserve(columnMapping_.size());

  // Extract and convert desired columns in the correct order.
  for (auto name : columnMapping_) {
    auto column = input->GetColumnByName(name);
    VELOX_CHECK_NOT_NULL(column, "column with name '{}' not found", name);
    ArrowArray array;
    ArrowSchema schema;
    AFC_RAISE_NOT_OK(arrow::ExportArray(*column, &array, &schema));
    children.push_back(importFromArrowAsOwner(schema, array, pool_));
  }

  return std::make_shared<RowVector>(
      pool_,
      outputType_,
      BufferPtr() /*nulls*/,
      input->num_rows(),
      std::move(children));
}

} // namespace facebook::presto::connector::arrow_flight
