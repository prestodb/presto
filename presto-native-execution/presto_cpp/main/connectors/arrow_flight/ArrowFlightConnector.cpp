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

// wrapper for CallOptions which doesn't add any members variables
// but provides a write-only interface for adding call headers
class CallOptionsAddHeaders : public FlightCallOptions, public AddCallHeaders {
 public:
  void AddHeader(const std::string& key, const std::string& value) override {
    headers.emplace_back(key, value);
  }
};

FlightDataSource::FlightDataSource(
    const RowTypePtr& outputType,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    std::shared_ptr<auth::Authenticator> authenticator,
    memory::MemoryPool* pool,
    const std::shared_ptr<FlightConfig>& flightConfig)
    : outputType_{outputType},
      authenticator_{authenticator},
      pool_{pool},
      flightConfig_(flightConfig) {
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
    auto defaultHost = flightConfig_->defaultServerHostname();
    auto defaultPort = flightConfig_->defaultServerPort();
    VELOX_CHECK(defaultHost.has_value(), "Server Hostname not given");
    VELOX_CHECK(defaultPort.has_value(), "Server Port not given");

    bool defaultSslEnabled = flightConfig_->defaultServerSslEnabled();
    AFC_ASSIGN_OR_RAISE(
        loc,
        defaultSslEnabled
            ? Location::ForGrpcTls(defaultHost.value(), defaultPort.value())
            : Location::ForGrpcTcp(defaultHost.value(), defaultPort.value()));
  }

  FlightClientOptions clientOpts{
      .disable_server_verification{!flightConfig_->serverVerify()}};
  auto certPath = flightConfig_->serverSslCertificate();
  if (certPath.hasValue()) {
    std::ifstream file(certPath.value());
    VELOX_CHECK(file.is_open(), "Could not open TLS certificate");
    std::string cert(
        (std::istreambuf_iterator<char>(file)),
        (std::istreambuf_iterator<char>()));
    clientOpts.tls_root_certs = cert;
  }

  AFC_ASSIGN_OR_RAISE(auto client, FlightClient::Connect(loc, clientOpts));

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
    velox::ContinueFuture& future) {
  VELOX_CHECK_NOT_NULL(currentReader_, "Missing split, call addSplit() first");

  AFC_ASSIGN_OR_RAISE(auto chunk, currentReader_->Next());
  auto recordBatch = std::move(chunk).data;

  // null values in the chunk indicates that the Flight stream is complete
  if (!recordBatch) {
    currentReader_ = nullptr;
    return nullptr;
  }

  // extract only required columns from the record batch as a velox RowVector
  auto output = projectOutputColumns(recordBatch);

  completedRows_ += output->size();
  completedBytes_ += output->inMemoryBytes();
  return output;
}

RowVectorPtr FlightDataSource::projectOutputColumns(
    std::shared_ptr<arrow::RecordBatch> input) {
  std::vector<VectorPtr> children;
  children.reserve(columnMapping_.size());

  // extract and convert desired columns in the correct order
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
