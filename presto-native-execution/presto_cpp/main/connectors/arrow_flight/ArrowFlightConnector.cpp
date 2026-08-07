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
#include <arrow/status.h>
#include <fmt/format.h>
#include <folly/base64.h>
#include <atomic>
#include <optional>
#include <utility>
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightErrors.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox::connector;

namespace facebook::presto {
namespace {
std::atomic<int32_t> kActiveArrowFlightStreams{0};

int64_t elapsedNanos(const std::chrono::steady_clock::time_point& start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

int64_t toMillis(int64_t nanos) {
  return nanos / 1'000'000;
}

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
        certFile.is_open(),
        "Could not open client certificate at {}",
        clientCertPath.value());
    clientOpts->cert_chain.assign(
        (std::istreambuf_iterator<char>(certFile)),
        (std::istreambuf_iterator<char>()));
  }

  auto clientKeyPath = config->clientSslKey();
  if (clientKeyPath.has_value()) {
    std::ifstream keyFile(clientKeyPath.value());
    VELOX_CHECK(
        keyFile.is_open(),
        "Could not open client key at {}",
        clientKeyPath.value());
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

ArrowFlightDataSource::~ArrowFlightDataSource() {
  // Ensure cleanup happens even if cancel() wasn't called
  cancel();
}

void ArrowFlightDataSource::startStream() {
  if (streamStarted_) {
    return;
  }
  streamStarted_ = true;
  streamStart_ = std::chrono::steady_clock::now();
  streamsStarted_.addValue(1);
  RECORD_METRIC_VALUE(kCounterArrowFlightStreamsStarted, 1);
}

void ArrowFlightDataSource::finishStream(StreamOutcome outcome) {
  if (!streamStarted_) {
    return;
  }

  if (streamActive_) {
    streamActive_ = false;
    kActiveArrowFlightStreams.fetch_sub(1);
    RECORD_METRIC_VALUE(
        kCounterArrowFlightActiveStreams, kActiveArrowFlightStreams.load());
  }

  const auto streamNanos = elapsedNanos(streamStart_);
  streamWallNanos_.addValue(streamNanos);
  RECORD_METRIC_VALUE(
      kCounterArrowFlightStreamLatencyMs, toMillis(streamNanos));

  switch (outcome) {
    case StreamOutcome::kCompleted:
      streamsCompleted_.addValue(1);
      RECORD_METRIC_VALUE(kCounterArrowFlightStreamsCompleted, 1);
      break;
    case StreamOutcome::kFailed:
      streamsFailed_.addValue(1);
      RECORD_METRIC_VALUE(kCounterArrowFlightStreamsFailed, 1);
      break;
    case StreamOutcome::kCancelled:
      streamsCancelled_.addValue(1);
      RECORD_METRIC_VALUE(kCounterArrowFlightStreamsCancelled, 1);
      break;
  }

  streamStarted_ = false;
}

void ArrowFlightDataSource::closeResources(bool cancelReader) {
  if (currentReader_ != nullptr) {
    if (cancelReader) {
      currentReader_->Cancel();
    }
    currentReader_.reset();
  }

  if (currentClient_ != nullptr) {
    auto status = currentClient_->Close();
    if (!status.ok()) {
      LOG(WARNING) << "Failed to close Arrow Flight client: "
                   << status.message();
    }
    currentClient_.reset();
  }
}

void ArrowFlightDataSource::recordError(
    ErrorPhase phase,
    std::string_view category) {
  errors_.addValue(1);
  switch (phase) {
    case ErrorPhase::kConnect:
      RECORD_METRIC_VALUE(kCounterArrowFlightConnectErrors, 1);
      break;
    case ErrorPhase::kAuthenticate:
      RECORD_METRIC_VALUE(kCounterArrowFlightAuthenticateErrors, 1);
      break;
    case ErrorPhase::kDoGet:
      RECORD_METRIC_VALUE(kCounterArrowFlightDoGetErrors, 1);
      break;
    case ErrorPhase::kRead:
      RECORD_METRIC_VALUE(kCounterArrowFlightReadErrors, 1);
      break;
    case ErrorPhase::kDecode:
      RECORD_METRIC_VALUE(kCounterArrowFlightDecodeErrors, 1);
      break;
  }
  recordCategoryCounter(category);

  std::string_view phaseStr;
  switch (phase) {
    case ErrorPhase::kConnect:
      phaseStr = "connect";
      break;
    case ErrorPhase::kAuthenticate:
      phaseStr = "authenticate";
      break;
    case ErrorPhase::kDoGet:
      phaseStr = "doGet";
      break;
    case ErrorPhase::kRead:
      phaseStr = "read";
      break;
    case ErrorPhase::kDecode:
      phaseStr = "decode";
      break;
  }
  const auto key = fmt::format("arrowFlightError.{}.{}", phaseStr, category);
  errorStats_[key].addValue(1);
}

void ArrowFlightDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto flightSplit = std::dynamic_pointer_cast<ArrowFlightSplit>(split);
  VELOX_CHECK(
      flightSplit, "ArrowFlightDataSource received wrong type of split");

  VELOX_CHECK(
      currentClient_ == nullptr && currentReader_ == nullptr,
      "Cannot add new split while previous client/reader are still active. "
      "Previous split must reach EOS or be cancelled first.");

  startStream();
  auto phase = ErrorPhase::kConnect;
  bool errorRecorded = false;
  try {
    auto flightEndpointStr =
        folly::base64Decode(flightSplit->flightEndpointBytes_);
    auto deserializeResult =
        arrow::flight::FlightEndpoint::Deserialize(flightEndpointStr);
    if (!deserializeResult.ok()) {
      recordError(phase, errorCategory(deserializeResult.status()));
      errorRecorded = true;
      raiseFlightError(deserializeResult.status());
    }
    auto flightEndpoint = std::move(deserializeResult).ValueUnsafe();

    arrow::flight::Location loc;
    if (!flightEndpoint.locations.empty()) {
      loc = flightEndpoint.locations[0];
    } else {
      VELOX_CHECK_NOT_NULL(
          defaultLocation_,
          "No location from Flight endpoint, default host or port is missing");
      loc = *defaultLocation_;
    }

    const auto connectStart = std::chrono::steady_clock::now();
    auto connectResult =
        arrow::flight::FlightClient::Connect(loc, *clientOpts_);
    const auto connectNanos = elapsedNanos(connectStart);
    connectWallNanos_.addValue(connectNanos);
    RECORD_METRIC_VALUE(
        kCounterArrowFlightConnectLatencyMs, toMillis(connectNanos));
    if (!connectResult.ok()) {
      recordError(phase, errorCategory(connectResult.status()));
      errorRecorded = true;
      raiseFlightError(connectResult.status());
    }
    currentClient_ = std::move(connectResult).ValueUnsafe();

    phase = ErrorPhase::kAuthenticate;
    CallOptionsAddHeaders callOptsAddHeaders{};
    const auto authenticateStart = std::chrono::steady_clock::now();
    try {
      authenticator_->authenticateClient(
          currentClient_,
          connectorQueryCtx_->sessionProperties(),
          callOptsAddHeaders);
    } catch (...) {
      const auto authenticateNanos = elapsedNanos(authenticateStart);
      authenticateWallNanos_.addValue(authenticateNanos);
      RECORD_METRIC_VALUE(
          kCounterArrowFlightAuthenticateLatencyMs,
          toMillis(authenticateNanos));
      throw;
    }
    const auto authenticateNanos = elapsedNanos(authenticateStart);
    authenticateWallNanos_.addValue(authenticateNanos);
    RECORD_METRIC_VALUE(
        kCounterArrowFlightAuthenticateLatencyMs, toMillis(authenticateNanos));

    phase = ErrorPhase::kDoGet;
    const auto doGetStart = std::chrono::steady_clock::now();
    auto doGetResult =
        currentClient_->DoGet(callOptsAddHeaders, flightEndpoint.ticket);
    const auto doGetNanos = elapsedNanos(doGetStart);
    doGetWallNanos_.addValue(doGetNanos);
    RECORD_METRIC_VALUE(
        kCounterArrowFlightDoGetLatencyMs, toMillis(doGetNanos));
    if (!doGetResult.ok()) {
      recordError(phase, errorCategory(doGetResult.status()));
      errorRecorded = true;
      raiseFlightError(doGetResult.status());
    }
    currentReader_ = std::move(doGetResult).ValueUnsafe();
    streamActive_ = true;
    kActiveArrowFlightStreams.fetch_add(1);
    RECORD_METRIC_VALUE(
        kCounterArrowFlightActiveStreams, kActiveArrowFlightStreams.load());
  } catch (...) {
    if (!errorRecorded) {
      recordError(phase, currentExceptionCategory());
    }
    closeResources(true);
    finishStream(StreamOutcome::kFailed);
    throw;
  }
}

std::optional<velox::RowVectorPtr> ArrowFlightDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /* unused */) {
  VELOX_CHECK_NOT_NULL(currentReader_, "Missing split, call addSplit() first");

  arrow::flight::FlightStreamChunk chunk;
  const auto batchWaitStart = std::chrono::steady_clock::now();
  bool errorRecorded = false;
  try {
    auto chunkResult = currentReader_->Next();
    const auto batchWaitNanos = elapsedNanos(batchWaitStart);
    batchWaitWallNanos_.addValue(batchWaitNanos);
    RECORD_METRIC_VALUE(
        kCounterArrowFlightBatchWaitLatencyMs, toMillis(batchWaitNanos));
    if (!chunkResult.ok()) {
      recordError(ErrorPhase::kRead, errorCategory(chunkResult.status()));
      errorRecorded = true;
      raiseFlightError(chunkResult.status());
    }
    chunk = std::move(chunkResult).ValueUnsafe();
  } catch (...) {
    if (!errorRecorded) {
      const auto batchWaitNanos = elapsedNanos(batchWaitStart);
      batchWaitWallNanos_.addValue(batchWaitNanos);
      RECORD_METRIC_VALUE(
          kCounterArrowFlightBatchWaitLatencyMs, toMillis(batchWaitNanos));
      recordError(ErrorPhase::kRead, currentExceptionCategory());
    }
    closeResources(true);
    finishStream(StreamOutcome::kFailed);
    throw;
  }

  // Null values in the chunk indicates that the Flight stream is complete.
  if (!chunk.data) {
    finishStream(StreamOutcome::kCompleted);
    closeResources(false);
    return nullptr;
  }

  velox::RowVectorPtr output;
  const auto decodeStart = std::chrono::steady_clock::now();
  try {
    output = projectOutputColumns(chunk.data);
  } catch (...) {
    const auto decodeNanos = elapsedNanos(decodeStart);
    decodeWallNanos_.addValue(decodeNanos);
    RECORD_METRIC_VALUE(
        kCounterArrowFlightDecodeLatencyMs, toMillis(decodeNanos));
    recordError(ErrorPhase::kDecode, currentExceptionCategory());
    finishStream(StreamOutcome::kFailed);
    closeResources(true);
    throw;
  }
  const auto decodeNanos = elapsedNanos(decodeStart);
  decodeWallNanos_.addValue(decodeNanos);
  RECORD_METRIC_VALUE(
      kCounterArrowFlightDecodeLatencyMs, toMillis(decodeNanos));

  const auto rowCount = output->size();
  const auto byteCount = output->estimateFlatSize();
  batches_.addValue(1);
  rows_.addValue(rowCount);
  bytes_.addValue(byteCount);
  completedRows_ += rowCount;
  completedBytes_ += byteCount;
  RECORD_METRIC_VALUE(kCounterArrowFlightBatchesReceived, 1);
  RECORD_METRIC_VALUE(kCounterArrowFlightRowsReceived, rowCount);
  RECORD_METRIC_VALUE(kCounterArrowFlightBytesReceived, byteCount);
  return output;
}

void ArrowFlightDataSource::cancel() {
  if (streamActive_ || currentReader_ != nullptr || currentClient_ != nullptr) {
    finishStream(StreamOutcome::kCancelled);
  }
  closeResources(true);
}

std::unordered_map<std::string, velox::RuntimeMetric>
ArrowFlightDataSource::getRuntimeStats() {
  std::unordered_map<std::string, velox::RuntimeMetric> stats;
  stats.emplace("arrowFlightConnectWallNanos", connectWallNanos_);
  stats.emplace("arrowFlightAuthenticateWallNanos", authenticateWallNanos_);
  stats.emplace("arrowFlightDoGetWallNanos", doGetWallNanos_);
  stats.emplace("arrowFlightBatchWaitWallNanos", batchWaitWallNanos_);
  stats.emplace("arrowFlightDecodeWallNanos", decodeWallNanos_);
  stats.emplace("arrowFlightStreamWallNanos", streamWallNanos_);
  stats.emplace("arrowFlightBatches", batches_);
  stats.emplace("arrowFlightRows", rows_);
  stats.emplace("arrowFlightBytes", bytes_);
  stats.emplace("arrowFlightErrors", errors_);
  stats.emplace("arrowFlightStreamsStarted", streamsStarted_);
  stats.emplace("arrowFlightStreamsCompleted", streamsCompleted_);
  stats.emplace("arrowFlightStreamsFailed", streamsFailed_);
  stats.emplace("arrowFlightStreamsCancelled", streamsCancelled_);
  stats.insert(errorStats_.begin(), errorStats_.end());
  return stats;
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
