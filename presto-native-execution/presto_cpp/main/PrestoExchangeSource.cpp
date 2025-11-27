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
#include "presto_cpp/main/PrestoExchangeSource.h"

#include <fmt/core.h>
#include <folly/SocketAddress.h>
#include <re2/re2.h>
#include <sstream>

#include "presto_cpp/main/QueryContextManager.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TestValue.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {

std::string extractTaskId(const std::string& path) {
  static const RE2 kPattern("/v1/task/([^/]*)/.*");
  std::string taskId;
  if (RE2::PartialMatch(path, kPattern, &taskId)) {
    return taskId;
  }

  VLOG(1) << "Failed to extract task ID from remote split: " << path;

  throw std::invalid_argument(
      fmt::format("Cannot extract task ID from remote split URL: {}", path));
}

void onFinalFailure(
    const std::string& errorMessage,
    const std::shared_ptr<exec::ExchangeQueue>& queue) {
  VLOG(1) << errorMessage;

  queue->setError(errorMessage);
}

std::string bodyAsString(
    http::HttpResponse& response,
    memory::MemoryPool* pool) {
  if (response.hasError()) {
    return response.error();
  }
  std::ostringstream oss;
  auto iobufs = response.consumeBody();
  for (auto& body : iobufs) {
    oss << std::string(
        reinterpret_cast<const char*>(body->data()), body->length());
    if (pool != nullptr) {
      pool->free(body->writableData(), body->capacity());
    }
  }
  return oss.str();
}
} // namespace

PrestoExchangeSource::PrestoExchangeSource(
    const folly::Uri& baseUri,
    int destination,
    const std::shared_ptr<exec::ExchangeQueue>& queue,
    memory::MemoryPool* pool,
    folly::CPUThreadPoolExecutor* driverExecutor,
    folly::EventBase* ioEventBase,
    http::HttpClientConnectionPool* connPool,
    const proxygen::Endpoint& endpoint,
    folly::SSLContextPtr sslContext)
    : ExchangeSource(extractTaskId(baseUri.path()), destination, queue, pool),
      basePath_(baseUri.path()),
      host_(baseUri.host()),
      port_(baseUri.port()),
      sslContext_(std::move(sslContext)),
      enableBufferCopy_(SystemConfig::instance()->exchangeEnableBufferCopy()),
      immediateBufferTransfer_(
          enableBufferCopy_ &&
          SystemConfig::instance()->exchangeImmediateBufferTransfer()),
      driverExecutor_(driverExecutor) {
  folly::SocketAddress address;
  if (folly::IPAddress::validate(host_)) {
    address = folly::SocketAddress(folly::IPAddress(host_), port_);
  } else {
    address = folly::SocketAddress(host_, port_, true);
  }
  const auto requestTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeRequestTimeoutMs());
  const auto connectTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeConnectTimeoutMs());
  VELOX_CHECK_NOT_NULL(driverExecutor_);
  VELOX_CHECK_NOT_NULL(ioEventBase);
  VELOX_CHECK_NOT_NULL(pool_);
  httpClient_ = std::make_shared<http::HttpClient>(
      ioEventBase,
      connPool,
      endpoint,
      address,
      requestTimeoutMs,
      connectTimeoutMs,
      immediateBufferTransfer_ ? pool_ : nullptr,
      sslContext_);
}

void PrestoExchangeSource::close() {
  closed_.store(true);
  checkSetRequestPromise();
  abortResults();
}

bool PrestoExchangeSource::shouldRequestLocked() {
  if (atEnd_) {
    return false;
  }

  if (!requestPending_) {
    VELOX_CHECK(!promise_.valid() || promise_.isFulfilled());
    requestPending_ = true;
    return true;
  }

  // We are still processing previous request.
  return false;
}

folly::SemiFuture<PrestoExchangeSource::Response> PrestoExchangeSource::request(
    uint32_t maxBytes,
    std::chrono::microseconds maxWait) {
  // Before calling 'request', the caller should have called
  // 'shouldRequestLocked' and received 'true' response. Hence, we expect
  // requestPending_ == true, atEnd_ == false.
  VELOX_CHECK(requestPending_);
  // This call cannot be made concurrently from multiple threads, but other
  // calls that mutate promise_ can be called concurrently.
  VeloxPromise<Response> promise{"PrestoExchangeSource::request"};
  auto future = promise.getSemiFuture();
  velox::common::testutil::TestValue::adjust(
      "facebook::presto::PrestoExchangeSource::request", this);
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    VELOX_CHECK(!promise_.valid() || promise_.isFulfilled());
    if (closed_.load()) {
      promise.setValue(Response{0, false});
      return future;
    }
    promise_ = std::move(promise);
  }

  failedAttempts_ = 0;
  dataRequestRetryState_ = RetryState(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeMaxErrorDuration())
          .count());
  doRequest(dataRequestRetryState_.nextDelayMs(), maxBytes, maxWait);

  return future;
}

void PrestoExchangeSource::doRequest(
    int64_t delayMs,
    uint32_t maxBytes,
    std::chrono::microseconds maxWait) {
  if (closed_.load()) {
    queue_->setError("PrestoExchangeSource closed");
    return;
  }

  auto path = fmt::format("{}/{}", basePath_, sequence_);
  auto self = getSelfPtr();
  proxygen::HTTPMethod method;
  if (maxBytes == 0) {
    method = proxygen::HTTPMethod::HEAD;
  } else {
    method = proxygen::HTTPMethod::GET;
  }
  auto requestBuilder = http::RequestBuilder().method(method).url(path);

  velox::common::testutil::TestValue::adjust(
      "facebook::presto::PrestoExchangeSource::doRequest", this);
  requestBuilder
      .header(
          protocol::PRESTO_MAX_SIZE_HTTP_HEADER,
          protocol::DataSize(maxBytes, protocol::DataUnit::BYTE).toString())
      .header(
          protocol::PRESTO_MAX_WAIT_HTTP_HEADER,
          protocol::Duration(maxWait.count(), protocol::TimeUnit::MICROSECONDS)
              .toString())
      .header(proxygen::HTTP_HEADER_HOST, fmt::format("{}:{}", host_, port_))
      .send(httpClient_.get(), "", delayMs)
      .via(driverExecutor_)
      .thenTry(
          [this, path, maxBytes, maxWait, self = getSelfPtr()](
              folly::Try<std::unique_ptr<http::HttpResponse>> responseTry) {
            // self needs to be held for keeping 'this' source alive during
            // processing
            handleDataResponse(std::move(responseTry), maxWait, maxBytes, path);
          });
};

void PrestoExchangeSource::handleDataResponse(
    folly::Try<std::unique_ptr<http::HttpResponse>> responseTry,
    std::chrono::microseconds maxWait,
    uint32_t maxBytes,
    const std::string& httpRequestPath) {
  velox::common::testutil::TestValue::adjust(
      "facebook::presto::PrestoExchangeSource::handleDataResponse", this);
  if (responseTry.hasException()) {
    processDataError(
        httpRequestPath,
        maxBytes,
        maxWait,
        responseTry.exception().what().toStdString());
  } else {
    try {
      auto& response = responseTry.value();
      auto* headers = response->headers();
      if (headers->getStatusCode() != http::kHttpOk &&
          headers->getStatusCode() != http::kHttpNoContent) {
        // Ideally, not all errors are retryable - especially internal
        // server errors - which usually point to a query failure on another
        // machine. But we retry all such errors and rely on the coordinator
        // to cancel other tasks, when some tasks have failed.
        processDataError(
            httpRequestPath,
            maxBytes,
            maxWait,
            fmt::format(
                "Received HTTP {} {} {}",
                headers->getStatusCode(),
                headers->getStatusMessage(),
                bodyAsString(
                    *response,
                    immediateBufferTransfer_ ? pool_.get() : nullptr)));
      } else if (response->hasError()) {
        processDataError(httpRequestPath, maxBytes, maxWait, response->error());
      } else {
        const bool isGetDataSizeRequest = (maxBytes == 0);
        processDataResponse(std::move(response), isGetDataSizeRequest);
      }
    } catch (const std::exception& e) {
      processDataError(httpRequestPath, maxBytes, maxWait, e.what());
    }
  }
}

void PrestoExchangeSource::processDataResponse(
    std::unique_ptr<http::HttpResponse> response,
    bool isGetDataSizeRequest) {
  if (isGetDataSizeRequest) {
    RECORD_HISTOGRAM_METRIC_VALUE(
        kCounterExchangeGetDataSizeDuration,
        dataRequestRetryState_.durationMs());
    RECORD_HISTOGRAM_METRIC_VALUE(
        kCounterExchangeGetDataSizeNumTries, dataRequestRetryState_.numTries());
  } else {
    RECORD_HISTOGRAM_METRIC_VALUE(
        kCounterExchangeRequestDuration, dataRequestRetryState_.durationMs());
    RECORD_HISTOGRAM_METRIC_VALUE(
        kCounterExchangeRequestNumTries, dataRequestRetryState_.numTries());
  }
  if (closed_.load()) {
    // If PrestoExchangeSource is already closed, just free all buffers
    // allocated without doing any processing. This can happen when a super slow
    // response comes back after its owning 'Task' gets destroyed.
    response->freeBuffers();
    return;
  }
  auto* headers = response->headers();
  if (!SystemConfig::instance()->httpClientHttp2Enabled()) {
    VELOX_CHECK(
        !headers->getIsChunked(),
        "Chunked http transferring encoding is not supported.");
  }
  const uint64_t contentLength =
      atol(headers->getHeaders()
               .getSingleOrEmpty(proxygen::HTTP_HEADER_CONTENT_LENGTH)
               .c_str());
  VLOG(1) << "Fetched data for " << basePath_ << "/" << sequence_ << ": "
          << contentLength << " bytes";

  auto complete = headers->getHeaders()
                      .getSingleOrEmpty(protocol::PRESTO_BUFFER_COMPLETE_HEADER)
                      .compare("true") == 0;
  if (complete) {
    VLOG(1) << "Received buffer-complete header for " << basePath_ << "/"
            << sequence_;
  }

  std::vector<int64_t> remainingBytes;
  auto remainingBytesString = headers->getHeaders().getSingleOrEmpty(
      protocol::PRESTO_BUFFER_REMAINING_BYTES_HEADER);
  if (!remainingBytesString.empty()) {
    folly::split(',', remainingBytesString, remainingBytes);
    if (!remainingBytes.empty() && remainingBytes[0] == 0) {
      VELOX_CHECK_EQ(remainingBytes.size(), 1);
      remainingBytes.clear();
    }
  }

  std::optional<int64_t> ackSequenceOpt;
  const auto nextTokenStr = headers->getHeaders().getSingleOrEmpty(
      protocol::PRESTO_PAGE_NEXT_TOKEN_HEADER);
  if (!nextTokenStr.empty()) {
    // NOTE: when get data size from Presto coordinator, it might not set next
    // token so we shouldn't update 'sequence_' if it is empty. Otherwise,
    // 'sequence_' gets reset and we can't fetch any data from the source with
    // the rolled back 'sequence_'.
    ackSequenceOpt = atol(nextTokenStr.c_str());
  } else {
    VELOX_CHECK_EQ(
        contentLength, 0, "next token is not set in non-empty data response");
  }

  std::unique_ptr<exec::SerializedPageBase> page;
  const bool empty = response->empty();
  if (!empty) {
    std::vector<std::unique_ptr<folly::IOBuf>> iobufs;
    if (immediateBufferTransfer_ || !enableBufferCopy_) {
      iobufs = response->consumeBody();
    } else {
      iobufs.emplace_back(response->consumeBody(pool_.get()));
    }
    int64_t totalBytes{0};
    std::unique_ptr<folly::IOBuf> singleChain;
    for (auto& buf : iobufs) {
      totalBytes += buf->capacity();
      if (!singleChain) {
        singleChain = std::move(buf);
      } else {
        singleChain->prev()->appendChain(std::move(buf));
      }
    }
    PrestoExchangeSource::updateMemoryUsage(totalBytes);

    // Record page size counter when not a get-data-size request
    if (!isGetDataSizeRequest) {
      RECORD_HISTOGRAM_METRIC_VALUE(
          kCounterExchangeRequestPageSize, totalBytes);
    }

    if (enableBufferCopy_) {
      page = std::make_unique<exec::PrestoSerializedPage>(
          std::move(singleChain), [pool = pool_](folly::IOBuf& iobuf) {
            int64_t freedBytes{0};
            // Free the backed memory from MemoryAllocator on page dtor
            folly::IOBuf* start = &iobuf;
            auto curr = start;
            do {
              freedBytes += curr->capacity();
              pool->free(curr->writableData(), curr->capacity());
              curr = curr->next();
            } while (curr != start);
            PrestoExchangeSource::updateMemoryUsage(-freedBytes);
          });
    } else {
      page = std::make_unique<exec::PrestoSerializedPage>(
          std::move(singleChain), [totalBytes](folly::IOBuf& iobuf) {
            PrestoExchangeSource::updateMemoryUsage(-totalBytes);
          });
    }
  }

  const int64_t pageSize = empty ? 0 : page->size();
  VeloxPromise<Response> requestPromise{VeloxPromise<Response>::makeEmpty()};
  std::vector<ContinuePromise> queuePromises;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (page) {
      VLOG(1) << "Enqueuing page for " << basePath_ << "/" << sequence_ << ": "
              << pageSize << " bytes";
      ++numPages_;
      totalBytes_ += pageSize;
      queue_->enqueueLocked(std::move(page), queuePromises);
    }
    if (complete) {
      VLOG(1) << "Enqueuing empty page for " << basePath_ << "/" << sequence_;
      atEnd_ = true;
      queue_->enqueueLocked(nullptr, queuePromises);
    }

    if (ackSequenceOpt.has_value()) {
      sequence_ = ackSequenceOpt.value();
    }
    requestPending_ = false;
    requestPromise = std::move(promise_);
  }
  for (auto& promise : queuePromises) {
    promise.setValue();
  }

  if (requestPromise.valid() && !requestPromise.isFulfilled()) {
    requestPromise.setValue(
        Response{pageSize, complete, std::move(remainingBytes)});
  } else {
    // The source must have been closed.
    VELOX_CHECK(closed_.load());
  }

  if (complete) {
    abortResults();
  }
}

void PrestoExchangeSource::processDataError(
    const std::string& path,
    uint32_t maxBytes,
    std::chrono::microseconds maxWait,
    const std::string& error) {
  ++failedAttempts_;
  if (!dataRequestRetryState_.isExhausted()) {
    VLOG(1) << "Failed to fetch data from " << host_ << ":" << port_ << " "
            << path << ", duration: " << dataRequestRetryState_.durationMs()
            << "ms - Retrying: " << error;

    doRequest(dataRequestRetryState_.nextDelayMs(), maxBytes, maxWait);
    return;
  }

  onFinalFailure(
      fmt::format(
          "Failed to fetch data from {}:{} {} - Exhausted after {} retries, duration {}ms: {}",
          host_,
          port_,
          path,
          failedAttempts_,
          dataRequestRetryState_.durationMs(),
          error),
      queue_);

  if (!checkSetRequestPromise()) {
    // The source must have been closed.
    VELOX_CHECK(closed_.load());
  }
}

void PrestoExchangeSource::pause() {
  int64_t ackSequence;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    ackSequence = sequence_;
  }
  acknowledgeResults(ackSequence);
}

void PrestoExchangeSource::acknowledgeResults(int64_t ackSequence) {
  auto ackPath = fmt::format("{}/{}/acknowledge", basePath_, ackSequence);
  VLOG(1) << "Sending ack " << ackPath;
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(ackPath)
      .send(httpClient_.get())
      .via(driverExecutor_)
      .thenTry(
          [this, self = getSelfPtr()](
              folly::Try<std::unique_ptr<http::HttpResponse>> responseTry) {
            // self needs to be held for keeping 'this' source alive during
            // processing
            handleAckResponse(std::move(responseTry));
          });
}

void PrestoExchangeSource::handleAckResponse(
    folly::Try<std::unique_ptr<http::HttpResponse>> responseTry) {
  if (!responseTry.hasException()) {
    try {
      auto& response = responseTry.value();
      VLOG(1) << "Ack " << response->headers()->getStatusCode();
    } catch (const std::exception& e) {
      // Acks are optional. No need to fail the query.
      VLOG(1) << "Ack failed: " << e.what();
    }
  } else {
    // Acks are optional. No need to fail the query.
    VLOG(1) << "Ack failed: " << responseTry.exception().what();
  }
}

void PrestoExchangeSource::abortResults() {
  if (abortResultsIssued_.exchange(true)) {
    return;
  }

  abortRetryState_ = RetryState(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeMaxErrorDuration())
          .count());
  VLOG(1) << "Sending abort results " << basePath_;
  doAbortResults(abortRetryState_.nextDelayMs());
}

void PrestoExchangeSource::doAbortResults(int64_t delayMs) {
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::DELETE)
      .url(basePath_)
      .send(httpClient_.get(), "", delayMs)
      .via(driverExecutor_)
      .thenTry([this, self = getSelfPtr()](
                   folly::Try<std::unique_ptr<http::HttpResponse>> response) {
        handleAbortResponse(std::move(response));
      });
}

void PrestoExchangeSource::handleAbortResponse(
    folly::Try<std::unique_ptr<http::HttpResponse>> responseTry) {
  std::optional<std::string> error;
  if (responseTry.hasException()) {
    error = responseTry.exception().what();
  } else {
    auto statusCode = responseTry.value()->headers()->getStatusCode();
    if (statusCode != http::kHttpOk && statusCode != http::kHttpNoContent) {
      error = std::to_string(statusCode);
    }
  }
  if (!error.has_value()) {
    return;
  }
  if (abortRetryState_.isExhausted()) {
    const std::string errMsg = fmt::format(
        "Abort results failed: {}, path {}", error.value(), basePath_);
    LOG(ERROR) << errMsg;
    return onFinalFailure(errMsg, queue_);
  }
  doAbortResults(abortRetryState_.nextDelayMs());
}

bool PrestoExchangeSource::checkSetRequestPromise() {
  VeloxPromise<Response> promise{VeloxPromise<Response>::makeEmpty()};
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    promise = std::move(promise_);
  }
  if (promise.valid() && !promise.isFulfilled()) {
    promise.setValue(Response{0, false});
    return true;
  }

  return false;
}

std::shared_ptr<PrestoExchangeSource> PrestoExchangeSource::getSelfPtr() {
  return std::dynamic_pointer_cast<PrestoExchangeSource>(shared_from_this());
}

// static
std::shared_ptr<PrestoExchangeSource> PrestoExchangeSource::create(
    const std::string& url,
    int destination,
    const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
    velox::memory::MemoryPool* memoryPool,
    folly::CPUThreadPoolExecutor* cpuExecutor,
    folly::IOThreadPoolExecutor* ioExecutor,
    http::HttpClientConnectionPool* connPool,
    folly::SSLContextPtr sslContext) {
  folly::Uri uri(url);
  auto* eventBase = ioExecutor->getEventBase();
  if (uri.scheme() == "http") {
    VELOX_CHECK_NULL(sslContext);
    proxygen::Endpoint ep(uri.host(), uri.port(), false);
    return std::make_shared<PrestoExchangeSource>(
        uri,
        destination,
        queue,
        memoryPool,
        cpuExecutor,
        eventBase,
        connPool,
        ep,
        sslContext);
  }
  if (uri.scheme() == "https") {
    VELOX_CHECK_NOT_NULL(sslContext);
    proxygen::Endpoint ep(uri.host(), uri.port(), true);
    return std::make_shared<PrestoExchangeSource>(
        uri,
        destination,
        queue,
        memoryPool,
        cpuExecutor,
        eventBase,
        connPool,
        ep,
        std::move(sslContext));
  }
  return nullptr;
}

void PrestoExchangeSource::updateMemoryUsage(int64_t updateBytes) {
  const int64_t newMemoryBytes =
      currQueuedMemoryBytes().fetch_add(updateBytes) + updateBytes;
  if (updateBytes > 0) {
    peakQueuedMemoryBytes() =
        std::max<int64_t>(peakQueuedMemoryBytes(), newMemoryBytes);
  } else {
    VELOX_CHECK_GE(currQueuedMemoryBytes(), 0);
  }
}

void PrestoExchangeSource::getMemoryUsage(
    int64_t& currentBytes,
    int64_t& peakBytes) {
  currentBytes = currQueuedMemoryBytes();
  peakBytes = std::max<int64_t>(currentBytes, peakQueuedMemoryBytes());
}

void PrestoExchangeSource::resetPeakMemoryUsage() {
  peakQueuedMemoryBytes() = currQueuedMemoryBytes().load();
}
} // namespace facebook::presto
