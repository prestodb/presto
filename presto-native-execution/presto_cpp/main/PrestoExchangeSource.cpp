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
#include "velox/common/base/StatsReporter.h"
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
    oss << std::string((const char*)body->data(), body->length());
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
    proxygen::SessionPool* sessionPool,
    const std::string& clientCertAndKeyPath,
    const std::string& ciphers)
    : ExchangeSource(extractTaskId(baseUri.path()), destination, queue, pool),
      basePath_(baseUri.path()),
      host_(baseUri.host()),
      port_(baseUri.port()),
      clientCertAndKeyPath_(clientCertAndKeyPath),
      ciphers_(ciphers),
      immediateBufferTransfer_(
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
      sessionPool,
      address,
      requestTimeoutMs,
      connectTimeoutMs,
      immediateBufferTransfer_ ? pool_ : nullptr,
      clientCertAndKeyPath_,
      ciphers_,
      [](size_t bufferBytes) {
        RECORD_METRIC_VALUE(kCounterHttpClientPrestoExchangeNumOnBody);
        RECORD_HISTOGRAM_METRIC_VALUE(
            kCounterHttpClientPrestoExchangeOnBodyBytes, bufferBytes);
      });
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
    uint32_t maxWaitSeconds) {
  // Before calling 'request', the caller should have called
  // 'shouldRequestLocked' and received 'true' response. Hence, we expect
  // requestPending_ == true, atEnd_ == false.
  VELOX_CHECK(requestPending_);
  // This call cannot be made concurrently from multiple threads, but other
  // calls that mutate promise_ can be called concurrently.
  auto promise = VeloxPromise<Response>("PrestoExchangeSource::request");
  auto future = promise.getSemiFuture();
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    VELOX_CHECK(!promise_.valid() || promise_.isFulfilled());
    promise_ = std::move(promise);
  }

  failedAttempts_ = 0;
  dataRequestRetryState_ =
      RetryState(std::chrono::duration_cast<std::chrono::milliseconds>(
                     SystemConfig::instance()->exchangeMaxErrorDuration())
                     .count());
  doRequest(dataRequestRetryState_.nextDelayMs(), maxBytes, maxWaitSeconds);

  return future;
}

void PrestoExchangeSource::doRequest(
    int64_t delayMs,
    uint32_t maxBytes,
    uint32_t maxWaitSeconds) {
  if (closed_.load()) {
    queue_->setError("PrestoExchangeSource closed");
    return;
  }

  auto path = fmt::format("{}/{}", basePath_, sequence_);
  VLOG(1) << "Fetching data from " << host_ << ":" << port_ << " " << path;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(path)
      .header(
          protocol::PRESTO_MAX_SIZE_HTTP_HEADER,
          protocol::DataSize(maxBytes, protocol::DataUnit::BYTE).toString())
      .header(
          protocol::PRESTO_MAX_WAIT_HTTP_HEADER,
          protocol::Duration(maxWaitSeconds, protocol::TimeUnit::SECONDS)
              .toString())
      .send(httpClient_.get(), "", delayMs)
      .via(driverExecutor_)
      .thenValue([path, maxBytes, maxWaitSeconds, self](
                     std::unique_ptr<http::HttpResponse> response) {
        velox::common::testutil::TestValue::adjust(
            "facebook::presto::PrestoExchangeSource::doRequest", self.get());
        auto* headers = response->headers();
        if (headers->getStatusCode() != http::kHttpOk &&
            headers->getStatusCode() != http::kHttpNoContent) {
          // Ideally, not all errors are retryable - especially internal server
          // errors - which usually point to a query failure on another machine.
          // But we retry all such errors and rely on the coordinator to
          // cancel other tasks, when some tasks have failed.
          self->processDataError(
              path,
              maxBytes,
              maxWaitSeconds,
              fmt::format(
                  "Received HTTP {} {} {}",
                  headers->getStatusCode(),
                  headers->getStatusMessage(),
                  bodyAsString(
                      *response,
                      self->immediateBufferTransfer_ ? self->pool_.get()
                                                     : nullptr)));
        } else if (response->hasError()) {
          self->processDataError(
              path, maxBytes, maxWaitSeconds, response->error());
        } else {
          self->processDataResponse(std::move(response));
        }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [path, maxBytes, maxWaitSeconds, self](const std::exception& e) {
            self->processDataError(path, maxBytes, maxWaitSeconds, e.what());
          });
};

void PrestoExchangeSource::processDataResponse(
    std::unique_ptr<http::HttpResponse> response) {
  if (closed_.load()) {
    // If PrestoExchangeSource is already closed, just free all buffers
    // allocated without doing any processing. This can happen when a super slow
    // response comes back after its owning 'Task' gets destroyed.
    response->freeBuffers();
    return;
  }
  auto* headers = response->headers();
  VELOX_CHECK(
      !headers->getIsChunked(),
      "Chunked http transferring encoding is not supported.")
  uint64_t contentLength =
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

  int64_t ackSequence =
      atol(headers->getHeaders()
               .getSingleOrEmpty(protocol::PRESTO_PAGE_NEXT_TOKEN_HEADER)
               .c_str());

  std::unique_ptr<exec::SerializedPage> page;
  const bool empty = response->empty();
  if (!empty) {
    std::vector<std::unique_ptr<folly::IOBuf>> iobufs;
    if (immediateBufferTransfer_) {
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

    page = std::make_unique<exec::SerializedPage>(
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
  }

  const int64_t pageSize = empty ? 0 : page->size();

  RECORD_HISTOGRAM_METRIC_VALUE(
      kCounterPrestoExchangeSerializedPageSize, pageSize);

  {
    VeloxPromise<Response> requestPromise;
    std::vector<ContinuePromise> queuePromises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      if (page) {
        VLOG(1) << "Enqueuing page for " << basePath_ << "/" << sequence_
                << ": " << pageSize << " bytes";
        ++numPages_;
        totalBytes_ += pageSize;
        queue_->enqueueLocked(std::move(page), queuePromises);
      }
      if (complete) {
        VLOG(1) << "Enqueuing empty page for " << basePath_ << "/" << sequence_;
        atEnd_ = true;
        queue_->enqueueLocked(nullptr, queuePromises);
      }

      sequence_ = ackSequence;
      requestPending_ = false;
      requestPromise = std::move(promise_);
    }
    for (auto& promise : queuePromises) {
      promise.setValue();
    }

    if (requestPromise.valid() && !requestPromise.isFulfilled()) {
      requestPromise.setValue(Response{pageSize, complete});
    } else {
      // The source must have been closed.
      VELOX_CHECK(closed_.load());
    }
  }

  if (complete) {
    abortResults();
  } else if (!empty) {
    // Acknowledge results for non-empty content.
    acknowledgeResults(ackSequence);
  }
}

void PrestoExchangeSource::processDataError(
    const std::string& path,
    uint32_t maxBytes,
    uint32_t maxWaitSeconds,
    const std::string& error) {
  ++failedAttempts_;
  if (!dataRequestRetryState_.isExhausted()) {
    VLOG(1) << "Failed to fetch data from " << host_ << ":" << port_ << " "
            << path << ", duration: " << dataRequestRetryState_.durationMs()
            << "ms - Retrying: " << error;

    doRequest(dataRequestRetryState_.nextDelayMs(), maxBytes, maxWaitSeconds);
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

bool PrestoExchangeSource::checkSetRequestPromise() {
  VeloxPromise<Response> promise;
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

void PrestoExchangeSource::acknowledgeResults(int64_t ackSequence) {
  auto ackPath = fmt::format("{}/{}/acknowledge", basePath_, ackSequence);
  VLOG(1) << "Sending ack " << ackPath;
  auto self = getSelfPtr();

  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(ackPath)
      .send(httpClient_.get())
      .via(driverExecutor_)
      .thenValue([self](std::unique_ptr<http::HttpResponse> response) {
        VLOG(1) << "Ack " << response->headers()->getStatusCode();
      })
      .thenError(
          folly::tag_t<std::exception>{}, [self](const std::exception& e) {
            // Acks are optional. No need to fail the query.
            VLOG(1) << "Ack failed: " << e.what();
          });
}

void PrestoExchangeSource::abortResults() {
  if (abortResultsIssued_.exchange(true)) {
    return;
  }

  abortRetryState_ =
      RetryState(std::chrono::duration_cast<std::chrono::milliseconds>(
                     SystemConfig::instance()->exchangeMaxErrorDuration())
                     .count());
  VLOG(1) << "Sending abort results " << basePath_;
  doAbortResults(abortRetryState_.nextDelayMs());
}

void PrestoExchangeSource::doAbortResults(int64_t delayMs) {
  auto queue = queue_;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::DELETE)
      .url(basePath_)
      .send(httpClient_.get(), "", delayMs)
      .via(driverExecutor_)
      .thenTry([queue, self](
                   folly::Try<std::unique_ptr<http::HttpResponse>> response) {
        std::optional<std::string> error;
        if (response.hasException()) {
          error = response.exception().what();
        } else {
          auto statusCode = response.value()->headers()->getStatusCode();
          if (statusCode != http::kHttpOk &&
              statusCode != http::kHttpNoContent) {
            error = std::to_string(statusCode);
          }
        }
        if (!error.has_value()) {
          return;
        }
        if (self->abortRetryState_.isExhausted()) {
          const std::string errMsg = fmt::format(
              "Abort results failed: {}, path {}",
              error.value(),
              self->basePath_);
          LOG(ERROR) << errMsg;
          return onFinalFailure(errMsg, queue);
        }
        self->doAbortResults(self->abortRetryState_.nextDelayMs());
      });
}

void PrestoExchangeSource::close() {
  closed_.store(true);
  checkSetRequestPromise();
  abortResults();
}

std::shared_ptr<PrestoExchangeSource> PrestoExchangeSource::getSelfPtr() {
  return std::dynamic_pointer_cast<PrestoExchangeSource>(shared_from_this());
}

const ConnectionPool& ConnectionPools::get(
    const proxygen::Endpoint& endpoint,
    folly::IOThreadPoolExecutor* ioExecutor) {
  return *pools_.withULockPtr([&](auto ulock) -> const ConnectionPool* {
    auto it = ulock->find(endpoint);
    if (it != ulock->end()) {
      return it->second.get();
    }
    auto wlock = ulock.moveFromUpgradeToWrite();
    auto& pool = (*wlock)[endpoint];
    if (!pool) {
      pool = std::make_unique<ConnectionPool>();
      pool->eventBase = ioExecutor->getEventBase();
      pool->sessionPool = std::make_unique<proxygen::SessionPool>(nullptr, 10);
      // Creation of the timer is not thread safe, so we do it here instead of
      // in the constructor of HttpClient.
      pool->eventBase->timer();
    }
    return pool.get();
  });
}

void ConnectionPools::destroy() {
  pools_.withWLock([](auto& pools) {
    for (auto& [_, pool] : pools) {
      pool->eventBase->runInEventBaseThread(
          [sessionPool = std::move(pool->sessionPool)] {});
    }
    pools.clear();
  });
}

namespace {

std::pair<folly::EventBase*, proxygen::SessionPool*> getSessionPool(
    ConnectionPools* connectionPools,
    folly::IOThreadPoolExecutor* ioExecutor,
    const proxygen::Endpoint& ep) {
  if (!connectionPools) {
    return {ioExecutor->getEventBase(), nullptr};
  }
  auto& connPool = connectionPools->get(ep, ioExecutor);
  return {connPool.eventBase, connPool.sessionPool.get()};
}

} // namespace

// static
std::shared_ptr<PrestoExchangeSource> PrestoExchangeSource::create(
    const std::string& url,
    int destination,
    const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
    velox::memory::MemoryPool* memoryPool,
    folly::CPUThreadPoolExecutor* cpuExecutor,
    folly::IOThreadPoolExecutor* ioExecutor,
    ConnectionPools* connectionPools) {
  folly::Uri uri(url);
  if (uri.scheme() == "http") {
    proxygen::Endpoint ep(uri.host(), uri.port(), false);
    auto [eventBase, sessionPool] =
        getSessionPool(connectionPools, ioExecutor, ep);
    return std::make_shared<PrestoExchangeSource>(
        uri,
        destination,
        queue,
        memoryPool,
        cpuExecutor,
        eventBase,
        sessionPool);
  }
  if (uri.scheme() == "https") {
    proxygen::Endpoint ep(uri.host(), uri.port(), true);
    auto [eventBase, sessionPool] =
        getSessionPool(connectionPools, ioExecutor, ep);
    const auto* systemConfig = SystemConfig::instance();
    const auto clientCertAndKeyPath =
        systemConfig->httpsClientCertAndKeyPath().value_or("");
    const auto ciphers = systemConfig->httpsSupportedCiphers();
    return std::make_shared<PrestoExchangeSource>(
        uri,
        destination,
        queue,
        memoryPool,
        cpuExecutor,
        eventBase,
        sessionPool,
        clientCertAndKeyPath,
        ciphers);
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

void PrestoExchangeSource::testingClearMemoryUsage() {
  currQueuedMemoryBytes() = 0;
  peakQueuedMemoryBytes() = 0;
}
} // namespace facebook::presto
