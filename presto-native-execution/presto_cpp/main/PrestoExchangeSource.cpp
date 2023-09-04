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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Operator.h"

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
    std::shared_ptr<exec::ExchangeQueue> queue) {
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
    pool->free(body->writableData(), body->capacity());
  }
  return oss.str();
}
} // namespace

PrestoExchangeSource::PrestoExchangeSource(
    const folly::Uri& baseUri,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool,
    const std::shared_ptr<folly::IOThreadPoolExecutor>& executor,
    const std::string& clientCertAndKeyPath,
    const std::string& ciphers)
    : ExchangeSource(extractTaskId(baseUri.path()), destination, queue, pool),
      basePath_(baseUri.path()),
      host_(baseUri.host()),
      port_(baseUri.port()),
      clientCertAndKeyPath_(clientCertAndKeyPath),
      ciphers_(ciphers),
      exchangeExecutor_(executor) {
  folly::SocketAddress address(folly::IPAddress(host_).str(), port_);
  auto timeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      SystemConfig::instance()->exchangeRequestTimeout());
  VELOX_CHECK_NOT_NULL(exchangeExecutor_.get());
  auto* eventBase = exchangeExecutor_->getEventBase();
  httpClient_ = std::make_shared<http::HttpClient>(
      eventBase,
      address,
      timeoutMs,
      pool_,
      clientCertAndKeyPath_,
      ciphers_,
      [](size_t bufferBytes) {
        REPORT_ADD_STAT_VALUE(kCounterHttpClientPrestoExchangeNumOnBody);
        REPORT_ADD_HISTOGRAM_VALUE(
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

velox::ContinueFuture PrestoExchangeSource::request(uint32_t maxBytes) {
  // Before calling 'request', the caller should have called
  // 'shouldRequestLocked' and received 'true' response. Hence, we expect
  // requestPending_ == true, atEnd_ == false.
  // This call cannot be made concurrently from multiple threads.
  VELOX_CHECK(requestPending_);
  VELOX_CHECK(!promise_.valid() || promise_.isFulfilled());

  auto [promise, future] =
      makeVeloxContinuePromiseContract("PrestoExchangeSource::request");

  promise_ = std::move(promise);
  failedAttempts_ = 0;
  retryState_ =
      RetryState(std::chrono::duration_cast<std::chrono::milliseconds>(
                     SystemConfig::instance()->exchangeMaxErrorDuration())
                     .count());
  doRequest(retryState_.nextDelayMs(), maxBytes);

  return std::move(future);
}

void PrestoExchangeSource::doRequest(int64_t delayMs, uint32_t maxBytes) {
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
      .send(httpClient_.get(), "", delayMs)
      .via(exchangeExecutor_.get())
      .thenValue([path, maxBytes, self](
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
              fmt::format(
                  "Received HTTP {} {} {}",
                  headers->getStatusCode(),
                  headers->getStatusMessage(),
                  bodyAsString(*response, self->pool_.get())));
        } else if (response->hasError()) {
          self->processDataError(path, maxBytes, response->error(), false);
        } else {
          self->processDataResponse(std::move(response));
        }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [path, maxBytes, self](const std::exception& e) {
            self->processDataError(path, maxBytes, e.what());
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
  std::unique_ptr<folly::IOBuf> singleChain;
  const bool empty = response->empty();
  int64_t totalBytes{0};
  if (!empty) {
    auto iobufs = response->consumeBody();
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

  REPORT_ADD_HISTOGRAM_VALUE(
      kCounterPrestoExchangeSerializedPageSize, page ? page->size() : 0);

  {
    ContinuePromise requestPromise;
    std::vector<ContinuePromise> queuePromises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      if (page) {
        VLOG(1) << "Enqueuing page for " << basePath_ << "/" << sequence_
                << ": " << page->size() << " bytes";
        ++numPages_;
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
      requestPromise.setValue();
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
    const std::string& error,
    bool retry) {
  ++failedAttempts_;
  if (retry && !retryState_.isExhausted()) {
    VLOG(1) << "Failed to fetch data from " << host_ << ":" << port_ << " "
            << path << " - Retrying: " << error;

    doRequest(retryState_.nextDelayMs(), maxBytes);
    return;
  }

  onFinalFailure(
      fmt::format(
          "Failed to fetch data from {}:{} {} - Exhausted retries: {}",
          host_,
          port_,
          path,
          error),
      queue_);

  if (!checkSetRequestPromise()) {
    // The source must have been closed.
    VELOX_CHECK(closed_.load());
  }
}

bool PrestoExchangeSource::checkSetRequestPromise() {
  ContinuePromise promise;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    promise = std::move(promise_);
  }
  if (promise.valid() && !promise.isFulfilled()) {
    promise.setValue();
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
      .via(exchangeExecutor_.get())
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
  VLOG(1) << "Sending abort results " << basePath_;
  auto queue = queue_;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::DELETE)
      .url(basePath_)
      .send(httpClient_.get())
      .via(exchangeExecutor_.get())
      .thenValue([queue, self](std::unique_ptr<http::HttpResponse> response) {
        auto statusCode = response->headers()->getStatusCode();
        if (statusCode != http::kHttpOk && statusCode != http::kHttpNoContent) {
          const std::string errMsg = fmt::format(
              "Abort results failed: {}, path {}", statusCode, self->basePath_);
          LOG(ERROR) << errMsg;
          onFinalFailure(errMsg, queue);
        } else {
          self->abortResultsSucceeded_.store(true);
        }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [queue, self](const std::exception& e) {
            const std::string errMsg = fmt::format(
                "Abort results failed: {}, path {}", e.what(), self->basePath_);
            LOG(ERROR) << errMsg;
            // Captures 'queue' by value to ensure lifetime. Error
            // detection can be arbitrarily late, for example after cancellation
            // due to other errors.
            onFinalFailure(errMsg, queue);
          });
}

void PrestoExchangeSource::close() {
  closed_.store(true);
  checkSetRequestPromise();
  if (!abortResultsSucceeded_.load()) {
    abortResults();
  }
}

std::shared_ptr<PrestoExchangeSource> PrestoExchangeSource::getSelfPtr() {
  return std::dynamic_pointer_cast<PrestoExchangeSource>(shared_from_this());
}

// static
std::unique_ptr<exec::ExchangeSource> PrestoExchangeSource::create(
    const std::string& url,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool,
    std::shared_ptr<folly::IOThreadPoolExecutor> executor) {
  if (strncmp(url.c_str(), "http://", 7) == 0) {
    return std::make_unique<PrestoExchangeSource>(
        folly::Uri(url), destination, queue, pool, executor);
  } else if (strncmp(url.c_str(), "https://", 8) == 0) {
    const auto systemConfig = SystemConfig::instance();
    const auto clientCertAndKeyPath =
        systemConfig->httpsClientCertAndKeyPath().value_or("");
    const auto ciphers = systemConfig->httpsSupportedCiphers();
    return std::make_unique<PrestoExchangeSource>(
        folly::Uri(url),
        destination,
        queue,
        pool,
        executor,
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
