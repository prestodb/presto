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

#include <folly/Uri.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Retrying.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/Exchange.h"

namespace facebook::presto {

class PrestoExchangeSource : public velox::exec::ExchangeSource {
 public:
  class RetryState {
   public:
    RetryState(int64_t maxWaitMs = 1'000)
        : maxWaitMs_(maxWaitMs), startMs_(velox::getCurrentTimeMs()) {}

    // Returns the delay in millis to wait before next try. This is an
    // exponential backoff delay with jitter. The first call to this always
    // returns 0.
    int64_t nextDelayMs() {
      if (++numTries_ == 1) {
        return 0;
      }
      auto rng = folly::ThreadLocalPRNG();
      return folly::futures::detail::retryingJitteredExponentialBackoffDur(
                 numTries_ - 1,
                 std::chrono::milliseconds(kMinBackoffMs),
                 std::chrono::milliseconds(kMaxBackoffMs),
                 kJitterParam,
                 rng)
          .count();
    }

    // Returns whether we have exhausted all retries. We only retry if we spent
    // less than maxWaitMs_ time after we first started.
    bool isExhausted() const {
      return velox::getCurrentTimeMs() - startMs_ > maxWaitMs_;
    }

   private:
    int64_t maxWaitMs_;
    int64_t startMs_;
    size_t numTries_{0};

    static constexpr int64_t kMinBackoffMs = 100;
    static constexpr int64_t kMaxBackoffMs = 10000;
    static constexpr double kJitterParam = 0.1;
  };

  PrestoExchangeSource(
      const folly::Uri& baseUri,
      int destination,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      velox::memory::MemoryPool* pool,
      folly::CPUThreadPoolExecutor* driverExecutor,
      folly::IOThreadPoolExecutor* httpExecutor,
      const std::string& clientCertAndKeyPath_ = "",
      const std::string& ciphers_ = "");

  bool supportsFlowControlV2() const override {
    return true;
  }

  /// Returns 'true' is there is no request in progress, this source is not at
  /// end and most recent request hasn't failed. Transitions into
  /// 'request-pending' state if not there already. The caller must follow up
  /// with a call to 'request()' if this method returns true. The caller must
  /// hold a lock over queue's mutex while making this call.
  bool shouldRequestLocked() override;

  /// Requests up to 'maxBytes' from the upstream worker. Returns a future that
  /// completes after successful response is received from the upstream worker
  /// and the data received (if any) has been added to the queue. The future
  /// completes even if response came back empty. Failed responses are retried
  /// until SystemConfig::exchangeMaxErrorDuration() timeout expires. Retries
  /// use exponential backoff starting at 100ms and going up to 10s. Final
  /// failure is reported to the queue and completes the future.
  ///
  /// This method should not be called concurrently. The caller must receive
  /// 'true' from shouldRequestLocked() before calling this method. The caller
  /// should not hold a lock over queue's mutex when making this call.
  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      uint32_t maxWaitSeconds) override;

  static std::shared_ptr<ExchangeSource> create(
      const std::string& url,
      int destination,
      const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
      velox::memory::MemoryPool* pool,
      folly::CPUThreadPoolExecutor* cpuExecutor,
      folly::IOThreadPoolExecutor* ioExecutor);

  /// Completes the future returned by 'request()' if it hasn't completed
  /// already.
  void close() override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {
        {"prestoExchangeSource.numPages", numPages_},
        {"prestoExchangeSource.totalBytes", totalBytes_},
    };
  }

  std::string toJsonString() override {
    folly::dynamic obj = folly::dynamic::object;
    obj["taskId"] = taskId_;
    obj["destination"] = destination_;
    obj["sequence"] = sequence_;
    obj["requestPending"] = requestPending_.load();
    obj["basePath"] = basePath_;
    obj["host"] = host_;
    obj["numPages"] = numPages_;
    obj["totalBytes"] = totalBytes_;
    obj["closed"] = std::to_string(closed_);
    obj["abortResultsIssued"] = std::to_string(abortResultsIssued_);
    obj["atEnd"] = atEnd_;
    return folly::toPrettyJson(obj);
  }

  int testingFailedAttempts() const {
    return failedAttempts_;
  }

  /// Invoked to track the node-wise memory usage queued in
  /// PrestoExchangeSource. If 'updateBytes' > 0, then increment the usage,
  /// otherwise decrement the usage.
  static void updateMemoryUsage(int64_t updateBytes);

  /// Invoked to get the node-wise queued memory usage from
  /// PrestoExchangeSource.
  static void getMemoryUsage(int64_t& currentBytes, int64_t& peakBytes);

  /// Invoked to reset the node-wise peak memory usage back to the current
  /// memory usage in PrestoExchangeSource. Instead of getting all time peak,
  /// this can be useful when tracking the peak within some fixed time
  /// intervals.
  static void resetPeakMemoryUsage();

  /// Used by test to clear the node-wise memory usage tracking.
  static void testingClearMemoryUsage();

 private:
  void doRequest(int64_t delayMs, uint32_t maxBytes, uint32_t maxWaitSeconds);

  /// Handles successful, possibly empty, response. Adds received data to the
  /// queue. If received an end marker, notifies the queue by adding null page.
  /// Completes the future returned by 'request()' unless it has been completed
  /// already by a call to 'close()'. Sends an ack if received non-empty
  /// response without an end marker. Sends delete-results if received an end
  /// marker. The sequence of operations is: add data or end marker to the
  /// queue; complete the future, send ack or delete-results.
  void processDataResponse(std::unique_ptr<http::HttpResponse> response);

  /// If 'retry' is true, then retry the http request failure until reaches the
  /// retry limit, otherwise just set exchange source error without retry. As
  /// for now, we don't retry on the request failure which is caused by the
  /// memory allocation failure for the http response data.
  ///
  /// Upon final failure, completes the future returned from 'request'.
  void processDataError(
      const std::string& path,
      uint32_t maxBytes,
      uint32_t maxWaitSeconds,
      const std::string& error,
      bool retry = true);

  void acknowledgeResults(int64_t ackSequence);

  void abortResults();

  /// Send abort results after specified delay. This function is called
  /// multiple times by abortResults for retries.
  void doAbortResults(int64_t delayMs);

  /// Completes the future returned from 'request()' if it hasn't completed
  /// already.
  bool checkSetRequestPromise();

  // Returns a shared ptr owning the current object.
  std::shared_ptr<PrestoExchangeSource> getSelfPtr();

  // Tracks the currently node-wide queued memory usage in bytes.
  static std::atomic<int64_t>& currQueuedMemoryBytes() {
    static std::atomic<int64_t> currQueuedMemoryBytes{0};
    return currQueuedMemoryBytes;
  }

  // Records the node-wide peak queued memory usage in bytes.
  // Tracks the currently node-wide queued memory usage in bytes.
  static std::atomic<int64_t>& peakQueuedMemoryBytes() {
    static std::atomic<int64_t> peakQueuedMemoryBytes{0};
    return peakQueuedMemoryBytes;
  }

  const std::string basePath_;
  const std::string host_;
  const uint16_t port_;
  const std::string clientCertAndKeyPath_;
  const std::string ciphers_;
  const bool immediateBufferTransfer_;

  folly::CPUThreadPoolExecutor* const driverExecutor_;
  folly::IOThreadPoolExecutor* const httpExecutor_;

  std::shared_ptr<http::HttpClient> httpClient_;
  RetryState dataRequestRetryState_;
  RetryState abortRetryState_;
  int failedAttempts_;
  // The number of pages received from this presto exchange source.
  uint64_t numPages_{0};
  uint64_t totalBytes_{0};
  std::atomic_bool closed_{false};
  // A boolean indicating whether abortResults() call was issued
  std::atomic_bool abortResultsIssued_{false};
  velox::VeloxPromise<Response> promise_{
      velox::VeloxPromise<Response>::makeEmpty()};
};
} // namespace facebook::presto
