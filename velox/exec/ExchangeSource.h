/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/exec/ExchangeQueue.h"

namespace facebook::velox::exec {

class ExchangeSource : public std::enable_shared_from_this<ExchangeSource> {
 public:
  ExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<ExchangeQueue> queue,
      memory::MemoryPool* pool)
      : taskId_(taskId),
        destination_(destination),
        queue_(std::move(queue)),
        pool_(pool->shared_from_this()) {}

  virtual ~ExchangeSource() = default;

  static std::shared_ptr<ExchangeSource> create(
      const std::string& taskId,
      int destination,
      std::shared_ptr<ExchangeQueue> queue,
      memory::MemoryPool* pool);

  /// Temporary API to indicate whether 'metrics()' API
  /// is supported.
  virtual bool supportsMetrics() const {
    return false;
  }

  /// Returns true if there is no request to the source pending or if
  /// this should be retried. If true, the caller is expected to call
  /// request(). This is expected to be called while holding lock over
  /// queue_.mutex(). This sets the status of 'this' to be pending. The
  /// caller is thus expected to call request() without holding a lock over
  /// queue_.mutex(). This pattern prevents multiple exchange consumer
  /// threads from issuing the same request.
  virtual bool shouldRequestLocked() = 0;

  virtual bool isRequestPendingLocked() const {
    return requestPending_;
  }

  struct Response {
    /// Size of the response in bytes. Zero means response didn't contain any
    /// data.
    const int64_t bytes;

    /// Boolean indicating that there will be no more data.
    const bool atEnd;

    /// Number of bytes still buffered at the source.  Each element represent
    /// one page, and the consumer can choose to fetch a prefix of them
    /// according to the memory restriction.
    const std::vector<int64_t> remainingBytes;
  };

  /// Requests the producer to generate up to 'maxBytes' more data and reply
  /// within 'maxWait'. Returns a future that completes when producer responds
  /// either with 'data' or with a message indicating that all data has been
  /// already produced or data will take more time to produce.
  virtual folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) = 0;

  /// Ask for available data sizes that can be fetched.  Normally should not
  /// fetching any actual data (i.e. Response::bytes should be 0).  However for
  /// backward compatibility (e.g. communicating with coordinator), we allow
  /// small data (1MB) to be returned.
  virtual folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) = 0;

  /// Notifies that the engine needs some time to process already received data
  /// and may not request more for a while. The implementation may choose to
  /// release temporary buffers or pause fetching any new data until any of
  /// the 'request' or 'requestDataSizes' methods are called.
  virtual void pause() {};

  /// Close the exchange source. May be called before all data
  /// has been received and processed. This can happen in case
  /// of an error or an operator like Limit aborting the query
  /// once it received enough data.
  virtual void close() = 0;

  // Returns runtime statistics. ExchangeSource is expected to report
  // background CPU time by including a runtime metric named
  // ExchangeClient::kBackgroundCpuTimeMs.
  virtual folly::F14FastMap<std::string, int64_t> stats() const {
    VELOX_UNREACHABLE();
  }

  /// Returns runtime statistics. ExchangeSource is expected to report
  /// Specify units of individual counters in ExchangeSource.
  /// for an example: 'totalBytes ：count: 9, sum: 11.17GB, max: 1.39GB,
  /// min:  1.16GB'
  virtual folly::F14FastMap<std::string, RuntimeMetric> metrics() const {
    VELOX_NYI();
  }

  virtual std::string toString() {
    std::stringstream out;
    out << "[ExchangeSource " << taskId_ << ":" << destination_
        << (requestPending_ ? " pending " : "") << (atEnd_ ? " at end" : "");
    return out.str();
  }

  virtual folly::dynamic toJson() {
    folly::dynamic obj = folly::dynamic::object;
    obj["taskId"] = taskId_;
    obj["destination"] = destination_;
    obj["sequence"] = sequence_;
    obj["requestPending"] = requestPending_.load();
    obj["atEnd"] = atEnd_;
    return obj;
  }

  using Factory = std::function<std::shared_ptr<ExchangeSource>(
      const std::string& taskId,
      int destination,
      std::shared_ptr<ExchangeQueue> queue,
      memory::MemoryPool* pool)>;

  static bool registerFactory(Factory factory) {
    factories().push_back(factory);
    return true;
  }

  static std::vector<Factory>& factories();

  ExchangeQueue* testingQueue() const {
    return queue_.get();
  }

 protected:
  // ID of the task producing data
  const std::string taskId_;
  // Destination number of 'this' on producer
  const int destination_;
  const std::shared_ptr<ExchangeQueue> queue_{nullptr};
  // Holds a shared reference on the memory pool as it might be still possible
  // to be accessed by external components after the query task is destroyed.
  // For instance, in Prestissimo, there might be a pending http request issued
  // by PrestoExchangeSource to fetch data from the remote task. When the http
  // response returns back, the task might have already terminated and deleted
  // so we need to hold an additional shared reference on the memory pool to
  // keeps it alive.
  const std::shared_ptr<memory::MemoryPool> pool_;

  int64_t sequence_{0};
  std::atomic<bool> requestPending_{false};
  bool atEnd_{false};
};

} // namespace facebook::velox::exec
