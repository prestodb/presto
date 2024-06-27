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
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <atomic>
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/OutputBufferManager.h"

namespace facebook::velox::exec::test {
namespace {

class LocalExchangeSource : public exec::ExchangeSource {
 public:
  LocalExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      memory::MemoryPool* pool)
      : ExchangeSource(taskId, destination, queue, pool) {}

  bool supportsMetrics() const override {
    return true;
  }

  bool shouldRequestLocked() override {
    if (atEnd_) {
      return false;
    }
    return !requestPending_.exchange(true);
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) override {
    ++numRequests_;

    auto promise = VeloxPromise<Response>("LocalExchangeSource::request");
    auto future = promise.getSemiFuture();

    promise_ = std::move(promise);

    auto buffers = OutputBufferManager::getInstance().lock();
    VELOX_CHECK_NOT_NULL(buffers, "invalid OutputBufferManager");
    VELOX_CHECK(requestPending_);
    auto requestedSequence = sequence_;
    auto self = shared_from_this();
    // Since this lambda may outlive 'this', we need to capture a
    // shared_ptr to the current object (self).
    auto resultCallback = [self, requestedSequence, buffers, this](
                              std::vector<std::unique_ptr<folly::IOBuf>> data,
                              int64_t sequence,
                              std::vector<int64_t> remainingBytes) {
      {
        std::lock_guard<std::mutex> l(mutex_);
        // This function is called either for a result or timeout. Only the
        // first of these calls has an effect.
        auto iter = timeouts_.find(self);
        if (iter == timeouts_.end()) {
          return;
        }
        timeouts_.erase(iter);
      }

      if (requestedSequence > sequence && !data.empty()) {
        VLOG(2) << "Receives earlier sequence than requested: task " << taskId_
                << ", destination " << destination_ << ", requested "
                << sequence << ", received " << requestedSequence;
        int64_t nExtra = requestedSequence - sequence;
        VELOX_CHECK(nExtra < data.size());
        data.erase(data.begin(), data.begin() + nExtra);
        sequence = requestedSequence;
      }
      if (data.empty()) {
        sequence = requestedSequence;
      }
      std::vector<std::unique_ptr<SerializedPage>> pages;
      bool atEnd = false;
      int64_t totalBytes = 0;
      for (auto& inputPage : data) {
        if (!inputPage) {
          atEnd = true;
          // Keep looping, there could be extra end markers.
          continue;
        }
        totalBytes += inputPage->length();
        inputPage->unshare();
        pages.push_back(std::make_unique<SerializedPage>(std::move(inputPage)));
        inputPage = nullptr;
      }
      numPages_ += pages.size();
      totalBytes_ += totalBytes;
      if (data.empty()) {
        common::testutil::TestValue::adjust(
            "facebook::velox::exec::test::LocalExchangeSource::timeout", this);
      }

      try {
        common::testutil::TestValue::adjust(
            "facebook::velox::exec::test::LocalExchangeSource", this);
      } catch (const std::exception& e) {
        queue_->setError(e.what());
        checkSetRequestPromise();
        return;
      }

      VeloxPromise<Response> requestPromise;
      {
        std::vector<ContinuePromise> queuePromises;
        {
          std::lock_guard<std::mutex> l(queue_->mutex());
          requestPending_ = false;
          requestPromise = std::move(promise_);
          for (auto& page : pages) {
            queue_->enqueueLocked(std::move(page), queuePromises);
          }
          if (atEnd) {
            queue_->enqueueLocked(nullptr, queuePromises);
            atEnd_ = true;
          }
          if (!data.empty()) {
            sequence_ = sequence + pages.size();
          }
        }
        for (auto& promise : queuePromises) {
          promise.setValue();
        }
      }
      // Outside of queue mutex.
      if (atEnd_) {
        buffers->deleteResults(taskId_, destination_);
      }

      if (!requestPromise.isFulfilled()) {
        requestPromise.setValue(Response{totalBytes, atEnd_, remainingBytes});
      }
    };

    registerTimeout(self, resultCallback, maxWait);

    buffers->getData(
        taskId_, destination_, maxBytes, sequence_, resultCallback);

    return future;
  }

  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override {
    return request(0, maxWait);
  }

  void pause() override {
    common::testutil::TestValue::adjust(
        "facebook::velox::exec::test::LocalExchangeSource::pause", nullptr);
    auto buffers = OutputBufferManager::getInstance().lock();
    VELOX_CHECK_NOT_NULL(buffers, "invalid OutputBufferManager");
    int64_t ackSequence;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      ackSequence = sequence_;
    }
    buffers->acknowledge(taskId_, destination_, ackSequence);
  }

  void close() override {
    checkSetRequestPromise();

    auto buffers = OutputBufferManager::getInstance().lock();
    buffers->deleteResults(taskId_, destination_);
  }

  folly::F14FastMap<std::string, RuntimeMetric> metrics() const override {
    return {
        {"localExchangeSource.numPages", RuntimeMetric(numPages_)},
        {"localExchangeSource.totalBytes",
         RuntimeMetric(totalBytes_, RuntimeCounter::Unit::kBytes)},
        {ExchangeClient::kBackgroundCpuTimeMs,
         RuntimeMetric(123 * 1000000, RuntimeCounter::Unit::kNanos)},
    };
  }

  // Invoked to start exchange source to run. It clears 'stop_' which allows
  // the background timeout executor to run on the first exchange source
  // request.
  static void start() {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_NULL(timerThread_);
    VELOX_CHECK(timeouts_.empty());
    stop_ = false;
  }

  // Invoked to stop the exchange source. It sets 'stop_', clears the current
  // pending request 'timeouts_' and join/destroy 'timeoutCheckExecutor_' if
  // created.
  static void stop() {
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (stop_) {
        VELOX_CHECK(timeouts_.empty());
        return;
      }
      stop_ = true;
      if (timerThread_ == nullptr) {
        VELOX_CHECK(timeouts_.empty());
        return;
      }
      timeouts_.clear();
    }
    timerThread_->join();
    timerThread_.reset();
  }

 private:
  using ResultCallback = std::function<void(
      std::vector<std::unique_ptr<folly::IOBuf>> data,
      int64_t sequence,
      std::vector<int64_t> remainingBytes)>;

  static void registerTimeout(
      const std::shared_ptr<ExchangeSource>& self,
      ResultCallback callback,
      std::chrono::microseconds maxWait) {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(!stop_, "Local exchange source has stopped");

    if (timerThread_ == nullptr) {
      timerThread_ = std::make_unique<std::thread>([&]() {
        while (!stop_) {
          auto now = std::chrono::system_clock::now();
          ResultCallback callback = nullptr;
          {
            std::lock_guard<std::mutex> t(mutex_);
            for (auto& pair : timeouts_) {
              if (pair.second.second < now) {
                callback = pair.second.first;
                break;
              }
            }
          }
          if (callback) {
            // Outside of mutex.
            callback({}, 0, {});
            continue;
          }
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
      });
      if (!exitInitialized_) {
        exitInitialized_ = true;
        atexit([]() { stop(); });
      }
    }
    timeouts_[self] =
        std::make_pair(callback, std::chrono::system_clock::now() + maxWait);
  }

  bool checkSetRequestPromise() {
    VeloxPromise<Response> promise;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      promise = std::move(promise_);
    }
    if (promise.valid() && !promise.isFulfilled()) {
      promise.setValue(Response{0, false, {}});
      return true;
    }

    return false;
  }

  static inline std::mutex mutex_;
  static inline folly::F14FastMap<
      std::shared_ptr<ExchangeSource>,
      std::pair<ResultCallback, std::chrono::system_clock::time_point>>
      timeouts_;
  static inline std::unique_ptr<std::thread> timerThread_;
  static inline std::atomic_bool stop_{false};
  static inline bool exitInitialized_{false};

  // Records the total number of pages fetched from sources.
  std::atomic<int64_t> numPages_{0};
  std::atomic<uint64_t> totalBytes_{0};
  VeloxPromise<Response> promise_{VeloxPromise<Response>::makeEmpty()};
  int32_t numRequests_{0};
};
} // namespace

std::unique_ptr<ExchangeSource> createLocalExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  if (strncmp(taskId.c_str(), "local://", 8) == 0) {
    return std::make_unique<LocalExchangeSource>(
        taskId, destination, std::move(queue), pool);
  } else if (strncmp(taskId.c_str(), "bad://", 6) == 0) {
    throw std::runtime_error("Testing error");
  }
  return nullptr;
}

void testingStartLocalExchangeSource() {
  LocalExchangeSource::start();
}

void testingShutdownLocalExchangeSource() {
  LocalExchangeSource::stop();
}

} // namespace facebook::velox::exec::test
