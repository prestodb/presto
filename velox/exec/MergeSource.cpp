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

#include "velox/exec/MergeSource.h"

#include <boost/circular_buffer.hpp>
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Merge.h"
#include "velox/vector/VectorStream.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
namespace {

namespace {
class ScopedPromiseNotification {
 public:
  explicit ScopedPromiseNotification(size_t initSize) {
    promises_.reserve(initSize);
  }

  ~ScopedPromiseNotification() {
    for (auto& promise : promises_) {
      promise.setValue();
    }
  }

  void add(std::vector<ContinuePromise>&& promises) {
    promises_.reserve(promises_.size() + promises.size());
    for (auto& promise : promises) {
      promises_.emplace_back(std::move(promise));
    }
    promises.clear();
  }

  void add(ContinuePromise&& promise) {
    promises_.emplace_back(std::move(promise));
  }

 private:
  std::vector<ContinuePromise> promises_;
};

void deferNotify(
    std::optional<ContinuePromise>& deferPromise,
    ScopedPromiseNotification& notification) {
  if (deferPromise.has_value()) {
    notification.add(std::move(deferPromise.value()));
    deferPromise.reset();
  }
}
} // namespace

class LocalMergeSource : public MergeSource {
 public:
  explicit LocalMergeSource(int queueSize)
      : queue_(LocalMergeSourceQueue(queueSize)) {}

  void start() override {
    TestValue::adjust("facebook::velox::exec::LocalMergeSource::start", this);
    ScopedPromiseNotification notification(1);
    queue_.withWLock([&](auto& queue) { queue.start(notification); });
  }

  BlockingReason started(ContinueFuture* future) override {
    return queue_.withWLock([&](auto& queue) { return queue.started(future); });
  }

  BlockingReason next(RowVectorPtr& data, ContinueFuture* future) override {
    ScopedPromiseNotification notification(1);
    return queue_.withWLock(
        [&](auto& queue) { return queue.next(data, future, notification); });
  }

  BlockingReason enqueue(RowVectorPtr input, ContinueFuture* future) override {
    ScopedPromiseNotification notification(1);
    return queue_.withWLock([&](auto& queue) {
      return queue.enqueue(input, future, notification);
    });
  }

  void close() override {}

 private:
  class LocalMergeSourceQueue {
   public:
    explicit LocalMergeSourceQueue(int queueSize) : data_(queueSize) {}

    void start(ScopedPromiseNotification& notification) {
      VELOX_CHECK(!started_);
      started_ = true;
      notifyProducers(notification);
    }

    BlockingReason started(ContinueFuture* future) {
      if (started_) {
        return BlockingReason::kNotBlocked;
      }
      producerPromises_.emplace_back("LocalMergeSourceQueue::started");
      *future = producerPromises_.back().getSemiFuture();
      return BlockingReason::kWaitForConsumer;
    }

    BlockingReason next(
        RowVectorPtr& data,
        ContinueFuture* future,
        ScopedPromiseNotification& notification) {
      VELOX_CHECK(started_);
      data.reset();

      if (data_.empty()) {
        if (atEnd_) {
          return BlockingReason::kNotBlocked;
        }
        consumerPromises_.emplace_back("LocalMergeSourceQueue::next");
        *future = consumerPromises_.back().getSemiFuture();
        return BlockingReason::kWaitForProducer;
      }

      data = data_.front();

      // advance to next batch.
      data_.pop_front();

      notifyProducers(notification);
      return BlockingReason::kNotBlocked;
    }

    BlockingReason enqueue(
        RowVectorPtr input,
        ContinueFuture* future,
        ScopedPromiseNotification& notification) {
      if (!input) {
        atEnd_ = true;
        notifyConsumers(notification);
        return BlockingReason::kNotBlocked;
      }
      VELOX_CHECK(started_);
      VELOX_CHECK(!data_.full(), "LocalMergeSourceQueue is full");

      for (auto& child : input->children()) {
        child->loadedVector();
      }

      data_.push_back(input);
      notifyConsumers(notification);

      if (data_.full()) {
        producerPromises_.emplace_back("LocalMergeSourceQueue::enqueue");
        *future = producerPromises_.back().getSemiFuture();
        return BlockingReason::kWaitForConsumer;
      }
      return BlockingReason::kNotBlocked;
    }

   private:
    void notifyConsumers(ScopedPromiseNotification& notification) {
      notification.add(std::move(consumerPromises_));
      VELOX_CHECK(consumerPromises_.empty());
    }

    void notifyProducers(ScopedPromiseNotification& notification) {
      notification.add(std::move(producerPromises_));
      VELOX_CHECK(producerPromises_.empty());
    }

    bool started_{false};
    bool atEnd_{false};
    boost::circular_buffer<RowVectorPtr> data_;
    std::vector<ContinuePromise> consumerPromises_;
    std::vector<ContinuePromise> producerPromises_;
  };

  folly::Synchronized<LocalMergeSourceQueue> queue_;
};

class MergeExchangeSource : public MergeSource {
 public:
  MergeExchangeSource(
      MergeExchange* mergeExchange,
      const std::string& taskId,
      int destination,
      int64_t maxQueuedBytes,
      memory::MemoryPool* pool,
      folly::Executor* executor)
      : mergeExchange_(mergeExchange),
        client_(std::make_shared<ExchangeClient>(
            mergeExchange->taskId(),
            destination,
            maxQueuedBytes,
            1,
            // Deliver right away to avoid blocking other sources
            0,
            pool,
            executor)) {
    client_->addRemoteTaskId(taskId);
    client_->noMoreRemoteTasks();
  }

  ~MergeExchangeSource() override {
    close();
  }

  void start() override {}

  BlockingReason started(ContinueFuture* /*unused*/) override {
    VELOX_NYI();
  }

  BlockingReason next(RowVectorPtr& data, ContinueFuture* future) override {
    data.reset();

    if (atEnd_ && !currentPage_) {
      return BlockingReason::kNotBlocked;
    }

    if (!currentPage_) {
      auto pages = client_->next(0, 1, &atEnd_, future);
      VELOX_CHECK_LE(pages.size(), 1);
      currentPage_ = pages.empty() ? nullptr : std::move(pages.front());

      if (!currentPage_) {
        if (atEnd_) {
          return BlockingReason::kNotBlocked;
        }
        return BlockingReason::kWaitForProducer;
      }
    }
    if (inputStream_ == nullptr) {
      mergeExchange_->stats().wlock()->rawInputBytes += currentPage_->size();
      inputStream_ = currentPage_->prepareStreamForDeserialize();
    }

    if (!inputStream_->atEnd()) {
      VectorStreamGroup::read(
          inputStream_.get(),
          mergeExchange_->pool(),
          mergeExchange_->outputType(),
          mergeExchange_->serde(),
          &data,
          mergeExchange_->serdeOptions());

      auto lockedStats = mergeExchange_->stats().wlock();
      lockedStats->addInputVector(data->estimateFlatSize(), data->size());
      lockedStats->rawInputPositions += data->size();
    }

    // Since VectorStreamGroup::read() may cause inputStream to be at end,
    // check again and reset currentPage_ and inputStream_ here.
    if (inputStream_->atEnd()) {
      // Reached end of the stream.
      currentPage_ = nullptr;
      inputStream_.reset();
    }

    return BlockingReason::kNotBlocked;
  }

  void close() override {
    if (client_) {
      client_->close();
      client_ = nullptr;
    }
  }

 private:
  BlockingReason enqueue(RowVectorPtr input, ContinueFuture* future) override {
    VELOX_FAIL();
  }

  MergeExchange* const mergeExchange_;

  std::shared_ptr<ExchangeClient> client_;
  std::unique_ptr<ByteInputStream> inputStream_;
  std::unique_ptr<SerializedPage> currentPage_;
  bool atEnd_ = false;
};
} // namespace

std::shared_ptr<MergeSource> MergeSource::createLocalMergeSource(
    int queueSize) {
  return std::make_shared<LocalMergeSource>(queueSize);
}

std::shared_ptr<MergeSource> MergeSource::createMergeExchangeSource(
    MergeExchange* mergeExchange,
    const std::string& taskId,
    int destination,
    int64_t maxQueuedBytes,
    memory::MemoryPool* pool,
    folly::Executor* executor) {
  return std::make_shared<MergeExchangeSource>(
      mergeExchange, taskId, destination, maxQueuedBytes, pool, executor);
}

BlockingReason MergeJoinSource::next(
    ContinueFuture* future,
    RowVectorPtr* data,
    bool& drained) {
  drained = false;
  common::testutil::TestValue::adjust(
      "facebook::velox::exec::MergeJoinSource::next", this);
  ScopedPromiseNotification notification(1);
  return state_.withWLock([&](auto& state) {
    VELOX_CHECK_LE(!!state.atEnd + !!state.drained, 1);
    if (state.data != nullptr) {
      *data = std::move(state.data);

      deferNotify(producerPromise_, notification);
      return BlockingReason::kNotBlocked;
    }

    if (state.atEnd) {
      data = nullptr;
      return BlockingReason::kNotBlocked;
    }

    if (state.drained) {
      drained = true;
      state.drained = false;
      deferNotify(producerPromise_, notification);
      return BlockingReason::kNotBlocked;
    }

    consumerPromise_ = ContinuePromise("MergeJoinSource::next");
    *future = consumerPromise_->getSemiFuture();
    return BlockingReason::kWaitForProducer;
  });
}

BlockingReason MergeJoinSource::enqueue(
    RowVectorPtr data,
    ContinueFuture* future) {
  common::testutil::TestValue::adjust(
      "facebook::velox::exec::MergeJoinSource::enqueue", this);
  ScopedPromiseNotification notification(1);
  return state_.withWLock([&](auto& state) {
    if (state.atEnd) {
      // This can happen if consumer called close() because it doesn't need any
      // more data, or because the Task failed or was aborted and the Driver is
      // cleaning up.
      // TODO: Finish the pipeline early and avoid unnecessary computing.

      // Notify consumerPromise_ so the consumer doesn't hang indefinitely if
      // this is because the Driver is closing operators.
      deferNotify(consumerPromise_, notification);
      return BlockingReason::kNotBlocked;
    }

    if (data == nullptr) {
      state.atEnd = true;
      deferNotify(consumerPromise_, notification);
      return BlockingReason::kNotBlocked;
    }

    VELOX_CHECK_NULL(state.data);
    state.data = std::move(data);
    deferNotify(consumerPromise_, notification);
    return waitForConsumer(future);
  });
}

void MergeJoinSource::drain() {
  ScopedPromiseNotification notification(1);
  state_.withWLock([&](auto& state) {
    VELOX_CHECK(!state.atEnd);
    VELOX_CHECK(!state.drained);
    state.drained = true;
    deferNotify(consumerPromise_, notification);
  });
}

void MergeJoinSource::close() {
  ScopedPromiseNotification notification(2);
  state_.withWLock([&](auto& state) {
    state.data = nullptr;
    state.atEnd = true;
    deferNotify(producerPromise_, notification);
    deferNotify(consumerPromise_, notification);
  });
}
} // namespace facebook::velox::exec
