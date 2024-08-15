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
#include "velox/exec/Merge.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {
namespace {

class LocalMergeSource : public MergeSource {
 public:
  explicit LocalMergeSource(int queueSize)
      : queue_(LocalMergeSourceQueue(queueSize)) {}

  BlockingReason next(RowVectorPtr& data, ContinueFuture* future) override {
    return queue_.withWLock(
        [&](auto& queue) { return queue.next(data, future); });
  }

  BlockingReason enqueue(RowVectorPtr input, ContinueFuture* future) override {
    return queue_.withWLock(
        [&](auto& queue) { return queue.enqueue(input, future); });
  }

  void close() override {}

 private:
  class LocalMergeSourceQueue {
   public:
    explicit LocalMergeSourceQueue(int queueSize) : data_(queueSize) {}

    BlockingReason next(RowVectorPtr& data, ContinueFuture* future) {
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

      notifyProducers();

      return BlockingReason::kNotBlocked;
    }

    BlockingReason enqueue(RowVectorPtr input, ContinueFuture* future) {
      if (!input) {
        atEnd_ = true;
        notifyConsumers();
        return BlockingReason::kNotBlocked;
      }
      VELOX_CHECK(!data_.full(), "LocalMergeSourceQueue is full");

      for (auto& child : input->children()) {
        child->loadedVector();
      }

      data_.push_back(input);
      notifyConsumers();

      if (data_.full()) {
        producerPromises_.emplace_back("LocalMergeSourceQueue::enqueue");
        *future = producerPromises_.back().getSemiFuture();
        return BlockingReason::kWaitForConsumer;
      }
      return BlockingReason::kNotBlocked;
    }

   private:
    void notifyConsumers() {
      for (auto& promise : consumerPromises_) {
        promise.setValue();
      }
      consumerPromises_.clear();
    }

    void notifyProducers() {
      for (auto& promise : producerPromises_) {
        promise.setValue();
      }
      producerPromises_.clear();
    }

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
            pool,
            executor)) {
    client_->addRemoteTaskId(taskId);
    client_->noMoreRemoteTasks();
  }

  BlockingReason next(RowVectorPtr& data, ContinueFuture* future) override {
    data.reset();

    if (atEnd_ && !currentPage_) {
      return BlockingReason::kNotBlocked;
    }

    if (!currentPage_) {
      auto pages = client_->next(1, &atEnd_, future);
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
          &data);

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
  MergeExchange* const mergeExchange_;
  std::shared_ptr<ExchangeClient> client_;
  std::unique_ptr<ByteInputStream> inputStream_;
  std::unique_ptr<SerializedPage> currentPage_;
  bool atEnd_ = false;

  BlockingReason enqueue(RowVectorPtr input, ContinueFuture* future) override {
    VELOX_FAIL();
  }
};
} // namespace

std::shared_ptr<MergeSource> MergeSource::createLocalMergeSource() {
  // Buffer up to 2 vectors from each source before blocking to wait
  // for consumers.
  static const int kDefaultQueueSize = 2;
  return std::make_shared<LocalMergeSource>(kDefaultQueueSize);
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

namespace {
void notify(std::optional<ContinuePromise>& promise) {
  if (promise) {
    promise->setValue();
    promise.reset();
  }
}
} // namespace

BlockingReason MergeJoinSource::next(
    ContinueFuture* future,
    RowVectorPtr* data) {
  common::testutil::TestValue::adjust(
      "facebook::velox::exec::MergeSource::next", this);
  return state_.withWLock([&](auto& state) {
    if (state.data != nullptr) {
      *data = std::move(state.data);
      notify(producerPromise_);
      return BlockingReason::kNotBlocked;
    }

    if (state.atEnd) {
      data = nullptr;
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
      "facebook::velox::exec::MergeSource::enqueue", this);
  return state_.withWLock([&](auto& state) {
    if (state.atEnd) {
      // This can happen if consumer called close() because it doesn't need any
      // more data, or because the Task failed or was aborted and the Driver is
      // cleaning up.
      // TODO: Finish the pipeline early and avoid unnecessary computing.

      // Notify consumerPromise_ so the consumer doesn't hang indefinitely if
      // this is because the Driver is closing operators.
      notify(consumerPromise_);
      return BlockingReason::kNotBlocked;
    }

    if (data == nullptr) {
      state.atEnd = true;
      notify(consumerPromise_);
      return BlockingReason::kNotBlocked;
    }

    if (state.data != nullptr) {
      return waitForConsumer(future);
    }

    state.data = std::move(data);
    notify(consumerPromise_);

    return waitForConsumer(future);
  });
}

void MergeJoinSource::close() {
  state_.withWLock([&](auto& state) {
    state.data = nullptr;
    state.atEnd = true;
    notify(producerPromise_);
    notify(consumerPromise_);
  });
}
} // namespace facebook::velox::exec
