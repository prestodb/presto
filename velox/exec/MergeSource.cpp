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
        return BlockingReason::kWaitForExchange;
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
        promise.setValue(true);
      }
      consumerPromises_.clear();
    }

    void notifyProducers() {
      for (auto& promise : producerPromises_) {
        promise.setValue(true);
      }
      producerPromises_.clear();
    }

    bool atEnd_{false};
    boost::circular_buffer<RowVectorPtr> data_;
    std::vector<VeloxPromise<bool>> consumerPromises_;
    std::vector<VeloxPromise<bool>> producerPromises_;
  };

  folly::Synchronized<LocalMergeSourceQueue> queue_;
};

class MergeExchangeSource : public MergeSource {
 public:
  MergeExchangeSource(MergeExchange* mergeExchange, const std::string& taskId)
      : mergeExchange_(mergeExchange),
        client_(std::make_shared<ExchangeClient>(0)) {
    client_->addRemoteTaskId(taskId);
    client_->noMoreRemoteTasks();
  }

  BlockingReason next(RowVectorPtr& data, ContinueFuture* future) override {
    data.reset();

    if (atEnd_) {
      return BlockingReason::kNotBlocked;
    }

    if (!currentPage_) {
      currentPage_ = client_->next(&atEnd_, future);
      if (atEnd_) {
        return BlockingReason::kNotBlocked;
      }

      if (!currentPage_) {
        return BlockingReason::kWaitForExchange;
      }
    }
    if (!inputStream_) {
      inputStream_ = std::make_unique<ByteStream>();
      mergeExchange_->stats().rawInputBytes += currentPage_->size();
      currentPage_->prepareStreamForDeserialize(inputStream_.get());
    }

    if (!inputStream_->atEnd()) {
      VectorStreamGroup::read(
          inputStream_.get(),
          mergeExchange_->pool(),
          mergeExchange_->outputType(),
          &data);

      mergeExchange_->stats().inputPositions += data->size();
      mergeExchange_->stats().inputBytes += data->retainedSize();
    }

    // Since VectorStreamGroup::read() may cause inputStream to be at end,
    // check again and reset currentPage_ and inputStream_ here.
    if (inputStream_->atEnd()) {
      // Reached end of the stream.
      currentPage_ = nullptr;
      inputStream_ = nullptr;
    }

    return BlockingReason::kNotBlocked;
  }

 private:
  MergeExchange* mergeExchange_;
  std::shared_ptr<ExchangeClient> client_;
  std::unique_ptr<ByteStream> inputStream_;
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
    const std::string& taskId) {
  return std::make_shared<MergeExchangeSource>(mergeExchange, taskId);
}

namespace {
void notify(std::optional<VeloxPromise<bool>>& promise) {
  if (promise) {
    promise->setValue(true);
    promise.reset();
  }
}
} // namespace

BlockingReason MergeJoinSource::next(
    ContinueFuture* future,
    RowVectorPtr* data) {
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

    consumerPromise_ = VeloxPromise<bool>("MergeJoinSource::next");
    *future = consumerPromise_->getSemiFuture();
    return BlockingReason::kWaitForExchange;
  });
}

BlockingReason MergeJoinSource::enqueue(
    RowVectorPtr data,
    ContinueFuture* future) {
  return state_.withWLock([&](auto& state) {
    if (state.atEnd) {
      // This can happen if consumer called close() because it doesn't need any
      // more data.
      // TODO Finish the pipeline early and avoid unnecessary computing.
      return BlockingReason::kNotBlocked;
    }

    if (data == nullptr) {
      state.atEnd = true;
      notify(consumerPromise_);
      return BlockingReason::kNotBlocked;
    }

    VELOX_CHECK_NULL(state.data);
    state.data = std::move(data);
    notify(consumerPromise_);

    producerPromise_ = VeloxPromise<bool>("MergeJoinSource::enqueue");
    *future = producerPromise_->getSemiFuture();
    return BlockingReason::kWaitForConsumer;
  });
}

void MergeJoinSource::close() {
  state_.withWLock([&](auto& state) {
    state.data = nullptr;
    state.atEnd = true;
    notify(producerPromise_);
  });
}
} // namespace facebook::velox::exec
