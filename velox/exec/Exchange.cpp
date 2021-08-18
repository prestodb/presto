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
#include "velox/exec/Exchange.h"
#include "velox/exec/PartitionedOutputBufferManager.h"

namespace facebook::velox::exec {

SerializedPage::SerializedPage(
    std::istream* stream,
    uint64_t size,
    memory::MappedMemory* memory)
    : allocation_(memory) {
  if (!memory->allocate(
          bits::roundUp(size, memory::MappedMemory::kPageSize) /
              memory::MappedMemory::kPageSize,
          kSerializedPageOwner,
          allocation_)) {
    VELOX_FAIL("Could not allocate memory for exchange input");
  }
  auto toRead = size;
  for (int i = 0; i < allocation_.numRuns(); ++i) {
    auto run = allocation_.runAt(i);
    auto runSize = run.numPages() * memory::MappedMemory::kPageSize;
    auto bytes = std::min<int32_t>(runSize, toRead);
    ranges_.push_back(ByteRange{run.data(), bytes, 0});
    stream->read(reinterpret_cast<char*>(run.data()), bytes);
    toRead -= bytes;
    if (!toRead) {
      break;
    }
  }

  VELOX_CHECK_EQ(toRead, 0);
}

void SerializedPage::prepareStreamForDeserialize(ByteStream* input) {
  input->resetInput(std::move(ranges_));
}

std::shared_ptr<ExchangeSource> ExchangeSource::create(
    const std::string& taskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue) {
  for (auto& factory : factories()) {
    auto result = factory(taskId, destination, queue);
    if (result) {
      return result;
    }
  }
  VELOX_FAIL("No ExchangeSource factory matches {}", taskId);
}

// static
std::vector<ExchangeSource::Factory>& ExchangeSource::factories() {
  static std::vector<Factory> factories;
  return factories;
}

namespace {
class LocalExchangeSource : public ExchangeSource {
 public:
  LocalExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<ExchangeQueue> queue)
      : ExchangeSource(taskId, destination, queue) {}

  bool shouldRequestLocked() override {
    if (atEnd_ && queue_->empty()) {
      return false;
    }
    bool pending = requestPending_;
    requestPending_ = true;
    return !pending;
  }

  void request() override {
    auto buffers = PartitionedOutputBufferManager::getInstance().lock();
    VELOX_CHECK_NOT_NULL(buffers, "invalid PartitionedOutputBufferManager");
    VELOX_CHECK(requestPending_);
    auto requestedSequence = sequence_;
    auto self = shared_from_this();
    buffers->getData(
        taskId_,
        destination_,
        kMaxBytes,
        sequence_,
        // Since this lambda may outlive 'this', we need to capture a
        // shared_ptr to the current object (self).
        [self, requestedSequence, buffers, this](
            std::vector<std::shared_ptr<VectorStreamGroup>>& data,
            int64_t sequence) {
          if (requestedSequence > sequence) {
            VLOG(2) << "Receives earlier sequence than requested: task "
                    << taskId_ << ", destination " << destination_
                    << ", requested " << sequence << ", received "
                    << requestedSequence;
            int64_t nExtra = requestedSequence - sequence;
            VELOX_CHECK(nExtra < data.size());
            data.erase(data.begin(), data.begin() + nExtra);
            sequence = requestedSequence;
          }
          std::vector<std::unique_ptr<SerializedPage>> pages;
          bool atEnd = false;
          for (auto& group : data) {
            if (!group) {
              atEnd = true;
              // Keep looping, there could be extra end markers.
              continue;
            }
            pages.push_back(SerializedPage::fromVectorStreamGroup(group.get()));
            group = nullptr;
          }
          int64_t ackSequence;
          {
            std::lock_guard<std::mutex> l(queue_->mutex());
            requestPending_ = false;
            for (auto& page : pages) {
              queue_->enqueue(std::move(page));
            }
            if (atEnd) {
              queue_->enqueue(nullptr);
              atEnd_ = true;
            }
            ackSequence = sequence_ = sequence + pages.size();
          }
          // Outside of queue mutex.
          if (atEnd_) {
            buffers->deleteResults(taskId_, destination_);
          } else {
            buffers->acknowledge(taskId_, destination_, ackSequence);
          }
        });
  }

  void close() override {}

 private:
  static constexpr uint64_t kMaxBytes = 32 * 1024 * 1024; // 32 MB
};

std::unique_ptr<ExchangeSource> createLocalExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue) {
  if (strncmp(taskId.c_str(), "local://", 8) == 0) {
    return std::make_unique<LocalExchangeSource>(
        taskId, destination, std::move(queue));
  }
  return nullptr;
}

} // namespace

void ExchangeClient::addRemoteTaskId(const std::string& taskId) {
  std::shared_ptr<ExchangeSource> toRequest;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    bool duplicate = !taskIds_.insert(taskId).second;
    if (duplicate) {
      // Do not add sources twice. Presto protocol may add duplicate sources
      // and the task updates have no guarantees of arriving in order.
      return;
    }
    auto source = ExchangeSource::create(taskId, destination_, queue_);
    sources_.push_back(source);
    queue_->addSource();
    if (source->shouldRequestLocked()) {
      toRequest = source;
    }
  }
  // Outside of lock
  toRequest->request();
}

void ExchangeClient::noMoreRemoteTasks() {
  std::lock_guard<std::mutex> l(queue_->mutex());
  queue_->noMoreSources();
}

std::unique_ptr<SerializedPage> ExchangeClient::next(
    bool* atEnd,
    ContinueFuture* future) {
  std::vector<std::shared_ptr<ExchangeSource>> toRequest;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    *atEnd = false;
    auto page = queue_->dequeue(atEnd, future);
    if (*atEnd || page) {
      return page;
    }
    // There is no data to return, send out more requests.
    for (auto& source : sources_) {
      if (source->shouldRequestLocked()) {
        toRequest.push_back(source);
      }
    }
  }

  // Outside of lock
  for (auto& source : toRequest) {
    source->request();
  }
  return nullptr;
}

ExchangeClient::~ExchangeClient() {
  for (auto& source : sources_) {
    source->close();
  }
}

std::string ExchangeClient::toString() {
  std::stringstream out;
  for (auto& source : sources_) {
    out << source->toString() << std::endl;
  }
  return out.str();
}

BlockingReason Exchange::getSplits(ContinueFuture* future) {
  if (operatorCtx_->driverCtx()->driverId != 0) {
    // When there are multiple pipelines, a single operator, the one from
    // pipeline 0, is responsible for feeding splits into shared ExchangeClient.
    return BlockingReason::kNotBlocked;
  }
  if (noMoreSplits_) {
    return BlockingReason::kNotBlocked;
  }
  for (;;) {
    exec::Split split;
    auto reason =
        operatorCtx_->task()->getSplitOrFuture(planNodeId_, split, *future);
    if (reason == BlockingReason::kNotBlocked) {
      if (split.hasConnectorSplit()) {
        auto remoteSplit = std::dynamic_pointer_cast<RemoteConnectorSplit>(
            split.connectorSplit);
        VELOX_CHECK(remoteSplit, "Wrong type of split");
        exchangeClient_->addRemoteTaskId(remoteSplit->taskId);
        ++numSplits_;
      } else {
        exchangeClient_->noMoreRemoteTasks();
        noMoreSplits_ = true;
        return BlockingReason::kNotBlocked;
      }
    } else {
      return BlockingReason::kWaitForSplit;
    }
  }
}

BlockingReason Exchange::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    blockingReason_ = BlockingReason::kNotBlocked;
    return BlockingReason::kWaitForExchange;
  }
  if (currentPage_) {
    return BlockingReason::kNotBlocked;
  }
  auto reason = getSplits(future);
  if (reason != BlockingReason::kNotBlocked) {
    blockingReason_ = BlockingReason::kNotBlocked;
    return reason;
  }
  currentPage_ = exchangeClient_->next(&atEnd_, future);
  if (currentPage_ || atEnd_) {
    return BlockingReason::kNotBlocked;
  }
  blockingReason_ = BlockingReason::kNotBlocked;
  return BlockingReason::kWaitForExchange;
}

void Exchange::finish() {
  Operator::finish();
  operatorCtx_->task()->multipleSplitsFinished(numSplits_);
}

RowVectorPtr Exchange::getOutput() {
  blockingReason_ = getSplits(&future_);
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return nullptr;
  }
  for (;;) {
    if (currentPage_) {
      if (!inputStream_) {
        inputStream_ = std::make_unique<ByteStream>();
        stats_.rawInputBytes += currentPage_->byteSize();
        currentPage_->prepareStreamForDeserialize(inputStream_.get());
      }

      VectorStreamGroup::read(
          inputStream_.get(), operatorCtx_->pool(), outputType_, &result_);

      stats_.inputPositions += result_->size();
      stats_.inputBytes += result_->retainedSize();

      if (inputStream_->atEnd()) {
        currentPage_ = nullptr;
        inputStream_ = nullptr;
      }

      return result_;
    }
    bool atEnd = false;
    currentPage_ = exchangeClient_->next(&atEnd, &future_);
    if (!currentPage_) {
      blockingReason_ = atEnd ? BlockingReason::kNotBlocked
                              : BlockingReason::kWaitForExchange;
      return nullptr;
    }
  }
}
VELOX_REGISTER_EXCHANGE_SOURCE_METHOD_DEFINITION(
    ExchangeSource,
    createLocalExchangeSource);

} // namespace facebook::velox::exec
