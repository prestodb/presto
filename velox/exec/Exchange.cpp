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
#include <velox/common/base/Exceptions.h>
#include <velox/common/memory/Memory.h>
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {

SerializedPage::SerializedPage(
    std::unique_ptr<folly::IOBuf> iobuf,
    memory::MemoryPool* pool,
    std::function<void(folly::IOBuf&)> onDestructionCb)
    : iobuf_(std::move(iobuf)),
      iobufBytes_(chainBytes(*iobuf_.get())),
      pool_(pool),
      onDestructionCb_(onDestructionCb) {
  VELOX_CHECK_NOT_NULL(iobuf_);
  if (pool_ != nullptr) {
    pool_->reserve(iobufBytes_);
  }
  for (auto& buf : *iobuf_) {
    int32_t bufSize = buf.size();
    ranges_.push_back(ByteRange{
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(buf.data())),
        bufSize,
        0});
  }
}

SerializedPage::~SerializedPage() {
  if (onDestructionCb_) {
    onDestructionCb_(*iobuf_.get());
  }
  if (pool_) {
    // Release the tracked memory consumption for the query
    pool_->release(iobufBytes_);
  }
}

void SerializedPage::prepareStreamForDeserialize(ByteStream* input) {
  input->resetInput(std::move(ranges_));
}

std::shared_ptr<ExchangeSource> ExchangeSource::create(
    const std::string& taskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* FOLLY_NONNULL pool) {
  for (auto& factory : factories()) {
    auto result = factory(taskId, destination, queue, pool);
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
      std::shared_ptr<ExchangeQueue> queue,
      memory::MemoryPool* FOLLY_NONNULL pool)
      : ExchangeSource(taskId, destination, queue, pool) {}

  bool shouldRequestLocked() override {
    if (atEnd_) {
      return false;
    }
    return !requestPending_.exchange(true);
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
            std::vector<std::unique_ptr<folly::IOBuf>> data, int64_t sequence) {
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
          for (auto& inputPage : data) {
            if (!inputPage) {
              atEnd = true;
              // Keep looping, there could be extra end markers.
              continue;
            }
            inputPage->unshare();
            pages.push_back(
                std::make_unique<SerializedPage>(std::move(inputPage), pool_));
            inputPage = nullptr;
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

  void close() override {
    auto buffers = PartitionedOutputBufferManager::getInstance().lock();
    buffers->deleteResults(taskId_, destination_);
  }

 private:
  static constexpr uint64_t kMaxBytes = 32 * 1024 * 1024; // 32 MB
};

std::unique_ptr<ExchangeSource> createLocalExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* FOLLY_NONNULL pool) {
  if (strncmp(taskId.c_str(), "local://", 8) == 0) {
    return std::make_unique<LocalExchangeSource>(
        taskId, destination, std::move(queue), pool);
  }
  return nullptr;
}

} // namespace

void ExchangeClient::initialize(memory::MemoryPool* FOLLY_NONNULL pool) {
  pool_ = pool;
}

void ExchangeClient::addRemoteTaskId(const std::string& taskId) {
  std::shared_ptr<ExchangeSource> toRequest;
  std::shared_ptr<ExchangeSource> toClose;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());

    bool duplicate = !taskIds_.insert(taskId).second;
    if (duplicate) {
      // Do not add sources twice. Presto protocol may add duplicate sources
      // and the task updates have no guarantees of arriving in order.
      return;
    }
    auto source = ExchangeSource::create(taskId, destination_, queue_, pool_);

    if (closed_) {
      toClose = std::move(source);
    } else {
      sources_.push_back(source);
      queue_->addSource();
      if (source->shouldRequestLocked()) {
        toRequest = source;
      }
    }
  }

  // Outside of lock.
  if (toClose) {
    toClose->close();
  } else if (toRequest) {
    toRequest->request();
  }
}

void ExchangeClient::noMoreRemoteTasks() {
  std::lock_guard<std::mutex> l(queue_->mutex());
  queue_->noMoreSources();
}

void ExchangeClient::close() {
  std::vector<std::shared_ptr<ExchangeSource>> sources;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (closed_) {
      return;
    }
    closed_ = true;
    sources = std::move(sources_);
  }

  // Outside of mutex.
  for (auto& source : sources) {
    source->close();
  }
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    queue_->closeLocked();
  }
}

std::unique_ptr<SerializedPage> ExchangeClient::next(
    bool* atEnd,
    ContinueFuture* future) {
  std::vector<std::shared_ptr<ExchangeSource>> toRequest;
  std::unique_ptr<SerializedPage> page;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    *atEnd = false;
    page = queue_->dequeue(atEnd, future);
    if (*atEnd) {
      return page;
    }
    if (page && queue_->totalBytes() > queue_->minBytes()) {
      return page;
    }
    // There is space for more data, send requests to sources with no pending
    // request.
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
  return page;
}

ExchangeClient::~ExchangeClient() {
  close();
}

std::string ExchangeClient::toString() {
  std::stringstream out;
  for (auto& source : sources_) {
    out << source->toString() << std::endl;
  }
  return out.str();
}

bool Exchange::getSplits(ContinueFuture* FOLLY_NONNULL future) {
  if (operatorCtx_->driverCtx()->driverId != 0) {
    // When there are multiple pipelines, a single operator, the one from
    // pipeline 0, is responsible for feeding splits into shared ExchangeClient.
    return false;
  }
  if (noMoreSplits_) {
    return false;
  }
  for (;;) {
    exec::Split split;
    auto reason = operatorCtx_->task()->getSplitOrFuture(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId_, split, *future);
    if (reason == BlockingReason::kNotBlocked) {
      if (split.hasConnectorSplit()) {
        auto remoteSplit = std::dynamic_pointer_cast<RemoteConnectorSplit>(
            split.connectorSplit);
        VELOX_CHECK(remoteSplit, "Wrong type of split");
        exchangeClient_->addRemoteTaskId(remoteSplit->taskId);
        ++stats_.numSplits;
      } else {
        exchangeClient_->noMoreRemoteTasks();
        noMoreSplits_ = true;
        if (atEnd_) {
          operatorCtx_->task()->multipleSplitsFinished(stats_.numSplits);
        }
        return false;
      }
    } else {
      return true;
    }
  }
}

BlockingReason Exchange::isBlocked(ContinueFuture* FOLLY_NONNULL future) {
  if (currentPage_ || atEnd_) {
    return BlockingReason::kNotBlocked;
  }

  // Start fetching data right away. Do not wait for all
  // splits to be available.

  if (!splitFuture_.valid()) {
    getSplits(&splitFuture_);
  }

  ContinueFuture dataFuture;
  currentPage_ = exchangeClient_->next(&atEnd_, &dataFuture);
  if (currentPage_ || atEnd_) {
    if (atEnd_ && noMoreSplits_) {
      operatorCtx_->task()->multipleSplitsFinished(stats_.numSplits);
    }
    return BlockingReason::kNotBlocked;
  }

  // We have a dataFuture and we may also have a splitFuture_.

  if (splitFuture_.valid()) {
    // Block until data becomes available or more splits arrive.
    std::vector<ContinueFuture> futures;
    futures.push_back(std::move(splitFuture_));
    futures.push_back(std::move(dataFuture));

    *future = folly::collectAny(futures).unit();
  } else {
    // Block until data becomes available.
    *future = std::move(dataFuture);
  }
  return stats_.numSplits == 0 ? BlockingReason::kWaitForSplit
                               : BlockingReason::kWaitForExchange;
}

bool Exchange::isFinished() {
  return atEnd_;
}

RowVectorPtr Exchange::getOutput() {
  if (!currentPage_) {
    return nullptr;
  }

  if (!inputStream_) {
    inputStream_ = std::make_unique<ByteStream>();
    stats_.rawInputBytes += currentPage_->size();
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

VELOX_REGISTER_EXCHANGE_SOURCE_METHOD_DEFINITION(
    ExchangeSource,
    createLocalExchangeSource);

} // namespace facebook::velox::exec
