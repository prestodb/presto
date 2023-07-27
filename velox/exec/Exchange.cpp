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
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

SerializedPage::SerializedPage(
    std::unique_ptr<folly::IOBuf> iobuf,
    std::function<void(folly::IOBuf&)> onDestructionCb)
    : iobuf_(std::move(iobuf)),
      iobufBytes_(chainBytes(*iobuf_.get())),
      onDestructionCb_(onDestructionCb) {
  VELOX_CHECK_NOT_NULL(iobuf_);
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
}

void SerializedPage::prepareStreamForDeserialize(ByteStream* input) {
  input->resetInput(std::move(ranges_));
}

std::shared_ptr<ExchangeSource> ExchangeSource::create(
    const std::string& taskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* pool) {
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

    std::shared_ptr<ExchangeSource> source;
    try {
      source = ExchangeSource::create(taskId, destination_, queue_, pool_);
    } catch (const VeloxException& e) {
      throw;
    } catch (const std::exception& e) {
      // Task ID can be very long. Truncate to 256 characters.
      VELOX_FAIL(
          "Failed to create ExchangeSource: {}. Task ID: {}.",
          e.what(),
          taskId.substr(0, 126));
    }

    if (closed_) {
      toClose = std::move(source);
    } else {
      sources_.push_back(source);
      queue_->addSourceLocked();
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
  queue_->close();
}

folly::F14FastMap<std::string, RuntimeMetric> ExchangeClient::stats() const {
  folly::F14FastMap<std::string, RuntimeMetric> stats;
  std::lock_guard<std::mutex> l(queue_->mutex());
  for (const auto& source : sources_) {
    for (const auto& [name, value] : source->stats()) {
      stats[name].addValue(value);
    }
  }
  return stats;
}

std::unique_ptr<SerializedPage> ExchangeClient::next(
    bool* atEnd,
    ContinueFuture* future) {
  std::vector<std::shared_ptr<ExchangeSource>> toRequest;
  std::unique_ptr<SerializedPage> page;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    *atEnd = false;
    page = queue_->dequeueLocked(atEnd, future);
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

std::string ExchangeClient::toString() const {
  std::stringstream out;
  for (auto& source : sources_) {
    out << source->toString() << std::endl;
  }
  return out.str();
}

std::string ExchangeClient::toJsonString() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["closed"] = closed_;
  folly::dynamic clientsObj = folly::dynamic::object;
  int index = 0;
  for (auto& source : sources_) {
    clientsObj[std::to_string(index++)] = source->toJsonString();
  }
  return folly::toPrettyJson(obj);
}

bool Exchange::getSplits(ContinueFuture* future) {
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
        ++stats_.wlock()->numSplits;
      } else {
        exchangeClient_->noMoreRemoteTasks();
        noMoreSplits_ = true;
        if (atEnd_) {
          operatorCtx_->task()->multipleSplitsFinished(
              stats_.rlock()->numSplits);
          recordStats();
        }
        return false;
      }
    } else {
      return true;
    }
  }
}

BlockingReason Exchange::isBlocked(ContinueFuture* future) {
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
      const auto numSplits = stats_.rlock()->numSplits;
      operatorCtx_->task()->multipleSplitsFinished(numSplits);
      recordStats();
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
    return BlockingReason::kWaitForSplit;
  }

  // Block until data becomes available.
  *future = std::move(dataFuture);
  return BlockingReason::kWaitForProducer;
}

bool Exchange::isFinished() {
  return atEnd_;
}

RowVectorPtr Exchange::getOutput() {
  if (!currentPage_) {
    return nullptr;
  }

  uint64_t rawInputBytes{0};
  if (!inputStream_) {
    inputStream_ = std::make_unique<ByteStream>();
    rawInputBytes += currentPage_->size();
    currentPage_->prepareStreamForDeserialize(inputStream_.get());
  }

  getSerde()->deserialize(
      inputStream_.get(), operatorCtx_->pool(), outputType_, &result_);

  {
    auto lockedStats = stats_.wlock();
    lockedStats->rawInputBytes += rawInputBytes;
    lockedStats->addInputVector(result_->estimateFlatSize(), result_->size());
  }

  if (inputStream_->atEnd()) {
    currentPage_ = nullptr;
    inputStream_ = nullptr;
  }

  return result_;
}

void Exchange::recordStats() {
  auto lockedStats = stats_.wlock();
  for (const auto& [name, value] : exchangeClient_->stats()) {
    lockedStats->runtimeStats[name].merge(value);
  }
}

VectorSerde* Exchange::getSerde() {
  return getVectorSerde();
}

} // namespace facebook::velox::exec
