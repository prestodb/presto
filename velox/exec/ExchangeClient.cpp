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
#include "velox/exec/ExchangeClient.h"

namespace facebook::velox::exec {

void ExchangeClient::addRemoteTaskId(const std::string& taskId) {
  RequestSpec toRequest;
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
      // Task ID can be very long. Truncate to 128 characters.
      VELOX_FAIL(
          "Failed to create ExchangeSource: {}. Task ID: {}.",
          e.what(),
          taskId.substr(0, 128));
    }

    if (closed_) {
      toClose = std::move(source);
    } else {
      if (!source->supportsFlowControl()) {
        allSourcesSupportFlowControl_ = false;
      }
      sources_.push_back(source);
      queue_->addSourceLocked();

      toRequest = pickSourcesToRequestLocked();
    }
  }

  // Outside of lock.
  if (toClose) {
    toClose->close();
  } else {
    request(toRequest);
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
  std::lock_guard<std::mutex> l(queue_->mutex());

  folly::F14FastMap<std::string, RuntimeMetric> stats;
  for (const auto& source : sources_) {
    for (const auto& [name, value] : source->stats()) {
      stats[name].addValue(value);
    }
  }

  stats["peakBytes"] =
      RuntimeMetric(queue_->peakBytes(), RuntimeCounter::Unit::kBytes);
  stats["numReceivedPages"] = RuntimeMetric(queue_->receivedPages());
  stats["averageReceivedPageBytes"] = RuntimeMetric(
      queue_->averageReceivedPageBytes(), RuntimeCounter::Unit::kBytes);

  return stats;
}

std::unique_ptr<SerializedPage> ExchangeClient::next(
    bool* atEnd,
    ContinueFuture* future) {
  RequestSpec toRequest;
  std::unique_ptr<SerializedPage> page;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    *atEnd = false;
    page = queue_->dequeueLocked(atEnd, future);
    if (*atEnd) {
      return page;
    }

    if (page && queue_->totalBytes() > maxQueuedBytes_) {
      return page;
    }

    toRequest = pickSourcesToRequestLocked();
  }

  // Outside of lock
  request(toRequest);
  return page;
}

void ExchangeClient::request(const RequestSpec& requestSpec) {
  for (auto& source : requestSpec.sources) {
    auto future = source->request(requestSpec.maxBytes);
    if (future.valid()) {
      auto& exec = folly::QueuedImmediateExecutor::instance();
      std::move(future)
          .via(&exec)
          .thenValue(
              [&](auto&& /* unused */) { request(pickSourcesToRequest()); })
          .thenError(
              folly::tag_t<std::exception>{},
              [&](const std::exception& e) { queue_->setError(e.what()); });
    }
  }
}

int32_t ExchangeClient::countPendingSourcesLocked() {
  int32_t numPending = 0;
  for (auto& source : sources_) {
    if (source->isRequestPendingLocked()) {
      ++numPending;
    }
  }
  return numPending;
}

ExchangeClient::RequestSpec ExchangeClient::pickSourcesToRequest() {
  std::lock_guard<std::mutex> l(queue_->mutex());
  return pickSourcesToRequestLocked();
}

int64_t ExchangeClient::getAveragePageSize() {
  auto averagePageSize =
      std::min<int64_t>(maxQueuedBytes_, queue_->averageReceivedPageBytes());
  if (averagePageSize == 0) {
    averagePageSize = 1 << 20; // 1 MB.
  }

  return averagePageSize;
}

int32_t ExchangeClient::getNumSourcesToRequestLocked(int64_t averagePageSize) {
  if (!allSourcesSupportFlowControl_) {
    return sources_.size();
  }

  // Figure out how many more 'averagePageSize' fit into 'maxQueuedBytes_'.
  // Make sure to leave room for 'numPending' pages.
  const auto numPending = countPendingSourcesLocked();

  auto numToRequest = std::max<int32_t>(
      1, (maxQueuedBytes_ - queue_->totalBytes()) / averagePageSize);
  if (numToRequest <= numPending) {
    return 0;
  }

  return numToRequest - numPending;
}

ExchangeClient::RequestSpec ExchangeClient::pickSourcesToRequestLocked() {
  if (closed_ || queue_->totalBytes() >= maxQueuedBytes_) {
    return {};
  }

  const auto averagePageSize = getAveragePageSize();
  const auto numToRequest = getNumSourcesToRequestLocked(averagePageSize);

  if (numToRequest == 0) {
    return {};
  }

  RequestSpec toRequest;
  toRequest.maxBytes = averagePageSize;

  // Pick up to 'numToRequest' next sources to request data from.
  for (auto i = 0; i < sources_.size(); ++i) {
    auto& source = sources_[nextSourceIndex_];

    nextSourceIndex_ = (nextSourceIndex_ + 1) % sources_.size();

    if (source->shouldRequestLocked()) {
      toRequest.sources.push_back(source);
      if (toRequest.sources.size() == numToRequest) {
        break;
      }
    }
  }

  return toRequest;
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

} // namespace facebook::velox::exec
