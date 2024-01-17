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
  RequestSpec requestSpec;
  std::shared_ptr<ExchangeSource> toClose;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());

    bool duplicate = !remoteTaskIds_.insert(taskId).second;
    if (duplicate) {
      // Do not add sources twice. Presto protocol may add duplicate sources
      // and the task updates have no guarantees of arriving in order.
      return;
    }

    std::shared_ptr<ExchangeSource> source;
    try {
      source = ExchangeSource::create(taskId, destination_, queue_, pool_);
    } catch (const VeloxException&) {
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
      sources_.push_back(source);
      queue_->addSourceLocked();
      // Put new source into 'producingSources_' queue to prioritise fetching
      // from these to find out whether these are productive or not.
      producingSources_.push(source);

      requestSpec = pickSourcesToRequestLocked();
    }
  }

  // Outside of lock.
  if (toClose) {
    toClose->close();
  } else {
    request(requestSpec);
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
    if (source->supportsMetrics()) {
      for (const auto& [name, value] : source->metrics()) {
        if (UNLIKELY(stats.count(name) == 0)) {
          stats.insert(std::pair(name, RuntimeMetric(value.unit)));
        }
        stats[name].merge(value);
      }
    } else {
      for (const auto& [name, value] : source->stats()) {
        stats[name].addValue(value);
      }
    }
  }

  stats["peakBytes"] =
      RuntimeMetric(queue_->peakBytes(), RuntimeCounter::Unit::kBytes);
  stats["numReceivedPages"] = RuntimeMetric(queue_->receivedPages());
  stats["averageReceivedPageBytes"] = RuntimeMetric(
      queue_->averageReceivedPageBytes(), RuntimeCounter::Unit::kBytes);

  return stats;
}

std::vector<std::unique_ptr<SerializedPage>>
ExchangeClient::next(uint32_t maxBytes, bool* atEnd, ContinueFuture* future) {
  RequestSpec requestSpec;
  std::vector<std::unique_ptr<SerializedPage>> pages;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    *atEnd = false;
    pages = queue_->dequeueLocked(maxBytes, atEnd, future);
    if (*atEnd) {
      return pages;
    }

    if (!pages.empty() && queue_->totalBytes() > maxQueuedBytes_) {
      return pages;
    }

    requestSpec = pickSourcesToRequestLocked();
  }

  // Outside of lock
  request(requestSpec);
  return pages;
}

void ExchangeClient::request(const RequestSpec& requestSpec) {
  auto self = shared_from_this();
  for (auto& source : requestSpec.sources) {
    auto future = source->request(requestSpec.maxBytes, kDefaultMaxWaitSeconds);
    VELOX_CHECK(future.valid());
    std::move(future)
        .via(executor_)
        .thenValue([self, requestSource = source](auto&& response) {
          RequestSpec requestSpec;
          {
            std::lock_guard<std::mutex> l(self->queue_->mutex());
            if (self->closed_) {
              return;
            }
            if (!response.atEnd) {
              if (response.bytes > 0) {
                self->producingSources_.push(requestSource);
              } else {
                self->emptySources_.push(requestSource);
              }
            }
            requestSpec = self->pickSourcesToRequestLocked();
          }
          self->request(requestSpec);
        })
        .thenError(
            folly::tag_t<std::exception>{}, [self](const std::exception& e) {
              self->queue_->setError(e.what());
            });
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

int64_t ExchangeClient::getAveragePageSize() {
  auto averagePageSize =
      std::min<int64_t>(maxQueuedBytes_, queue_->averageReceivedPageBytes());
  if (averagePageSize == 0) {
    averagePageSize = 1 << 20; // 1 MB.
  }

  return averagePageSize;
}

int32_t ExchangeClient::getNumSourcesToRequestLocked(int64_t averagePageSize) {
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

void ExchangeClient::pickSourcesToRequestLocked(
    RequestSpec& requestSpec,
    int32_t numToRequest,
    std::queue<std::shared_ptr<ExchangeSource>>& sources) {
  while (requestSpec.sources.size() < numToRequest && !sources.empty()) {
    auto& source = sources.front();
    if (source->shouldRequestLocked()) {
      requestSpec.sources.push_back(source);
    }
    sources.pop();
  }
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

  RequestSpec requestSpec;
  requestSpec.maxBytes = averagePageSize;

  // Pick up to 'numToRequest' next sources to request data from. Prioritize
  // sources that return data.
  pickSourcesToRequestLocked(requestSpec, numToRequest, producingSources_);
  pickSourcesToRequestLocked(requestSpec, numToRequest, emptySources_);

  return requestSpec;
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
  obj["taskId"] = taskId_;
  obj["closed"] = closed_;
  folly::dynamic clientsObj = folly::dynamic::object;
  int index = 0;
  for (auto& source : sources_) {
    clientsObj[std::to_string(index++)] = source->toJsonString();
  }
  return folly::toPrettyJson(obj);
}

} // namespace facebook::velox::exec
