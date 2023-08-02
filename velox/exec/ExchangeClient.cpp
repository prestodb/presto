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
  std::vector<std::shared_ptr<ExchangeSource>> toRequest;
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

} // namespace facebook::velox::exec
