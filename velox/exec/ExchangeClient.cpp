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
  std::vector<RequestSpec> requestSpecs;
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
      emptySources_.push(source);
      requestSpecs = pickSourcesToRequestLocked();
    }
  }

  // Outside of lock.
  if (toClose) {
    toClose->close();
  } else {
    request(std::move(requestSpecs));
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
  std::vector<RequestSpec> requestSpecs;
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

    requestSpecs = pickSourcesToRequestLocked();
  }

  // Outside of lock
  request(std::move(requestSpecs));
  return pages;
}

void ExchangeClient::request(std::vector<RequestSpec>&& requestSpecs) {
  auto self = shared_from_this();
  for (auto& spec : requestSpecs) {
    auto future = folly::SemiFuture<ExchangeSource::Response>::makeEmpty();
    if (spec.maxBytes == 0) {
      future = spec.source->requestDataSizes(kRequestDataSizesMaxWait);
    } else {
      future = spec.source->request(spec.maxBytes, kRequestDataMaxWait);
    }
    VELOX_CHECK(future.valid());
    std::move(future)
        .via(executor_)
        .thenValue([self, spec = std::move(spec)](auto&& response) {
          std::vector<RequestSpec> requestSpecs;
          {
            std::lock_guard<std::mutex> l(self->queue_->mutex());
            if (self->closed_) {
              return;
            }
            if (!response.atEnd) {
              if (!response.remainingBytes.empty()) {
                for (auto bytes : response.remainingBytes) {
                  VELOX_CHECK_GT(bytes, 0);
                }
                self->producingSources_.push(
                    {std::move(spec.source),
                     std::move(response.remainingBytes)});
              } else {
                self->emptySources_.push(std::move(spec.source));
              }
            }
            self->totalPendingBytes_ -= spec.maxBytes;
            requestSpecs = self->pickSourcesToRequestLocked();
          }
          self->request(std::move(requestSpecs));
        })
        .thenError(
            folly::tag_t<std::exception>{}, [self](const std::exception& e) {
              self->queue_->setError(e.what());
            });
  }
}

std::vector<ExchangeClient::RequestSpec>
ExchangeClient::pickSourcesToRequestLocked() {
  if (closed_) {
    return {};
  }
  std::vector<RequestSpec> requestSpecs;
  while (!emptySources_.empty()) {
    auto& source = emptySources_.front();
    VELOX_CHECK(source->shouldRequestLocked());
    requestSpecs.push_back({std::move(source), 0});
    emptySources_.pop();
  }
  int64_t availableSpace =
      maxQueuedBytes_ - queue_->totalBytes() - totalPendingBytes_;
  while (availableSpace > 0 && !producingSources_.empty()) {
    auto& source = producingSources_.front().source;
    int64_t requestBytes = 0;
    for (auto bytes : producingSources_.front().remainingBytes) {
      availableSpace -= bytes;
      if (availableSpace < 0) {
        break;
      }
      requestBytes += bytes;
    }
    if (requestBytes == 0) {
      VELOX_CHECK_LT(availableSpace, 0);
      break;
    }
    VELOX_CHECK(source->shouldRequestLocked());
    requestSpecs.push_back({std::move(source), requestBytes});
    producingSources_.pop();
    totalPendingBytes_ += requestBytes;
  }
  if (queue_->totalBytes() == 0 && totalPendingBytes_ == 0 &&
      !producingSources_.empty()) {
    // We have full capacity but still cannot initiate one single data transfer.
    // Let the transfer happen in this case to avoid getting stuck.
    auto& source = producingSources_.front().source;
    auto requestBytes = producingSources_.front().remainingBytes.at(0);
    LOG(INFO) << "Requesting large single page " << requestBytes
              << " bytes, exceeding capacity " << maxQueuedBytes_;
    VELOX_CHECK(source->shouldRequestLocked());
    requestSpecs.push_back({std::move(source), requestBytes});
    producingSources_.pop();
    totalPendingBytes_ += requestBytes;
  }
  return requestSpecs;
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

folly::dynamic ExchangeClient::toJson() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["taskId"] = taskId_;
  obj["closed"] = closed_;
  folly::dynamic clientsObj = folly::dynamic::object;
  int index = 0;
  for (auto& source : sources_) {
    clientsObj[std::to_string(index++)] = source->toJson();
  }
  obj["clients"] = clientsObj;
  return obj;
}

} // namespace facebook::velox::exec
