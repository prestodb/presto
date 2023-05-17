/*
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
#include <fmt/format.h>
#include <folly/Uri.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/FileBroadcast.h"

namespace facebook::presto::operators {

namespace {
std::optional<std::string> getBroadcastBasePath(folly::Uri& uri) {
  for (auto& pair : uri.getQueryParams()) {
    if (pair.first == "broadcastBasePath") {
      return std::make_optional(pair.second);
    }
  }
  return std::nullopt;
}
} // namespace

void BroadcastExchangeSource::request() {
  std::vector<velox::ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (atEnd_) {
      return;
    }

    if (!reader_->hasNext()) {
      atEnd_ = true;
      queue_->enqueueLocked(nullptr, promises);
    } else {
      auto buffer = reader_->next();
      auto ioBuf = folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
      queue_->enqueueLocked(
          std::make_unique<velox::exec::SerializedPage>(
              std::move(ioBuf), [buffer](auto& /*unused*/) {}),
          promises);
    }
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
}

folly::F14FastMap<std::string, int64_t> BroadcastExchangeSource::stats() const {
  return {{"file size", 100}};
}

// static
std::unique_ptr<velox::exec::ExchangeSource>
BroadcastExchangeSource::createExchangeSource(
    const std::string& url,
    int destination,
    std::shared_ptr<velox::exec::ExchangeQueue> queue,
    velox::memory::MemoryPool* pool) {
  if (::strncmp(url.c_str(), "batch://", 8) != 0) {
    return nullptr;
  }

  auto uri = folly::Uri(url);
  const auto& broadcastBasePath = getBroadcastBasePath(uri);
  if (!broadcastBasePath.has_value()) {
    return nullptr;
  }

  auto fileSystemBroadcast = FileBroadcast(broadcastBasePath.value());
  return std::make_unique<BroadcastExchangeSource>(
      uri.host(),
      destination,
      std::move(queue),
      fileSystemBroadcast.createReader(broadcastBasePath.value(), pool),
      pool);
}
}; // namespace facebook::presto::operators
