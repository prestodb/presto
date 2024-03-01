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

namespace facebook::presto::operators {

namespace {
std::optional<std::string> getBroadcastInfo(folly::Uri& uri) {
  for (auto& pair : uri.getQueryParams()) {
    if (pair.first == "broadcastInfo") {
      return std::make_optional(pair.second);
    }
  }
  return std::nullopt;
}
} // namespace

folly::SemiFuture<BroadcastExchangeSource::Response>
BroadcastExchangeSource::request(
    uint32_t /*maxBytes*/,
    uint32_t /*maxWaitSeconds*/) {
  if (atEnd_) {
    return folly::makeFuture(Response{0, true});
  }

  atEnd_ = !reader_->hasNext();
  int64_t totalBytes = 0;
  std::unique_ptr<velox::exec::SerializedPage> page;
  if (!atEnd_) {
    // Read outside the lock to avoid a potential deadlock
    // ExchangeClient guarantees not to call ExchangeSource#request concurrently
    auto buffer = reader_->next();
    totalBytes = buffer->size();
    auto ioBuf = folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
    page = std::make_unique<velox::exec::SerializedPage>(
        std::move(ioBuf), [buffer](auto& /*unused*/) {});
  }

  std::vector<velox::ContinuePromise> promises;
  {
    // Limit locking scope to queue manipulation
    std::lock_guard<std::mutex> l(queue_->mutex());
    queue_->enqueueLocked(std::move(page), promises);
  }
  for (auto& promise : promises) {
    promise.setValue();
  }

  return folly::makeFuture(Response{totalBytes, atEnd_});
}

folly::F14FastMap<std::string, int64_t> BroadcastExchangeSource::stats() const {
  return reader_->stats();
}

folly::SemiFuture<BroadcastExchangeSource::Response>
BroadcastExchangeSource::requestDataSizes(uint32_t /*maxWaitSeconds*/) {
  std::vector<int64_t> remainingBytes;
  if (!atEnd_) {
    // Use default value of ExchangeClient::getAveragePageSize() for now.
    //
    // TODO: Change BroadcastFileReader to return the next batch size.
    remainingBytes.push_back(1 << 20);
  }
  return folly::makeSemiFuture(Response{0, atEnd_, std::move(remainingBytes)});
}

// static
std::shared_ptr<exec::ExchangeSource>
BroadcastExchangeSource::createExchangeSource(
    const std::string& url,
    int destination,
    const std::shared_ptr<exec::ExchangeQueue>& queue,
    memory::MemoryPool* pool) {
  if (::strncmp(url.c_str(), "batch://", 8) != 0) {
    return nullptr;
  }

  auto uri = folly::Uri(url);
  const auto& broadcastInfoJson = getBroadcastInfo(uri);
  if (!broadcastInfoJson.has_value()) {
    return nullptr;
  }

  std::unique_ptr<BroadcastFileInfo> broadcastFileInfo;
  try {
    broadcastFileInfo =
        BroadcastFileInfo::deserialize(broadcastInfoJson.value());
  } catch (const VeloxException& e) {
    throw;
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("BroadcastInfo deserialization failed: {}", e.what());
  }

  auto fileSystemBroadcast = BroadcastFactory(broadcastFileInfo->filePath_);
  return std::make_shared<BroadcastExchangeSource>(
      uri.host(),
      destination,
      queue,
      fileSystemBroadcast.createReader(std::move(broadcastFileInfo), pool),
      pool);
}
}; // namespace facebook::presto::operators
