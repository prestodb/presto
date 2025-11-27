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

#include "presto_cpp/main/operators/BroadcastExchangeSource.h"

using namespace facebook::velox;

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
    uint32_t maxBytes,
    std::chrono::microseconds /*maxWait*/) {
  VELOX_CHECK_GT(maxBytes, 0);
  if (atEnd_) {
    return folly::makeFuture(Response{0, true});
  }

  return folly::makeTryWith([&]() -> Response {
    int64_t totalBytes = 0;
    std::vector<std::unique_ptr<velox::exec::SerializedPageBase>> pages;

    while (totalBytes < maxBytes && reader_->hasNext()) {
      auto buffer = reader_->next();
      VELOX_CHECK_NOT_NULL(buffer);

      auto ioBuf = folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
      auto page = std::make_unique<velox::exec::PrestoSerializedPage>(
          std::move(ioBuf), [buffer](auto& /*unused*/) {});
      pages.push_back(std::move(page));

      totalBytes += buffer->size();
    }

    atEnd_ = !reader_->hasNext();
    std::vector<velox::ContinuePromise> promises;
    {
      // Limit locking scope to queue manipulation
      std::lock_guard<std::mutex> l(queue_->mutex());
      for (auto& page : pages) {
        queue_->enqueueLocked(std::move(page), promises);
      }
      if (atEnd_) {
        // Notify exchange queue 'this' source has finished.
        queue_->enqueueLocked(nullptr, promises);
      }
    }
    for (auto& promise : promises) {
      promise.setValue();
    }

    return Response{totalBytes, atEnd_, reader_->remainingPageSizes()};
  });
}

folly::F14FastMap<std::string, int64_t> BroadcastExchangeSource::stats() const {
  return reader_->stats();
}

folly::SemiFuture<BroadcastExchangeSource::Response>
BroadcastExchangeSource::requestDataSizes(
    std::chrono::microseconds /*maxWait*/) {
  return folly::makeTryWith([&]() -> Response {
    auto remainingPageSizes = reader_->remainingPageSizes();

    // If the source is empty from the start, signal completion to ExchangeQueue
    if (remainingPageSizes.empty()) {
      atEnd_ = true;
      std::vector<velox::ContinuePromise> promises;
      {
        std::lock_guard<std::mutex> l(queue_->mutex());
        // Notify exchange queue 'this' source has finished.
        queue_->enqueueLocked(nullptr, promises);
      }
      for (auto& promise : promises) {
        promise.setValue();
      }
    }

    return Response{0, atEnd_, std::move(remainingPageSizes)};
  });
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

  auto fileSystem =
      velox::filesystems::getFileSystem(broadcastFileInfo->filePath_, nullptr);
  return std::make_shared<BroadcastExchangeSource>(
      uri.host(),
      destination,
      queue,
      std::make_shared<BroadcastFileReader>(
          broadcastFileInfo, fileSystem, pool),
      pool);
}
} // namespace facebook::presto::operators
