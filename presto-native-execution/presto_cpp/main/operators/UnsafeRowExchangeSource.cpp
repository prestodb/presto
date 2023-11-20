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
#include "presto_cpp/main/operators/UnsafeRowExchangeSource.h"

namespace facebook::presto::operators {

#define CALL_SHUFFLE(call, methodName)                                \
  try {                                                               \
    call;                                                             \
  } catch (const velox::VeloxException& e) {                          \
    throw;                                                            \
  } catch (const std::exception& e) {                                 \
    VELOX_FAIL("ShuffleReader::{} failed: {}", methodName, e.what()); \
  }

folly::SemiFuture<UnsafeRowExchangeSource::Response>
UnsafeRowExchangeSource::request(
    uint32_t /*maxBytes*/,
    uint32_t /*maxWaitSeconds*/) {
  auto nextBatch = [this]() {
    return std::move(shuffle_->next())
        .deferValue([this](velox::BufferPtr buffer) {
          std::vector<velox::ContinuePromise> promises;
          int64_t totalBytes = 0;

          {
            std::lock_guard<std::mutex> l(queue_->mutex());
            if (buffer == nullptr) {
              atEnd_ = true;
              queue_->enqueueLocked(nullptr, promises);
            } else {
              totalBytes = buffer->size();

              ++numBatches_;

              auto ioBuf =
                  folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
              queue_->enqueueLocked(
                  std::make_unique<velox::exec::SerializedPage>(
                      std::move(ioBuf), [buffer](auto& /*unused*/) {}),
                  promises);
            }
          }

          for (auto& promise : promises) {
            promise.setValue();
          }

          return folly::makeFuture(Response{totalBytes, atEnd_});
        })
        .deferError(
            [](folly::exception_wrapper e) mutable
            -> UnsafeRowExchangeSource::Response {
              VELOX_FAIL("ShuffleReader::{} failed: {}", "next", e.what());
            });
  };

  CALL_SHUFFLE(return nextBatch(), "next");
}

folly::F14FastMap<std::string, int64_t> UnsafeRowExchangeSource::stats() const {
  return shuffle_->stats();
}

#undef CALL_SHUFFLE

namespace {
std::optional<std::string> getSerializedShuffleInfo(folly::Uri& uri) {
  for (auto& pair : uri.getQueryParams()) {
    if (pair.first == "shuffleInfo") {
      return std::make_optional(pair.second);
    }
  }
  return std::nullopt;
}
} // namespace

// static
std::shared_ptr<velox::exec::ExchangeSource>
UnsafeRowExchangeSource::createExchangeSource(
    const std::string& url,
    int32_t destination,
    const std::shared_ptr<velox::exec::ExchangeQueue>& queue,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  if (::strncmp(url.c_str(), "batch://", 8) != 0) {
    return nullptr;
  }

  auto uri = folly::Uri(url);
  auto serializedShuffleInfo = getSerializedShuffleInfo(uri);
  // Not shuffle exchange source.
  if (!serializedShuffleInfo.has_value()) {
    return nullptr;
  }

  auto shuffleName = SystemConfig::instance()->shuffleName();
  VELOX_CHECK(
      !shuffleName.empty(),
      "shuffle.name is not provided in config.properties to create a shuffle "
      "interface.");
  auto shuffleFactory = ShuffleInterfaceFactory::factory(shuffleName);
  return std::make_shared<UnsafeRowExchangeSource>(
      uri.host(),
      destination,
      queue,
      shuffleFactory->createReader(
          serializedShuffleInfo.value(), destination, pool),
      pool);
}
}; // namespace facebook::presto::operators
