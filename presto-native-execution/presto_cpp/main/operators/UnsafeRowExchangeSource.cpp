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

using namespace facebook::velox;

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
    uint32_t maxBytes,
    uint32_t maxWaitSeconds) {
  if (atEnd_) {
    return folly::makeFuture(Response{0, atEnd_});
  }

  auto promise = VeloxPromise<Response>("UnsafeRowExchangeSource::request");
  auto requestFuture = promise.getSemiFuture();
  // This future will be set when the async fetch of data completes
  // The returned future is used to chain the next request for
  // more data from this source
  promise_ = std::move(promise);

  // We only ever have 1 outstanding request to fetch more data
  // Setting this flag tells ExchangeClient that this source has a pending
  // request and to not trigger another request
  requestPending_ = true;

  // kick off attempts to put data into exchange queue
  fetchDataAndUpdateQueueAsync();

  return requestFuture;
}

void UnsafeRowExchangeSource::fetchDataAndUpdateQueueAsync() {

  ioThreadPoolExecutor_->add([this]() {
    std::vector<ContinuePromise> queuePromises;
    VeloxPromise<Response> requestPromise;
    bool hasNext;
    int64_t totalBytes = 0;

    CALL_SHUFFLE(hasNext = shuffle_->hasNext(), "hasNext");
    if (!hasNext) {
      // now that we have data from the underlying shuffle client
      // we acquire write lock to the velox queue
      std::lock_guard<std::mutex> l(queue_->mutex());

      atEnd_ = true;
      queue_->enqueueLocked(nullptr, queuePromises);
    } else {
      velox::BufferPtr buffer;
      CALL_SHUFFLE(buffer = shuffle_->next(), "next");
      totalBytes = buffer->size();

      ++numBatches_;

      {
        // now that we have data from the underlying shuffle client
        // we acquire write lock to the velox queue
        std::lock_guard<std::mutex> l(queue_->mutex());

        auto ioBuf = folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
        // NOTE: SerializedPage's onDestructionCb_ captures one reference on
        // 'buffer' to keep its alive until SerializedPage destruction. Also note
        // that 'buffer' should have been allocated from memory pool. Hence, we
        // don't need to update the memory usage counting for the associated
        // 'ioBuf' attached to SerializedPage on destruction.
        queue_->enqueueLocked(
            std::make_unique<velox::exec::SerializedPage>(
                std::move(ioBuf), [buffer](auto& /*unused*/) {}),
            queuePromises);
      }
    }

    // This let's ExchangeClient know that this source can
    // take a new request
    requestPending_ = false;
    requestPromise = std::move(promise_);

    // Unblock a few driver threads
    // These promises are held by the queue and returned when
    // we enqueue some pages into the queue
    for (auto& blockedDriverPromise : queuePromises) {
      blockedDriverPromise.setValue();
    }
    // This triggers a new request
    if (requestPromise.valid() && !requestPromise.isFulfilled()) {
      requestPromise.setValue(Response{totalBytes, atEnd_});
    }
  });
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
std::unique_ptr<velox::exec::ExchangeSource>
UnsafeRowExchangeSource::createExchangeSource(
    const std::string& url,
    int32_t destination,
    std::shared_ptr<velox::exec::ExchangeQueue> queue,
    velox::memory::MemoryPool* FOLLY_NONNULL pool,
    folly::IOThreadPoolExecutor* ioThreadPoolExecutor) {
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
  return std::make_unique<UnsafeRowExchangeSource>(
      uri.host(),
      destination,
      std::move(queue),
      shuffleFactory->createReader(
          serializedShuffleInfo.value(), destination, pool),
      pool,
      ioThreadPoolExecutor);
}
}; // namespace facebook::presto::operators
