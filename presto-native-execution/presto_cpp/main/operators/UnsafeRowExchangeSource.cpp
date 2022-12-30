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

void UnsafeRowExchangeSource::request() {
  std::lock_guard<std::mutex> l(queue_->mutex());

  if (!shuffle_->hasNext()) {
    atEnd_ = true;
    queue_->enqueue(nullptr);
    return;
  }

  auto buffer = shuffle_->next(true);

  auto ioBuf = folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
  // NOTE: SerializedPage's onDestructionCb_ captures one reference on 'buffer'
  // to keep its alive until SerializedPage destruction. Also note that 'buffer'
  // should have been allocated from memory pool. Hence, we don't need to update
  // the memory usage counting for the associated 'ioBuf' attached to
  // SerializedPage on destruction.
  queue_->enqueue(std::make_unique<velox::exec::SerializedPage>(
      std::move(ioBuf), pool_, [buffer](auto&) {}));
}

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
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  if (::strncmp(url.c_str(), "batch://", 8) != 0) {
    return nullptr;
  }
  auto shuffleName = SystemConfig::instance()->shuffleName();
  VELOX_CHECK(
      !shuffleName.empty(),
      "shuffle.name is not provided in config.properties to create a shuffle "
      "interface.");
  auto shuffleFactory = ShuffleInterfaceFactory::factory(shuffleName);
  auto uri = folly::Uri(url);
  auto serializedShuffleInfo = getSerializedShuffleInfo(uri);
  VELOX_USER_CHECK(
      serializedShuffleInfo.has_value(),
      "Cannot find shuffleInfo parameter in split url '{}'",
      url);
  return std::make_unique<UnsafeRowExchangeSource>(
      uri.host(),
      destination,
      std::move(queue),
      shuffleFactory->createReader(
          serializedShuffleInfo.value(), destination, pool),
      pool);
}
}; // namespace facebook::presto::operators
