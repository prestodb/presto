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
#pragma once

#include <fmt/format.h>
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"

namespace facebook::velox {
class ByteInputStream;
}

namespace facebook::presto::operators {

class ShuffleWriter {
 public:
  virtual ~ShuffleWriter() = default;

  /// Write to the shuffle one row at a time.
  virtual void
  collect(int32_t partition, std::string_view key, std::string_view data) = 0;

  /// Tell the shuffle system the writer is done.
  /// @param success set to false to indicate aborted client.
  virtual void noMoreData(bool success) = 0;

  /// Runtime statistics.
  virtual folly::F14FastMap<std::string, int64_t> stats() const = 0;

  /// Returns true if the shuffle reader supports Velox runtime metrics.
  virtual bool supportsMetrics() const {
    return false;
  }

  /// Returns statistics in the form of Velox runtime metrics.
  virtual folly::F14FastMap<std::string, velox::RuntimeMetric> metrics() const {
    VELOX_NYI();
  }
};

class ShuffleSerializedPage : public velox::exec::SerializedPageBase {
 public:
  ShuffleSerializedPage() = default;
  ~ShuffleSerializedPage() override = default;

  std::unique_ptr<velox::ByteInputStream> prepareStreamForDeserialize()
      override {
    VELOX_UNSUPPORTED();
  }

  std::unique_ptr<folly::IOBuf> getIOBuf() const override {
    VELOX_UNSUPPORTED();
  }

  virtual const std::vector<std::string_view>& rows() = 0;
};

class ShuffleReader {
 public:
  virtual ~ShuffleReader() = default;

  /// Fetch the next batch of rows from the shuffle reader.
  /// @param bytes Maximum number of bytes to read in this batch.
  /// @return A semi-future resolving to a vector of ShuffleSerializedPage
  /// pointers, where each ShuffleSerializedPage contains rows and associated
  /// data buffers.
  virtual folly::SemiFuture<std::vector<std::unique_ptr<ShuffleSerializedPage>>>
  next(uint64_t maxBytes) = 0;

  /// Tell the shuffle system the reader is done. May be called with 'success'
  /// true before reading all the data. This happens when a query has a LIMIT or
  /// similar operator that finishes the query early.
  /// @param success set to false to indicate aborted client.
  virtual void noMoreData(bool success) = 0;

  /// Runtime statistics.
  virtual folly::F14FastMap<std::string, int64_t> stats() const = 0;

  /// Returns true if the shuffle reader supports Velox runtime metrics.
  virtual bool supportsMetrics() const {
    return false;
  }

  /// Returns statistics in the form of Velox runtime metrics.
  virtual folly::F14FastMap<std::string, velox::RuntimeMetric> metrics() const {
    VELOX_NYI();
  }
};

class ShuffleInterfaceFactory {
 public:
  virtual ~ShuffleInterfaceFactory() = default;

  virtual std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedShuffleInfo,
      int32_t partition,
      velox::memory::MemoryPool* pool) = 0;

  virtual std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* pool) = 0;

  /// Register ShuffleInterfaceFactory to its registry. It returns true if the
  /// registration is successful, false if a factory with the name already
  /// exists.
  /// This method is not thread safe.
  static bool registerFactory(
      const std::string& name,
      std::unique_ptr<ShuffleInterfaceFactory> shuffleInterfaceFactory) {
    std::unordered_map<std::string, std::unique_ptr<ShuffleInterfaceFactory>>&
        factoryMap = factories();
    return factoryMap.emplace(name, std::move(shuffleInterfaceFactory)).second;
  }

  /// Get a ShuffleInterfaceFactory with provided name. Throws if not found.
  /// This method is not thread safe.
  static ShuffleInterfaceFactory* factory(const std::string& name) {
    auto factoryIter = factories().find(name);
    if (factoryIter == factories().end()) {
      VELOX_FAIL("ShuffleInterface with name '{}' is not registered.", name);
    }
    return factoryIter->second.get();
  }

 private:
  static std::
      unordered_map<std::string, std::unique_ptr<ShuffleInterfaceFactory>>&
      factories() {
    static std::
        unordered_map<std::string, std::unique_ptr<ShuffleInterfaceFactory>>
            factories;
    return factories;
  }
};
} // namespace facebook::presto::operators
