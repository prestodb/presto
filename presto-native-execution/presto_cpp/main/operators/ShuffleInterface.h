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
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

class ShuffleWriter {
 public:
  virtual ~ShuffleWriter() = default;

  /// Write to the shuffle one row at a time.
  virtual void collect(int32_t partition, std::string_view data) = 0;

  /// Tell the shuffle system the writer is done.
  /// @param success set to false to indicate aborted client.
  virtual void noMoreData(bool success) = 0;

  /// Runtime statistics.
  virtual folly::F14FastMap<std::string, int64_t> stats() const = 0;
};

class ShuffleReader {
 public:
  virtual ~ShuffleReader() = default;

  /// Deprecate, do not use!
  virtual bool hasNext() {
    return true;
  }

  /// Reads the next block of data. The function returns null if it has read all
  /// the data. The function throws if run into any error.
  virtual velox::BufferPtr next() = 0;

  /// Tell the shuffle system the reader is done. May be called with 'success'
  /// true before reading all the data. This happens when a query has a LIMIT or
  /// similar operator that finishes the query early.
  /// @param success set to false to indicate aborted client.
  virtual void noMoreData(bool success) = 0;

  /// Runtime statistics.
  virtual folly::F14FastMap<std::string, int64_t> stats() const = 0;
};

class ShuffleInterfaceFactory {
 public:
  virtual ~ShuffleInterfaceFactory() = default;

  virtual std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedShuffleInfo,
      const int32_t partition,
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
