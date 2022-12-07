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

class ShuffleInterface {
 public:
  /// Indicates the type of ShuffleInterface. This common interface could be
  /// used for both READ and WRITE. This enum is used to differenciate the usage
  /// of the ShuffleInterface
  enum class Type {
    kRead,
    kWrite,
  };

  using ShuffleInterfaceFactory =
      std::function<std::shared_ptr<ShuffleInterface>(
          const std::string& serializedShuffleInfo,
          Type type,
          velox::memory::MemoryPool* pool)>;

  virtual ~ShuffleInterface() = default;

  /// Write to the shuffle one row at a time.
  virtual void collect(int32_t partition, std::string_view data) = 0;

  /// Tell the shuffle system the writer is done.
  /// @param success set to false to indicate aborted client.
  virtual void noMoreData(bool success) = 0;

  /// Check by the reader to see if more blocks are available for this
  /// partition.
  virtual bool hasNext(int32_t partition) const = 0;

  /// Read the next block of data for this partition.
  /// @param success set to false to indicate aborted client.
  virtual velox::BufferPtr next(int32_t partition, bool success) = 0;

  /// Return true if all the data is finished writing and is ready to
  /// to be read while noMoreData signals the shuffle service that there
  /// is no more data to be writen.
  virtual bool readyForRead() const = 0;

  /// Register ShuffleInterfaceFactory to its registry. It returns true if the
  /// registration is successful, false if a factory with the name already
  /// exists.
  /// This method is not thread safe.
  static bool registerFactory(
      const std::string& name,
      ShuffleInterfaceFactory shuffleInterfaceFactory) {
    std::unordered_map<std::string, ShuffleInterfaceFactory>& factoryMap =
        factories();
    return factoryMap.emplace(name, shuffleInterfaceFactory).second;
  }

  /// Get a ShuffleInterfaceFactory with provided name. Throws if not found.
  /// This method is not thread safe.
  static ShuffleInterfaceFactory& factory(const std::string& name) {
    auto factoryIter = factories().find(name);
    if (factoryIter == factories().end()) {
      VELOX_FAIL("ShuffleInterface with name '{}' is not registered.", name);
    }
    return factoryIter->second;
  }

 private:
  static std::unordered_map<std::string, ShuffleInterfaceFactory>& factories() {
    static std::unordered_map<std::string, ShuffleInterfaceFactory> factories;
    return factories;
  }
};

} // namespace facebook::presto::operators
