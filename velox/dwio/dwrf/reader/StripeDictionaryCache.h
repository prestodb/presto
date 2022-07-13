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

#pragma once

#include <folly/Function.h>

#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/common/IntDecoder.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwrf {
class StripeDictionaryCache {
  // This could be potentially made an interface to be shared for
  // string dictionaries. However, we will need a union return type
  // in that case.
  class DictionaryEntry {
   public:
    explicit DictionaryEntry(
        folly::Function<BufferPtr(velox::memory::MemoryPool*)>&& dictGen);

    BufferPtr getDictionaryBuffer(velox::memory::MemoryPool* pool);

   private:
    folly::Function<BufferPtr(velox::memory::MemoryPool*)> dictGen_;
    BufferPtr dictionaryBuffer_;
  };

 public:
  explicit StripeDictionaryCache(velox::memory::MemoryPool* pool);

  void registerIntDictionary(
      const EncodingKey& ek,
      folly::Function<BufferPtr(velox::memory::MemoryPool*)>&& dictGen);

  BufferPtr getIntDictionary(const EncodingKey& ek);

 private:
  // This is typically the reader's memory pool.
  memory::MemoryPool* pool_;
  std::unordered_map<
      EncodingKey,
      std::unique_ptr<DictionaryEntry>,
      EncodingKeyHash>
      intDictionaryFactories_;

  VELOX_FRIEND_TEST(TestStripeDictionaryCache, RegisterDictionary);
};

} // namespace facebook::velox::dwrf
