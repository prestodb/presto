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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/dwio/dwrf/reader/StripeDictionaryCache.h"
#include "velox/dwio/dwrf/test/OrcTest.h"

using namespace ::testing;

namespace facebook::velox::dwrf {

namespace {
folly::Function<BufferPtr(memory::MemoryPool*)> genConsecutiveRangeBuffer(
    int64_t begin,
    int64_t end) {
  return [begin, end](memory::MemoryPool* pool) {
    BufferPtr dictionaryBuffer =
        AlignedBuffer::allocate<int64_t>(end - begin, pool);
    auto data = dictionaryBuffer->asMutable<int64_t>();
    for (int64_t i = 0; i < end - begin; ++i) {
      data[i] = begin + i;
    }
    return dictionaryBuffer;
  };
}

void verifyRange(BufferPtr bufferPtr, int64_t begin, int64_t end) {
  size_t bufferSize = bufferPtr->size() / sizeof(int64_t);
  ASSERT_EQ(end - begin, bufferSize);
  auto data = bufferPtr->as<int64_t>();
  std::vector<int64_t> actualRange{};
  for (size_t i = 0; i < bufferSize; ++i) {
    actualRange.push_back(data[i]);
  }
  std::vector<int64_t> expectedRange{};
  while (begin < end) {
    expectedRange.push_back(begin++);
  }
  EXPECT_THAT(actualRange, ElementsAreArray(expectedRange));
}

class StripeDictionaryCacheTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};
} // namespace

TEST_F(StripeDictionaryCacheTest, RegisterDictionary) {
  {
    StripeDictionaryCache cache{pool_.get()};
    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(0, 100));
    EXPECT_EQ(1, cache.intDictionaryFactories_.size());
    EXPECT_EQ(1, cache.intDictionaryFactories_.count({9, 0}));
  }
  // When a dictionary is registered multiple times, we only honor the
  // first invocation. In practice, all params are retrieved from the same
  // StripeStream, so it won't matter. For here, we have to test that
  // the content is not changed in getDictionaryBuffer tests.
  {
    StripeDictionaryCache cache{pool_.get()};

    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(0, 100));
    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(100, 200));

    EXPECT_EQ(1, cache.intDictionaryFactories_.size());
    EXPECT_EQ(1, cache.intDictionaryFactories_.count({9, 0}));
  }
  {
    StripeDictionaryCache cache{pool_.get()};

    cache.registerIntDictionary({1, 0}, genConsecutiveRangeBuffer(0, 100));
    cache.registerIntDictionary({1, 1}, genConsecutiveRangeBuffer(0, 100));
    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(100, 200));

    EXPECT_EQ(3, cache.intDictionaryFactories_.size());
    EXPECT_EQ(1, cache.intDictionaryFactories_.count({1, 0}));
    EXPECT_EQ(1, cache.intDictionaryFactories_.count({1, 1}));
    EXPECT_EQ(1, cache.intDictionaryFactories_.count({9, 0}));
  }
}

TEST_F(StripeDictionaryCacheTest, GetDictionaryBuffer) {
  {
    StripeDictionaryCache cache{pool_.get()};

    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(0, 100));
    verifyRange(cache.getIntDictionary({9, 0}), 0, 100);
    EXPECT_ANY_THROW(cache.getIntDictionary({7, 0}));
  }
  // When a dictionary is registered multiple times, we only honor the
  // first invocation. In practice, all params are retrieved from the same
  // StripeStream, so it won't matter. For here, we have to test that
  // the content is not changed.
  {
    StripeDictionaryCache cache{pool_.get()};

    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(0, 100));
    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(100, 200));

    verifyRange(cache.getIntDictionary({9, 0}), 0, 100);
  }
  {
    StripeDictionaryCache cache{pool_.get()};

    cache.registerIntDictionary({1, 0}, genConsecutiveRangeBuffer(0, 100));
    cache.registerIntDictionary({1, 1}, genConsecutiveRangeBuffer(0, 100));
    cache.registerIntDictionary({9, 0}, genConsecutiveRangeBuffer(100, 200));

    verifyRange(cache.getIntDictionary({1, 0}), 0, 100);
    verifyRange(cache.getIntDictionary({1, 1}), 0, 100);
    verifyRange(cache.getIntDictionary({9, 0}), 100, 200);
    EXPECT_ANY_THROW(cache.getIntDictionary({2, 0}));
  }
}
} // namespace facebook::velox::dwrf
