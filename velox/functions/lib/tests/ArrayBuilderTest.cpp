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
#include "velox/functions/lib/ArrayBuilder.h"

#include <stdint.h>

#include <gtest/gtest.h>

#include "velox/buffer/StringViewBufferHolder.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::functions::test {
namespace {

TEST(ArrayBuilder, zeroState) {
  auto pool = memory::getDefaultMemoryPool();
  for (int arraySize : {0, 10, 100, 1000, 10000}) {
    auto arrays =
        ArrayBuilder<int32_t>(arraySize, 0, pool.get()).finish(pool.get());
    ASSERT_EQ(arrays->size(), arraySize);
    for (int i = 0; i < arraySize; ++i) {
      EXPECT_EQ(arrays->isNullAt(i), 0);
      EXPECT_EQ(arrays->sizeAt(i), 0);
      EXPECT_EQ(arrays->offsetAt(i), 0);
    }
  }
}

TEST(ArrayBuilder, int32) {
  constexpr int kNumArrays = 100;
  for (int estimatedElementCount : {0, 10, 100, 1000}) {
    LOG(INFO) << "estimatedElementCount=" << estimatedElementCount;
    auto pool = memory::getDefaultMemoryPool();
    ArrayBuilder<int32_t> builder(
        kNumArrays, estimatedElementCount, pool.get());
    int numElements = 0;
    for (int i = 0; i < kNumArrays; ++i) {
      auto arr = builder.startArray(i);
      for (int j = 0; j < i % 3; ++j) {
        arr.push_back(i * 100 + j);
        ++numElements;
      }
    }
    std::shared_ptr<ArrayVector> result = std::move(builder).finish(pool.get());
    ASSERT_EQ(result->size(), kNumArrays);
    const auto* elements = result->elements()->asFlatVector<int32_t>();
    ASSERT_TRUE(elements);
    ASSERT_EQ(elements->size(), numElements);
    for (int i = 0; i < kNumArrays; ++i) {
      ASSERT_EQ(result->sizeAt(i), i % 3);
      ASSERT_FALSE(result->isNullAt(i));
      for (int j = 0; j < i % 3; ++j) {
        EXPECT_EQ(elements->valueAt(result->offsetAt(i) + j), i * 100 + j);
        EXPECT_FALSE(elements->isNullAt(result->offsetAt(i) + j));
      }
    }
  }
}

TEST(ArrayBuilder, bool) {
  constexpr int kNumArrays = 100;
  for (int estimatedElementCount : {0, 10, 100, 1000}) {
    LOG(INFO) << "estimatedElementCount=" << estimatedElementCount;
    auto pool = memory::getDefaultMemoryPool();
    ArrayBuilder<bool> builder(kNumArrays, estimatedElementCount, pool.get());
    int numElements = 0;
    for (int i = 0; i < kNumArrays; ++i) {
      auto arr = builder.startArray(i);
      for (int j = 0; j < i % 5; ++j) {
        arr.push_back(i & j & 1);
        ++numElements;
      }
    }
    std::shared_ptr<ArrayVector> result = std::move(builder).finish(pool.get());
    ASSERT_EQ(result->size(), kNumArrays);
    const auto* elements = result->elements()->asFlatVector<bool>();
    ASSERT_TRUE(elements);
    ASSERT_EQ(elements->size(), numElements);
    for (int i = 0; i < kNumArrays; ++i) {
      ASSERT_EQ(result->sizeAt(i), i % 5);
      ASSERT_FALSE(result->isNullAt(i));
      for (int j = 0; j < i % 5; ++j) {
        EXPECT_EQ(elements->valueAt(result->offsetAt(i) + j), i & j & 1)
            << "i=" << i << " j=" << j;
        EXPECT_FALSE(elements->isNullAt(result->offsetAt(i) + j));
      }
    }
  }
}

TEST(ArrayBuilder, Varchar) {
  constexpr int kNumArrays = 100;
  for (int estimatedElementCount : {0, 10, 100, 1000}) {
    auto pool = memory::getDefaultMemoryPool();
    std::optional<StringViewBufferHolder> arena(pool.get());
    ArrayBuilder<Varchar> builder(
        kNumArrays, estimatedElementCount, pool.get());
    int numElements = 0;
    const auto valueFor = [](int i, int j) {
      return fmt::format("{} {} {}", i, j, 1 << (i % 32));
    };
    for (int i = 0; i < kNumArrays; ++i) {
      auto arr = builder.startArray(i);
      for (int j = 0; j < i % 7; ++j) {
        arr.emplace_back(arena->getOwnedValue(valueFor(i, j)));
        ++numElements;
      }
    }
    builder.setStringBuffers(arena->moveBuffers());
    arena = std::nullopt;
    std::shared_ptr<ArrayVector> result = std::move(builder).finish(pool.get());
    ASSERT_EQ(result->size(), kNumArrays);
    const auto* elements = result->elements()->asFlatVector<StringView>();
    ASSERT_TRUE(elements);
    ASSERT_EQ(elements->size(), numElements);
    for (int i = 0; i < kNumArrays; ++i) {
      ASSERT_EQ(result->sizeAt(i), i % 7);
      ASSERT_FALSE(result->isNullAt(i));
      for (int j = 0; j < i % 7; ++j) {
        EXPECT_EQ(
            std::string(elements->valueAt(result->offsetAt(i) + j)),
            valueFor(i, j))
            << "i=" << i << " j=" << j;
        EXPECT_FALSE(elements->isNullAt(result->offsetAt(i) + j));
      }
    }
  }
}

} // namespace
} // namespace facebook::velox::functions::test
