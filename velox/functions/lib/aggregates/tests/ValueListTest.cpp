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
#include "velox/functions/lib/aggregates/ValueList.h"
#include <gtest/gtest.h>
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class ValueListTest : public functions::test::FunctionBaseTest {
 protected:
  ValueListTest() : functions::test::FunctionBaseTest() {}

  // Make sure to test sizes that are multiples of 64.
  static constexpr vector_size_t kTestSizes[6] =
      {10, 64, 128, 1'000, 1'024, 10'000};

  VectorPtr
  read(aggregate::ValueList& values, const TypePtr& type, vector_size_t size) {
    aggregate::ValueListReader reader(values);
    auto result = BaseVector::create(type, size, pool());

    // Initialize result to all-nulls to ensure ValueListReader::next() reset
    // null bits correctly.
    for (auto i = 0; i < size; ++i) {
      result->setNull(i, true);
    }

    for (auto i = 0; i < size; i++) {
      reader.next(*result, i);
    }
    return result;
  }

  void testRoundTrip(const VectorPtr& data) {
    auto size = data->size();

    // Use ValueList::appendValue.
    {
      DecodedVector decoded(*data);
      aggregate::ValueList values;
      for (auto i = 0; i < size; i++) {
        values.appendValue(decoded, i, allocator());
      }

      ASSERT_EQ(size, values.size());
      auto result = read(values, data->type(), size);

      assertEqualVectors(data, result);
    }

    // Use ValueList::appendRange.
    {
      aggregate::ValueList values;
      values.appendRange(data, 0, size, allocator());

      ASSERT_EQ(size, values.size());
      auto result = read(values, data->type(), size);

      assertEqualVectors(data, result);
    }
  }

  HashStringAllocator* allocator() {
    return allocator_.get();
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  std::unique_ptr<HashStringAllocator> allocator_{
      std::make_unique<HashStringAllocator>(pool_.get())};
};

TEST_F(ValueListTest, empty) {
  aggregate::ValueList values;
  ASSERT_EQ(0, values.size());
}

TEST_F(ValueListTest, integers) {
  // No nulls.
  for (auto size : kTestSizes) {
    auto data = makeFlatVector<int32_t>(size, [](auto row) { return row; });

    testRoundTrip(data);
  }

  // Different percentage of nulls.
  for (auto size : kTestSizes) {
    for (auto nullEvery : {2, 7, 97}) {
      auto data = makeFlatVector<int32_t>(
          size,
          [](auto row) { return row; },
          test::VectorMaker::nullEvery(nullEvery));

      testRoundTrip(data);
    }
  }
}

TEST_F(ValueListTest, arrays) {
  // No nulls.
  int32_t kSizeCaps[] = {730, 4000, 7500, 50000};
  int32_t counter = 0;
  for (auto size : kTestSizes) {
    auto data = makeArrayVector<int32_t>(
        size,
        [](auto row) { return row % 7; },
        [](auto row) { return row % 11; });

    auto previousBytes = allocator()->cumulativeBytes();
    testRoundTrip(data);
    if (counter < sizeof(kSizeCaps) / sizeof(kSizeCaps[0])) {
      auto cap = kSizeCaps[counter++];
      EXPECT_GT(cap, allocator()->cumulativeBytes() - previousBytes);
    }
  }

  // Different percentage of nulls.
  for (auto size : kTestSizes) {
    for (auto nullEvery : {2, 7, 97}) {
      auto data = makeArrayVector<int32_t>(
          size,
          [](auto row) { return row % 7; },
          [](auto row) { return row % 11; },
          test::VectorMaker::nullEvery(nullEvery));

      testRoundTrip(data);
    }
  }
}
