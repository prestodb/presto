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
#include "velox/functions/prestosql/aggregates/ValueList.h"
#include <gtest/gtest.h>
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class ValueListTest : public functions::test::FunctionBaseTest {
 protected:
  ValueListTest() : functions::test::FunctionBaseTest() {}

  void testRoundTrip(const VectorPtr& data) {
    auto size = data->size();

    SelectivityVector allRows(size);
    DecodedVector decoded(*data, allRows);

    aggregate::ValueList values;
    for (auto i = 0; i < size; i++) {
      values.appendValue(decoded, i, allocator());
    }

    values.finalize(allocator());

    aggregate::ValueListReader reader(values);
    auto result = BaseVector::create(data->type(), size, pool());
    for (auto i = 0; i < size; i++) {
      reader.next(*result, i);
    }

    assertEqualVectors(data, result);
  }

  HashStringAllocator* allocator() {
    return allocator_.get();
  }

  std::unique_ptr<HashStringAllocator> allocator_{
      std::make_unique<HashStringAllocator>(
          memory::MappedMemory::getInstance())};
};

TEST_F(ValueListTest, integers) {
  // no nulls
  for (auto size : {10, 1'000, 10'000}) {
    auto data = makeFlatVector<int32_t>(size, [](auto row) { return row; });

    testRoundTrip(data);
  }

  // different percentage of nulls
  for (auto size : {10, 1'000, 10'000}) {
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
  // no nulls
  for (auto size : {10, 1'000, 10'000}) {
    auto data = makeArrayVector<int32_t>(
        size,
        [](auto row) { return row % 7; },
        [](auto row) { return row % 11; });

    testRoundTrip(data);
  }

  // different percentage of nulls
  for (auto size : {10, 1'000, 10'000}) {
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
