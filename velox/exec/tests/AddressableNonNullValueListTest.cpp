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
#include "velox/exec/AddressableNonNullValueList.h"
#include <gtest/gtest.h>
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

class AddressableNonNullValueListTest : public testing::Test,
                                        public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void test(const VectorPtr& data, const VectorPtr& uniqueData) {
    using T = AddressableNonNullValueList::Entry;
    using Set = folly::F14FastSet<
        T,
        AddressableNonNullValueList::Hash,
        AddressableNonNullValueList::EqualTo,
        AlignedStlAllocator<T, 16>>;

    Set uniqueValues{
        0,
        AddressableNonNullValueList::Hash{},
        AddressableNonNullValueList::EqualTo{data->type()},
        AlignedStlAllocator<T, 16>(allocator())};

    AddressableNonNullValueList values;

    std::vector<T> entries;

    DecodedVector decodedVector(*data);
    for (auto i = 0; i < data->size(); ++i) {
      auto entry = values.append(decodedVector, i, allocator());

      if (uniqueValues.contains(entry)) {
        values.removeLast(entry);
        continue;
      }

      entries.push_back(entry);

      ASSERT_TRUE(uniqueValues.insert(entry).second);
      ASSERT_TRUE(uniqueValues.contains(entry));
      ASSERT_FALSE(uniqueValues.insert(entry).second);
    }

    ASSERT_EQ(uniqueData->size(), values.size());
    ASSERT_EQ(uniqueData->size(), uniqueValues.size());

    auto copy = BaseVector::create(data->type(), uniqueData->size(), pool());
    for (auto i = 0; i < entries.size(); ++i) {
      auto entry = entries[i];
      ASSERT_TRUE(uniqueValues.contains(entry));
      AddressableNonNullValueList::read(entry, *copy, i);
    }

    test::assertEqualVectors(uniqueData, copy);
  }

  HashStringAllocator* allocator() {
    return allocator_.get();
  }

  std::unique_ptr<HashStringAllocator> allocator_{
      std::make_unique<HashStringAllocator>(pool())};
};

TEST_F(AddressableNonNullValueListTest, array) {
  auto data = makeArrayVector<int32_t>({
      {1, 2, 3},
      {4, 5},
      {6, 7, 8, 9},
      {},
  });

  test(data, data);

  auto dataWithDuplicates = makeArrayVector<int32_t>({
      {1, 2, 3},
      {1, 2, 3},
      {4, 5},
      {6, 7, 8, 9},
      {},
      {4, 5},
      {1, 2, 3},
      {},
  });

  test(dataWithDuplicates, data);
}

TEST_F(AddressableNonNullValueListTest, map) {
  auto data = makeMapVector<int16_t, int64_t>({
      {{1, 10}, {2, 20}},
      {{3, 30}, {4, 40}, {5, 50}},
      {{1, 10}, {3, 30}, {4, 40}, {6, 60}},
      {},
  });

  test(data, data);

  auto dataWithDuplicates = makeMapVector<int16_t, int64_t>({
      {{1, 10}, {2, 20}},
      {{3, 30}, {4, 40}, {5, 50}},
      {{3, 30}, {4, 40}, {5, 50}},
      {{1, 10}, {2, 20}},
      {{1, 10}, {3, 30}, {4, 40}, {6, 60}},
      {},
      {{1, 10}, {2, 20}},
      {},
      {{3, 30}, {4, 40}, {5, 50}},
  });

  test(dataWithDuplicates, data);
}

TEST_F(AddressableNonNullValueListTest, row) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
      makeFlatVector<int32_t>({10, 20, 30, 40, 50}),
      makeFlatVector<int64_t>({11, 22, 33, 44, 55}),
  });

  test(data, data);

  auto dataWithDuplicates = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 3, 4, 2, 5, 3}),
      makeFlatVector<int32_t>({10, 20, 30, 40, 20, 50, 30}),
      makeFlatVector<int64_t>({11, 22, 33, 44, 22, 55, 33}),
  });

  test(dataWithDuplicates, data);

  data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 1, 1, 1}),
      makeFlatVector<int32_t>({10, 20, 30, 30, 40}),
      makeFlatVector<int64_t>({11, 22, 33, 44, 55}),
  });

  dataWithDuplicates = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 1, 1, 1, 1, 1}),
      makeFlatVector<int32_t>({10, 10, 20, 20, 30, 30, 40}),
      makeFlatVector<int64_t>({11, 11, 22, 22, 33, 44, 55}),
  });

  test(dataWithDuplicates, data);

  data = makeRowVector({
      makeNullableFlatVector<int16_t>({1, 2, std::nullopt, 4, 5}),
      makeNullableFlatVector<int32_t>({10, 20, 30, std::nullopt, 50}),
      makeNullableFlatVector<int64_t>({std::nullopt, 22, 33, std::nullopt, 55}),
  });

  test(data, data);

  dataWithDuplicates = makeRowVector({
      makeNullableFlatVector<int16_t>(
          {1, 2, 2, std::nullopt, 4, 5, std::nullopt}),
      makeNullableFlatVector<int32_t>({10, 20, 20, 30, std::nullopt, 50, 30}),
      makeNullableFlatVector<int64_t>(
          {std::nullopt, 22, 22, 33, std::nullopt, 55, 33}),
  });
  test(dataWithDuplicates, data);
}

} // namespace
} // namespace facebook::velox::aggregate::prestosql
