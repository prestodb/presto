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
#include "velox/functions/prestosql/aggregates/MapAccumulator.h"
#include <gtest/gtest.h>
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

class MapAccumulatorTest : public testing::Test, public test::VectorTestBase {
 protected:
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  // Takes a vector of unique keys and a matching vector of non-null values.
  template <typename K>
  void test(const VectorPtr& uniqueKeys, const VectorPtr& values) {
    // No duplicates.
    auto expected = makeMapVector({0}, uniqueKeys, values);
    test<K>(uniqueKeys, values, expected);

    // Duplicate keys.
    auto size = uniqueKeys->size();
    auto indices = makeIndices(1'000, [&](auto row) { return row % size; });
    auto duplicateKeys = wrapInDictionary(indices, uniqueKeys);
    auto duplicateValues = wrapInDictionary(indices, values);

    test<K>(duplicateKeys, duplicateValues, expected);

    // Null values.
    auto valuesWithNulls = BaseVector::copy(*values);
    for (auto i = 0; i < size; ++i) {
      if (i % 3 == 0) {
        valuesWithNulls->setNull(i, true);
      }
    }

    expected = makeMapVector({0}, uniqueKeys, valuesWithNulls);
    test<K>(uniqueKeys, valuesWithNulls, expected);
  }

  // Takes a vector of non-null keys, a matching vector of possibly null values
  // and a map that contains only unique keys. Adds the keys and the values to a
  // MapAccumulator, extracts a map vector and assert that it matches
  // 'expectedMap'.
  template <typename K>
  void test(
      const VectorPtr& keys,
      const VectorPtr& values,
      const VectorPtr& expectedMap) {
    MapAccumulator<K> accumulator{keys->type(), allocator()};

    ASSERT_EQ(0, accumulator.size());

    DecodedVector decodedKeys(*keys);
    DecodedVector decodedValues(*values);
    for (auto i = 0; i < keys->size(); ++i) {
      accumulator.insert(decodedKeys, decodedValues, i, *allocator());
    }

    // Test extract with zero offset.
    auto mapKeys = BaseVector::create(keys->type(), accumulator.size(), pool());
    auto mapValues =
        BaseVector::create(values->type(), accumulator.size(), pool());
    accumulator.extract(mapKeys, mapValues, 0);

    auto mapVector = makeMapVector({0}, mapKeys, mapValues);
    test::assertEqualVectors(expectedMap, mapVector);

    // Test extract with non-zero offset.
    mapKeys =
        BaseVector::create(keys->type(), 111 + accumulator.size(), pool());
    mapValues =
        BaseVector::create(values->type(), 111 + accumulator.size(), pool());
    accumulator.extract(mapKeys, mapValues, 111);

    mapVector = makeMapVector({111}, mapKeys, mapValues);
    test::assertEqualVectors(expectedMap, mapVector);
  }

  HashStringAllocator* allocator() {
    return allocator_.get();
  }

  std::unique_ptr<HashStringAllocator> allocator_{
      std::make_unique<HashStringAllocator>(pool())};
};

TEST_F(MapAccumulatorTest, integerKeys) {
  auto keys = makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6});
  auto values = makeFlatVector<int32_t>({10, 20, 30, 40, 50, 60});

  test<int32_t>(keys, values);
}

TEST_F(MapAccumulatorTest, strings) {
  std::vector<std::string> s = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
      "sweet fruits:: pineapple"};

  auto keys = makeFlatVector<std::string>({s[0], s[1], s[2], s[3], s[4], s[5]});
  auto values = makeFlatVector<int32_t>({0, 10, 20, 30, 40, 50});

  test<StringView>(keys, values);

  // Swap keys and values.
  test<ComplexType>(values, keys);
}

TEST_F(MapAccumulatorTest, arrays) {
  auto keys = makeArrayVector<int32_t>({
      {1, 2},
      {1, 2, 3},
      {4, 5},
      {4, 5, 6},
      {},
  });
  auto values = makeFlatVector<int64_t>({10, 20, 30, 40, 50});

  test<ComplexType>(keys, values);

  // Swap keys and values.
  test<ComplexType>(values, keys);
}

TEST_F(MapAccumulatorTest, maps) {
  auto keys = makeMapVector<int32_t, double>({
      {{1, 1.1}, {2, 2.2}},
      {{1, 1.1}, {2, 2.2}, {3, 3.3}},
      {{4, 4.4}, {5, 5.5}},
      {{4, 4.4}, {5, 5.5}, {6, 6.6}},
      {},
  });
  auto values = makeFlatVector<int64_t>({10, 20, 30, 40, 50});

  test<ComplexType>(keys, values);

  // Swap keys and values.
  test<ComplexType>(values, keys);
}

TEST_F(MapAccumulatorTest, rows) {
  auto keys = makeRowVector(
      {makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int32_t>({11, 22, 33, 44, 55}),
       makeNullableFlatVector<float>(
           {1.1, std::nullopt, 3.3, std::nullopt, 4.4, std::nullopt})});
  auto values = makeFlatVector<int64_t>({10, 20, 30, 40, 50});

  test<ComplexType>(keys, values);

  // Swap keys and values.
  test<ComplexType>(values, keys);
}

} // namespace
} // namespace facebook::velox::aggregate::prestosql
