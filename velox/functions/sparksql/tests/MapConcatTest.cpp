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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class MapConcatTest : public SparkFunctionBaseTest {
 protected:
  template <typename TKey, typename TValue>
  static std::vector<TKey> mapKeys(const std::map<TKey, TValue>& m) {
    std::vector<TKey> keys;
    keys.reserve(m.size());
    for (const auto& [key, value] : m) {
      keys.push_back(key);
    }
    return keys;
  }

  template <typename TKey, typename TValue>
  static std::vector<TValue> mapValues(const std::map<TKey, TValue>& m) {
    std::vector<TValue> values;
    values.reserve(m.size());
    for (const auto& [key, value] : m) {
      values.push_back(value);
    }
    return values;
  }

  MapVectorPtr makeMapVector(
      vector_size_t size,
      const std::map<std::string, int32_t>& m,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    std::vector<std::string> keys = mapKeys(m);
    std::vector<int32_t> values = mapValues(m);
    return vectorMaker_.mapVector<StringView, int32_t>(
        size,
        [&](vector_size_t /*row*/) { return keys.size(); },
        [&](vector_size_t /*mapRow*/, vector_size_t row) {
          return StringView(keys[row]);
        },
        [&](vector_size_t mapRow, vector_size_t row) {
          return mapRow % 11 + values[row];
        },
        isNullAt);
  }

  template <typename TKey, typename TValue>
  std::map<TKey, TValue> concat(
      const std::map<TKey, TValue>& a,
      const std::map<TKey, TValue>& b) {
    std::map<TKey, TValue> result;
    result.insert(b.begin(), b.end());
    result.insert(a.begin(), a.end());
    return result;
  }
};

TEST_F(MapConcatTest, duplicateKeys) {
  vector_size_t size = 1'000;

  std::map<std::string, int32_t> a = {
      {"a1", 1}, {"a2", 2}, {"a3", 3}, {"a4", 4}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}, {"a2", -1}};
  auto aMap = makeMapVector(size, a);
  auto bMap = makeMapVector(size, b);

  // By default, if a key is found in multiple given maps, that key's value in
  // the resulting map comes from the last one of those maps.
  std::map<std::string, int32_t> ab = concat(a, b);
  auto expectedMap = makeMapVector(size, ab);
  auto result =
      evaluate<MapVector>("map_concat(c0, c1)", makeRowVector({aMap, bMap}));
  velox::test::assertEqualVectors(expectedMap, result);

  std::map<std::string, int32_t> ba = concat(b, a);
  expectedMap = makeMapVector(size, ba);
  result =
      evaluate<MapVector>("map_concat(c1, c0)", makeRowVector({aMap, bMap}));
  velox::test::assertEqualVectors(expectedMap, result);

  result =
      evaluate<MapVector>("map_concat(c0, c1)", makeRowVector({aMap, aMap}));
  velox::test::assertEqualVectors(aMap, result);

  // Throws exception when duplicate keys are found.
  queryCtx_->testingOverrideConfigUnsafe({
      {core::QueryConfig::kThrowExceptionOnDuplicateMapKeys, "true"},
  });
  VELOX_ASSERT_USER_THROW(
      evaluate<MapVector>("map_concat(c0, c1)", makeRowVector({aMap, bMap})),
      "Duplicate map key a2 was found");
}

TEST_F(MapConcatTest, singleArg) {
  vector_size_t size = 1'000;

  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  auto aMap = makeMapVector(size, a, nullEvery(5));

  auto expectedMap =
      makeMapVector(size, a, [](vector_size_t row) { return row % 5 == 0; });

  auto result = evaluate<MapVector>("map_concat(c0)", makeRowVector({aMap}));
  velox::test::assertEqualVectors(expectedMap, result);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
