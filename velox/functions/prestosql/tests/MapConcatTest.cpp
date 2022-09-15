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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::functions::test;

class MapConcatTest : public FunctionBaseTest {
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

TEST_F(MapConcatTest, basic) {
  vector_size_t size = 1'000;

  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}};
  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b, nullEvery(7));

  std::map<std::string, int32_t> ab = concat(a, b);
  auto expectedMap = makeMapVector(
      size, ab, [](vector_size_t row) { return row % 5 == 0 || row % 7 == 0; });

  auto result =
      evaluate<MapVector>("map_concat(c0, c1)", makeRowVector({aMap, bMap}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }

  result =
      evaluate<MapVector>("map_concat(c1, c0)", makeRowVector({aMap, bMap}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapConcatTest, nullKeys) {
  vector_size_t size = 1'000;

  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}};
  auto isNullA = nullEvery(2);
  auto isNullB = nullEvery(3);
  auto aMap = makeMapVector(size, a, isNullA);
  auto bMap = makeMapVector(size, b, isNullB);
  std::map<std::string, int32_t> ab = concat(a, b);

  auto keysAB = mapKeys(ab);
  auto valuesAB = mapValues(ab);
  auto keysA = mapKeys(a);
  auto valuesA = mapValues(a);
  auto keysB = mapKeys(b);
  auto valuesB = mapValues(b);
  auto expectedSizes = [&](vector_size_t mapRow) {
    if (!isNullA(mapRow) && !isNullB(mapRow)) {
      return keysAB.size();
    }
    if (isNullA(mapRow)) {
      return keysB.size();
    }
    if (isNullB(mapRow)) {
      return keysA.size();
    }
    return size_t(0);
  };
  auto expectedValues = [&](vector_size_t mapRow, vector_size_t row) {
    if (!isNullA(mapRow) && !isNullB(mapRow)) {
      return mapRow % 11 + valuesAB[row];
    }
    if (isNullA(mapRow)) {
      return mapRow % 11 + valuesB[row];
    }
    if (isNullB(mapRow)) {
      return mapRow % 11 + valuesA[row];
    }
    return 0;
  };
  auto expectedKeys = [&](vector_size_t mapRow, vector_size_t row) {
    if (!isNullA(mapRow) && !isNullB(mapRow)) {
      return StringView(keysAB[row]);
    }
    if (isNullA(mapRow)) {
      return StringView(keysB[row]);
    }
    if (isNullB(mapRow)) {
      return StringView(keysA[row]);
    }
    return StringView();
  };
  auto expectedMap = vectorMaker_.mapVector<StringView, int32_t>(
      size, expectedSizes, expectedKeys, expectedValues);

  // Setting the parts of the output with null key to empty
  for (auto i = 0; i < size; i++) {
    if (isNullA(i) && isNullB(i)) {
      expectedMap->setOffsetAndSize(i, i, 0);
    }
  }

  auto result = evaluate<MapVector>(
      "map_concat_empty_nulls(c0, c1)", makeRowVector({aMap, bMap}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapConcatTest, duplicateKeys) {
  vector_size_t size = 1'000;

  std::map<std::string, int32_t> a = {
      {"a1", 1}, {"a2", 2}, {"a3", 3}, {"a4", 4}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}, {"a2", -1}};
  auto aMap = makeMapVector(size, a);
  auto bMap = makeMapVector(size, b);

  std::map<std::string, int32_t> ab = concat(a, b);
  auto expectedMap = makeMapVector(size, ab);

  auto result =
      evaluate<MapVector>("map_concat(c0, c1)", makeRowVector({aMap, bMap}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }

  std::map<std::string, int32_t> ba = concat(b, a);
  expectedMap = makeMapVector(size, ba);

  result =
      evaluate<MapVector>("map_concat(c1, c0)", makeRowVector({aMap, bMap}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }

  result =
      evaluate<MapVector>("map_concat(c0, c1)", makeRowVector({aMap, aMap}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(aMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapConcatTest, partiallyPopulated) {
  vector_size_t size = 1'000;

  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}};
  std::map<std::string, int32_t> b = {{"b1", 10}, {"b2", 20}, {"b3", 30}};
  std::map<std::string, int32_t> c = {
      {"c1", 5}, {"c2", 15}, {"c3", 25}, {"c4", 35}};
  auto aMap = makeMapVector(size, a);
  auto bMap = makeMapVector(size, b);
  auto cMap = makeMapVector(size, c);

  std::map<std::string, int32_t> ab = concat(a, b);
  std::map<std::string, int32_t> ac = concat(a, c);
  auto abMap = makeMapVector(size, ab);
  auto acMap = makeMapVector(size, ac);

  auto result = evaluate<MapVector>(
      "if (c0 = 0, map_concat(c1, c2), map_concat(c1, c3))",
      makeRowVector(
          {makeFlatVector<int16_t>(
               size, [](vector_size_t row) { return row % 2; }),
           aMap,
           bMap,
           cMap}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i += 2) {
    ASSERT_TRUE(abMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << abMap->toString(i) << ", got "
        << result->toString(i);
  }
  for (vector_size_t i = 1; i < size; i += 2) {
    ASSERT_TRUE(acMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << acMap->toString(i) << ", got "
        << result->toString(i);
  }
}
