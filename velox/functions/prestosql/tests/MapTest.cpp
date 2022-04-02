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

#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

class MapTest : public FunctionBaseTest {};

TEST_F(MapTest, noNulls) {
  auto size = 1'000;

  auto sizeAt = [](vector_size_t row) { return row % 5; };
  auto keyAt = [](vector_size_t row) { return row % 11; };
  auto valueAt = [](vector_size_t row) { return row % 13; };
  auto keys = makeArrayVector<int64_t>(size, sizeAt, keyAt);
  auto values = makeArrayVector<int32_t>(size, sizeAt, valueAt);

  auto expectedMap =
      makeMapVector<int64_t, int32_t>(size, sizeAt, keyAt, valueAt);

  auto result =
      evaluate<MapVector>("map(c0, c1)", makeRowVector({keys, values}));
  assertEqualVectors(expectedMap, result);
}

TEST_F(MapTest, someNulls) {
  auto size = 1'000;

  auto sizeAt = [](vector_size_t row) { return row % 5; };
  auto keyAt = [](vector_size_t row) { return row % 11; };
  auto valueAt = [](vector_size_t row) { return row % 13; };
  auto keys = makeArrayVector<int64_t>(size, sizeAt, keyAt, nullEvery(7));
  auto values = makeArrayVector<int32_t>(size, sizeAt, valueAt);

  auto expectedMap = makeMapVector<int64_t, int32_t>(
      size, sizeAt, keyAt, valueAt, nullEvery(7));

  auto result =
      evaluate<MapVector>("map(c0, c1)", makeRowVector({keys, values}));
  assertEqualVectors(expectedMap, result);
}

TEST_F(MapTest, partiallyPopulated) {
  auto size = 1'000;

  auto sizeAt = [](vector_size_t row) { return row % 5; };
  auto keyAt = [](vector_size_t row) { return row % 11; };
  auto valueAt = [](vector_size_t row) { return row % 13; };
  auto keys = makeArrayVector<int64_t>(size, sizeAt, keyAt);
  auto values = makeArrayVector<int64_t>(size, sizeAt, valueAt);
  auto condition =
      makeFlatVector<int16_t>(size, [](vector_size_t row) { return row % 2; });

  auto expectedEvenMap =
      makeMapVector<int64_t, int64_t>(size, sizeAt, keyAt, valueAt);

  auto expectedOddMap =
      makeMapVector<int64_t, int64_t>(size, sizeAt, valueAt, keyAt);

  auto result = evaluate<MapVector>(
      "if(c2 = 0, map(c0, c1), map(c1, c0))",
      makeRowVector({keys, values, condition}));
  ASSERT_EQ(result->size(), size);
  for (vector_size_t i = 0; i < size; i += 2) {
    ASSERT_TRUE(expectedEvenMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedEvenMap->toString(i)
        << ", got " << result->toString(i);
  }

  for (vector_size_t i = 1; i < size; i += 2) {
    ASSERT_TRUE(expectedOddMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedOddMap->toString(i)
        << ", got " << result->toString(i);
  }
}

TEST_F(MapTest, duplicateKeys) {
  auto size = 1'000;

  auto sizeAt = [](vector_size_t row) { return row % 7; };
  auto keys = makeArrayVector<int64_t>(
      size, sizeAt, [](vector_size_t row) { return row % 3; });
  auto values = makeArrayVector<int32_t>(
      size, sizeAt, [](vector_size_t row) { return row % 5; });

  try {
    evaluate<MapVector>("map(c0, c1)", makeRowVector({keys, values}));
    ASSERT_TRUE(false) << "Expected an error";
  } catch (const VeloxUserError& e) {
    ASSERT_EQ(e.message(), "Duplicate map keys are not allowed");
  }
  // Trying the map version with allowing duplicates
  facebook::velox::functions::prestosql::registerMapAllowingDuplicates(
      std::string("map2"));
  try {
    evaluate<MapVector>("map2(c0, c1)", makeRowVector({keys, values}));
  } catch (const VeloxUserError& e) {
    ASSERT_TRUE(false) << "No error expected";
  }
}

TEST_F(MapTest, differentArraySizes) {
  auto size = 1'000;

  auto keys = makeArrayVector<int64_t>(
      size,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row % 11; });
  auto values = makeArrayVector<int32_t>(
      size,
      [](vector_size_t row) { return row % 7; },
      [](vector_size_t row) { return row % 13; });

  try {
    evaluate<MapVector>("map(c0, c1)", makeRowVector({keys, values}));
    ASSERT_TRUE(false) << "Expected an error";
  } catch (const VeloxUserError& e) {
    ASSERT_EQ(
        e.message(), "(0 vs. 5) Key and value arrays must be the same length");
  }
}

TEST_F(MapTest, encodings) {
  auto size = 1'000;

  auto sizeAt = [](vector_size_t row) { return (row / 2) % 5; };
  auto keyAt = [](vector_size_t row) { return row % 11; };
  auto valueAt = [](vector_size_t row) { return row % 13; };

  // Use different dictionary encodings for keys and values
  auto keys = wrapInDictionary(
      makeOddIndices(size),
      size,
      makeArrayVector<int64_t>(size * 2, sizeAt, keyAt));
  auto values = wrapInDictionary(
      makeEvenIndices(size),
      size,
      makeArrayVector<int32_t>(size * 2, sizeAt, valueAt));

  auto flatKeys = std::dynamic_pointer_cast<ArrayVector>(flatten(keys));
  auto flatValues = std::dynamic_pointer_cast<ArrayVector>(flatten(values));

  auto expectedMap = std::make_shared<MapVector>(
      execCtx_.pool(),
      MAP(BIGINT(), INTEGER()),
      BufferPtr(nullptr),
      size,
      flatKeys->offsets(),
      flatKeys->sizes(),
      flatKeys->elements(),
      flatValues->elements());

  auto result =
      evaluate<MapVector>("map(c0, c1)", makeRowVector({keys, values}));
  assertEqualVectors(expectedMap, result);
}

// Test map function applied to a constant array of keys and flat array of
// values.
TEST_F(MapTest, constantKeys) {
  auto size = 1'000;

  auto sizeAt = [](vector_size_t /*row*/) { return 1; };
  auto keyAt = [](vector_size_t /*row*/) { return "key"_sv; };
  auto valueAt = [](vector_size_t row) { return row; };

  auto expectedMap =
      makeMapVector<StringView, int32_t>(size, sizeAt, keyAt, valueAt);

  auto result = evaluate<MapVector>(
      "map(array['key'], array_constructor(c0))",
      makeRowVector({
          makeFlatVector<int32_t>(size, valueAt),
      }));
  assertEqualVectors(expectedMap, result);
}

// Test map function applied to a flat array of keys and constant array of
// values.
TEST_F(MapTest, constantValues) {
  auto size = 1'000;

  auto sizeAt = [](vector_size_t /*row*/) { return 1; };
  auto keyAt = [](vector_size_t row) { return row; };
  auto valueAt = [](vector_size_t /*row*/) { return "value"_sv; };

  auto expectedMap =
      makeMapVector<int32_t, StringView>(size, sizeAt, keyAt, valueAt);

  auto result = evaluate<MapVector>(
      "map(array_constructor(c0), array['value'])",
      makeRowVector({
          makeFlatVector<int32_t>(size, keyAt),
      }));
  assertEqualVectors(expectedMap, result);
}
