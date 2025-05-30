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

#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Variant.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

class MapTest : public SparkFunctionBaseTest {
 protected:
  template <typename K = int64_t, typename V = std::string>
  void testMap(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      const VectorPtr& expected) {
    auto result = evaluate<MapVector>(expression, makeRowVector(parameters));
    ::facebook::velox::test::assertEqualVectors(expected, result);
  }

  void testMapFails(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      const std::string errorMsg) {
    VELOX_ASSERT_USER_THROW(
        evaluate<MapVector>(expression, makeRowVector(parameters)), errorMsg);
  }
};

TEST_F(MapTest, Basics) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 = makeNullableFlatVector<int64_t>({4, 5, 6});
  auto mapVector =
      makeMapVector<int64_t, int64_t>({{{1, 4}}, {{2, 5}}, {{3, 6}}});
  testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
}

TEST_F(MapTest, Nulls) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 =
      makeNullableFlatVector<int64_t>({std::nullopt, 5, std::nullopt});
  auto mapVector = makeMapVector<int64_t, int64_t>(
      {{{1, std::nullopt}}, {{2, 5}}, {{3, std::nullopt}}});
  testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
}

TEST_F(MapTest, differentTypes) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
  auto mapVector =
      makeMapVector<int64_t, double>({{{1, 4.0}}, {{2, 5.0}}, {{3, 6.0}}});
  testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
}

TEST_F(MapTest, boolType) {
  auto inputVector1 = makeNullableFlatVector<bool>({1, 1, 0});
  auto inputVector2 = makeNullableFlatVector<bool>({0, 0, 1});
  auto mapVector = makeMapVector<bool, bool>({{{1, 0}}, {{1, 0}}, {{0, 1}}});
  testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
}

TEST_F(MapTest, duplicateMapKey) {
  // Input vectors with duplicate keys.
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 20, 3});
  auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
  auto inputVector3 = makeNullableFlatVector<int64_t>({10, 20, 30});
  auto inputVector4 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
  auto inputVector5 = makeNullableFlatVector<int64_t>({100, 200, 30});
  auto inputVector6 = makeNullableFlatVector<double>({4.2, 5.2, 6.2});

  // Deduplicate map keys based on LAST_WIN policy.
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kThrowExceptionOnDuplicateMapKeys, "false"}});

  const auto expectedMapVector1 =
      makeMapVector<int64_t, double>({{{1, 4.0}}, {{20, 5.0}}, {{3, 6.0}}});
  const auto expectedMapVector2 = makeMapVector<int64_t, double>(
      {{{10, 4.1}, {100, 4.2}}, {{20, 5.1}, {200, 5.2}}, {{30, 6.2}}});
  const auto expectedMapVector3 = makeMapVector<int64_t, double>(
      {{{1, 4.0}, {10, 4.1}, {100, 4.2}},
       {{20, 5.1}, {200, 5.2}},
       {{3, 6.0}, {30, 6.2}}});

  testMap("map(c0, c1)", {inputVector1, inputVector2}, expectedMapVector1);

  testMap(
      "map(c0, c1, c2, c3)",
      {inputVector3, inputVector4, inputVector5, inputVector6},
      expectedMapVector2);

  testMap(
      "map(c0, c1, c2, c3, c4, c5)",
      {inputVector1,
       inputVector2,
       inputVector3,
       inputVector4,
       inputVector5,
       inputVector6},
      expectedMapVector3);

  // Throw exception when duplicate keys are found.
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kThrowExceptionOnDuplicateMapKeys, "true"}});

  testMapFails(
      "map(c0, c1, c2, c3)",
      {inputVector3, inputVector4, inputVector5, inputVector6},
      "Duplicate map key (30) was found.");

  testMapFails(
      "map(c0, c1, c2, c3, c4, c5)",
      {inputVector1,
       inputVector2,
       inputVector3,
       inputVector4,
       inputVector5,
       inputVector6},
      "Duplicate map key (20) was found.");
}

TEST_F(MapTest, wide) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
  auto inputVector11 = makeNullableFlatVector<int64_t>({10, 20, 30});
  auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
  auto mapVector = makeMapVector<int64_t, double>(
      {{{1, 4.0}, {10, 4.1}}, {{2, 5.0}, {20, 5.1}}, {{3, 6.0}, {30, 6.1}}});
  testMap(
      "map(c0, c1, c2, c3)",
      {inputVector1, inputVector2, inputVector11, inputVector22},
      mapVector);
}

TEST_F(MapTest, errorCases) {
  auto inputVectorInt64 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVectorDouble = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
  auto nullInputVector = makeNullableFlatVector<int64_t>({1, std::nullopt, 3});

  // Number of args.
  testMapFails(
      "map(c0)",
      {inputVectorInt64},
      "Scalar function signature is not supported: map(BIGINT)");
  testMapFails(
      "map(c0, c1, c2)",
      {inputVectorInt64, inputVectorDouble, inputVectorInt64},
      "Scalar function signature is not supported: map(BIGINT, DOUBLE, BIGINT)");

  testMapFails(
      "map(c0, c1, c2, c3, c4, c5, c6, c7)",
      {inputVectorDouble,
       inputVectorDouble,
       inputVectorDouble,
       inputVectorDouble,
       inputVectorDouble,
       inputVectorDouble,
       inputVectorDouble,
       inputVectorDouble},
      "Scalar function signature is not supported: map(DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE)");

  // Types of args.
  testMapFails(
      "map(c0, c1, c2, c3)",
      {inputVectorInt64,
       inputVectorDouble,
       inputVectorDouble,
       inputVectorDouble},
      "Scalar function signature is not supported: map(BIGINT, DOUBLE, DOUBLE, DOUBLE)");
  testMapFails(
      "map(c0, c1, c2, c3)",
      {inputVectorDouble,
       inputVectorInt64,
       inputVectorDouble,
       inputVectorDouble},
      "Scalar function signature is not supported: map(DOUBLE, BIGINT, DOUBLE, DOUBLE)");

  testMapFails(
      "map(c0, c1)",
      {nullInputVector, inputVectorDouble},
      "Cannot use null as map key");
}

TEST_F(MapTest, complexTypes) {
  auto makeSingleMapVector = [&](const VectorPtr& keyVector,
                                 const VectorPtr& valueVector) {
    return makeMapVector(
        {
            0,
        },
        keyVector,
        valueVector);
  };

  auto makeSingleRowVector = [&](vector_size_t size = 1,
                                 vector_size_t base = 0) {
    return makeRowVector({
        makeFlatVector<int64_t>(size, [&](auto row) { return row + base; }),
    });
  };

  auto testSingleMap = [&](const VectorPtr& keyVector,
                           const VectorPtr& valueVector) {
    testMap(
        "map(c0, c1)",
        {keyVector, valueVector},
        makeSingleMapVector(keyVector, valueVector));
  };

  auto arrayKey = makeArrayVectorFromJson<int64_t>({"[1, 2, 3]"});
  auto arrayValue = makeArrayVectorFromJson<int64_t>({"[1, 3, 5]"});
  auto nullArrayValue = makeArrayVectorFromJson<int64_t>({"null"});

  testSingleMap(makeSingleRowVector(), makeSingleRowVector(1, 2));

  testSingleMap(arrayKey, arrayValue);

  testSingleMap(
      makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector(1, 3)),
      makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector(1, 2)));

  testSingleMap(
      makeSingleMapVector(
          makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector()),
          makeSingleRowVector()),
      makeSingleMapVector(
          arrayKey,
          makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector())));

  testSingleMap(arrayKey, nullArrayValue);

  auto mixedArrayKey1 = makeArrayVector<int64_t>({{1, 2, 3}});
  auto mixedRowValue1 = makeSingleRowVector();
  auto mixedArrayKey2 = makeArrayVector<int64_t>({{4, 5}});
  auto mixedRowValue2 = makeSingleRowVector(1, 1);
  auto mixedMapResult = makeSingleMapVector(
      makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}}), makeSingleRowVector(2, 0));
  testMap(
      "map(c0, c1, c2, c3)",
      {mixedArrayKey1, mixedRowValue1, mixedArrayKey2, mixedRowValue2},
      mixedMapResult);

  auto arrayMapResult1 = makeMapVector(
      {
          0,
          1,
      },
      makeArrayVector<int64_t>({{1, 2, 3}, {7, 9}}),
      makeArrayVector<int64_t>({{1, 2}, {4, 6}}));
  testMap(
      "map(c0, c1)",
      {makeArrayVector<int64_t>({{1, 2, 3}, {7, 9}}),
       makeArrayVector<int64_t>({{1, 2}, {4, 6}})},
      arrayMapResult1);
}

TEST_F(MapTest, complexTypesDuplicateMapKey) {
  auto makeSingleMapVector = [&](const VectorPtr& keyVector,
                                 const VectorPtr& valueVector) {
    return makeMapVector(
        {
            0,
        },
        keyVector,
        valueVector);
  };
  auto arrayKey = makeArrayVectorFromJson<int64_t>({"[1, 2, 3]"});
  auto arrayValue = makeArrayVectorFromJson<int64_t>({"[1, 3, 5]"});
  auto nullArrayValue = makeArrayVectorFromJson<int64_t>({"null"});

  // Deduplicate map keys based on LAST_WIN policy.
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kThrowExceptionOnDuplicateMapKeys, "false"}});
  testMap(
      "map(c0, c1, c2, c3)",
      {arrayKey, arrayValue, arrayKey, arrayValue},
      makeSingleMapVector(arrayKey, arrayValue));

  testMap(
      "map(c0, c1, c2, c3)",
      {arrayKey, nullArrayValue, arrayKey, nullArrayValue},
      makeSingleMapVector(arrayKey, nullArrayValue));

  // Throw exception on duplicate map key.
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kThrowExceptionOnDuplicateMapKeys, "true"}});
  testMapFails(
      "map(c0, c1, c2, c3)",
      {arrayKey, arrayValue, arrayKey, arrayValue},
      "Duplicate map key (3 elements starting at 0 {1, 2, 3}) was found.");
}

TEST_F(MapTest, complexTypesWithNestedNullsDuplicateMapKey) {
  // Create array keys with nulls.
  auto arrayKeysWithNull1 = makeNullableArrayVector<int64_t>(
      {{1, std::nullopt, 3}, {4, 5, std::nullopt}, {7, 8, 9}});
  auto valuesForKey1 = makeArrayVector<std::string>(
      {{"a", "b", "c"}, {"d", "e", "f"}, {"g", "h", "i"}});

  // Create duplicate keys with the same null pattern.
  auto arrayKeysWithNull2 = makeNullableArrayVector<int64_t>(
      {{1, std::nullopt, 3}, {10, 11, 12}, {13, 14, 15}});
  auto valuesForKey2 = makeArrayVector<std::string>(
      {{"x", "y", "z"}, {"p", "q", "r"}, {"s", "t", "u"}});

  // Create complex type with map containing null values.
  auto mapKey1 = makeMapVector<int64_t, int64_t>(
      {{{1, 10}, {2, std::nullopt}},
       {{3, 30}, {4, std::nullopt}},
       {{5, 50}, {6, 60}}});
  auto valueForMapKey1 = makeFlatVector<int64_t>({100, 200, 300});

  // Create duplicate complex map key with the same null pattern.
  auto mapKey2 = makeMapVector<int64_t, int64_t>(
      {{{1, 10}, {2, std::nullopt}},
       {{30, 300}, {40, 400}},
       {{50, 500}, {60, 600}}});
  auto valueForMapKey2 = makeFlatVector<int64_t>({1000, 2000, 3000});

  // Deduplicate map keys based on LAST_WIN policy.
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kThrowExceptionOnDuplicateMapKeys, "false"}});

  // Test case 1: Array keys with nulls.
  // Expected: [1, null, 3] appears twice, last value ["x", "y", "z"] wins.
  auto expectedArrayMap = makeMapVector(
      {0, 1, 3},
      makeNullableArrayVector<int64_t>(
          {{1, std::nullopt, 3},
           {4, 5, std::nullopt},
           {10, 11, 12},
           {7, 8, 9},
           {13, 14, 15}}),
      makeArrayVector<std::string>(
          {{"x", "y", "z"},
           {"d", "e", "f"},
           {"p", "q", "r"},
           {"g", "h", "i"},
           {"s", "t", "u"}}));

  testMap(
      "map(c0, c1, c2, c3)",
      {arrayKeysWithNull1, valuesForKey1, arrayKeysWithNull2, valuesForKey2},
      expectedArrayMap);

  // Test case 2: Map keys with nulls.
  // Expected: {{1, 10}, {2, null}} appears twice, last value 1000 wins.
  auto expectedMapOfMap = makeMapVector(
      {0, 1, 3},
      makeMapVector<int64_t, int64_t>(
          {{{1, 10}, {2, std::nullopt}},
           {{3, 30}, {4, std::nullopt}},
           {{30, 300}, {40, 400}},
           {{5, 50}, {6, 60}},
           {{50, 500}, {60, 600}}}),
      makeFlatVector<int64_t>({1000, 200, 2000, 300, 3000}));

  testMap(
      "map(c0, c1, c2, c3)",
      {mapKey1, valueForMapKey1, mapKey2, valueForMapKey2},
      expectedMapOfMap);

  // Test case 3: Test with deeply nested structures containing nulls.
  auto rowKey1 = makeRowVector(
      {makeNullableArrayVector<int64_t>({{1, std::nullopt, 3}}),
       makeMapVector<int64_t, int64_t>({{{1, 10}, {2, std::nullopt}}})});
  auto valueForRow1 = makeFlatVector<std::string>({"first"});

  // Duplicate row key with same null pattern.
  auto rowKey2 = makeRowVector(
      {makeNullableArrayVector<int64_t>({{1, std::nullopt, 3}}),
       makeMapVector<int64_t, int64_t>({{{1, 10}, {2, std::nullopt}}})});
  auto valueForRow2 = makeFlatVector<std::string>({"last"});

  // Expected: Complex row with nulls appears twice, last value "last" wins.
  auto expectedRowMap = makeMapVector(
      {0},
      makeRowVector(
          {makeNullableArrayVector<int64_t>({{1, std::nullopt, 3}}),
           makeMapVector<int64_t, int64_t>({{{1, 10}, {2, std::nullopt}}})}),
      makeFlatVector<std::string>({"last"}));

  testMap(
      "map(c0, c1, c2, c3)",
      {rowKey1, valueForRow1, rowKey2, valueForRow2},
      expectedRowMap);

  // Test case 4: Test with exception throwing enabled.
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kThrowExceptionOnDuplicateMapKeys, "true"}});

  testMapFails(
      "map(c0, c1, c2, c3)",
      {arrayKeysWithNull1, valuesForKey1, arrayKeysWithNull2, valuesForKey2},
      "Duplicate map key (3 elements starting at 0 {1, null, 3}) was found.");

  testMapFails(
      "map(c0, c1, c2, c3)",
      {mapKey1, valueForMapKey1, mapKey2, valueForMapKey2},
      "Duplicate map key (2 elements starting at 0 {1 => 10, 2 => null}) was found.");
}

TEST_F(MapTest, resultSize) {
  auto condition = makeFlatVector<int64_t>({1, 2, 3});
  auto keys = makeFlatVector<int64_t>({3, 2, 1});
  auto values = makeFlatVector<int64_t>({4, 5, 6});
  auto mapVector =
      makeMapVector<int64_t, int64_t>({{{4, 3}}, {{5, 2}}, {{1, 6}}});
  testMap(
      "if(greaterthan(c2, 2), map(c0, c1), map(c1, c0))",
      {keys, values, condition},
      mapVector);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
