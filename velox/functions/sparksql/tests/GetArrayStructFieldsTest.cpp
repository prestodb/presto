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
#include "velox/functions/sparksql/specialforms/GetArrayStructFields.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class GetArrayStructFieldsTest : public SparkFunctionBaseTest {
 protected:
  void testGetArrayStructFields(
      const VectorPtr& input,
      int ordinal,
      const VectorPtr& expected) {
    auto expr = std::make_shared<const core::CallTypedExpr>(
        expected->type(),
        GetArrayStructFieldsCallToSpecialForm::kGetArrayStructFields,
        std::make_shared<const core::FieldAccessTypedExpr>(input->type(), "c0"),
        std::make_shared<core::ConstantTypedExpr>(INTEGER(), variant(ordinal)));

    // Input is flat.
    auto result = evaluate(expr, makeRowVector({input}));
    assertEqualVectors(expected, result);

    // Input is dictionary or constant encoding.
    if (input->size() >= 3) {
      testEncodings(expr, {input}, expected);
    }
  }
};

TEST_F(GetArrayStructFieldsTest, simpleType) {
  std::vector<vector_size_t> offsets{0, 1, 1};
  auto colInt = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto colString = makeNullableFlatVector<std::string>(
      {"hello", "world", std::nullopt, "test"});
  auto colIntWithNull =
      makeNullableFlatVector<int32_t>({11, std::nullopt, 13, 14});
  auto elementVector = makeRowVector({colInt, colString, colIntWithNull});
  auto data = makeArrayVector(offsets, elementVector);

  testGetArrayStructFields(data, 0, makeArrayVector(offsets, colInt));
  testGetArrayStructFields(data, 1, makeArrayVector(offsets, colString));
  testGetArrayStructFields(data, 2, makeArrayVector(offsets, colIntWithNull));
}

TEST_F(GetArrayStructFieldsTest, complexType) {
  std::vector<vector_size_t> offsets{0, 1, 2};
  std::vector<vector_size_t> nulls{1};
  auto colArray = makeNullableArrayVector<int32_t>(
      {{1, 2, std::nullopt},
       {3, std::nullopt, 4},
       {5, std::nullopt, std::nullopt},
       {6, 7, 8}});
  auto colMap = makeMapVector<std::string, int32_t>(
      {{{"a", 0}, {"b", 1}},
       {{"c", 3}, {"d", 4}},
       {{"e", 5}, {"f", 6}},
       {{"g", 7}, {"h", 8}}});
  auto colRow = makeRowVector(
      {makeNullableArrayVector<int32_t>(
           {{100, 101, std::nullopt},
            {std::nullopt},
            {200, std::nullopt, 202},
            {300, 301, 302}}),
       makeNullableFlatVector<std::string>({"a", "b", std::nullopt, "c"})});
  auto elementVector = makeRowVector({colArray, colMap, colRow});
  auto data = makeArrayVector(offsets, elementVector, nulls);

  // Get array field array.
  testGetArrayStructFields(data, 0, makeArrayVector(offsets, colArray, nulls));

  // Get map field array.
  testGetArrayStructFields(data, 1, makeArrayVector(offsets, colMap, nulls));

  // Get row field array.
  testGetArrayStructFields(data, 2, makeArrayVector(offsets, colRow, nulls));
}

TEST_F(GetArrayStructFieldsTest, nullElement) {
  std::vector<vector_size_t> offsets{0, 1, 3};
  auto colInt = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto colIntWithNull =
      makeNullableFlatVector<int32_t>({11, std::nullopt, 13, 14});
  auto elementVector = makeRowVector({colInt, colIntWithNull}, nullEvery(2));
  auto data = makeArrayVector(offsets, elementVector);
  testGetArrayStructFields(
      data,
      0,
      makeArrayVector(
          offsets,
          makeNullableFlatVector<int32_t>({std::nullopt, 2, std::nullopt, 4})));
  testGetArrayStructFields(
      data,
      1,
      makeArrayVector(
          offsets,
          makeNullableFlatVector<int32_t>(
              {std::nullopt, std::nullopt, std::nullopt, 14})));
}

TEST_F(GetArrayStructFieldsTest, decodeElement) {
  std::vector<vector_size_t> offsets{0, 1, 3};
  auto colInt = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto elementVector =
      makeRowVector({wrapInDictionary(makeIndices({1, 0, 2, 3}), colInt)});
  auto data = makeArrayVector(
      offsets, wrapInDictionary(makeIndicesInReverse(4), elementVector));
  testGetArrayStructFields(
      data, 0, makeArrayVector(offsets, makeFlatVector<int32_t>({4, 3, 1, 2})));
}

TEST_F(GetArrayStructFieldsTest, emptyInput) {
  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
      data = {};

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayVector = makeArrayOfRowVector(data, rowType);
  testGetArrayStructFields(arrayVector, 0, makeArrayVector<int32_t>({}));
}

TEST_F(GetArrayStructFieldsTest, invalidOrdinal) {
  std::vector<vector_size_t> offsets{0};
  auto colInt = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto elementVector = makeRowVector({colInt});
  auto data = makeArrayVector(offsets, elementVector);

  VELOX_ASSERT_USER_THROW(
      testGetArrayStructFields(
          elementVector, -1, makeArrayVector(offsets, colInt)),
      "The first argument of get_array_struct_fields should be of array(row) type.");

  VELOX_ASSERT_USER_THROW(
      testGetArrayStructFields(data, -1, makeArrayVector(offsets, colInt)),
      "Invalid ordinal. Should be greater than or equal to 0.");

  VELOX_ASSERT_USER_THROW(
      testGetArrayStructFields(data, 2, makeArrayVector(offsets, colInt)),
      fmt::format("(2 vs. 1) Invalid ordinal 2 for struct with 1 fields."));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
