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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox {

using namespace facebook::velox::test;

class SimpleFunctionCallNullFreeTest
    : public functions::test::FunctionBaseTest {};

// Test that function with default contains nulls behavior won't get called when
// inputs are all null.
template <typename T>
struct DefaultContainsNullsBehaviorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool callNullFree(bool& out, const null_free_arg_type<Array<int32_t>>&) {
    out = true;

    return true;
  }
};

TEST_F(SimpleFunctionCallNullFreeTest, defaultContainsNullsBehavior) {
  registerFunction<DefaultContainsNullsBehaviorFunction, bool, Array<int32_t>>(
      {"default_contains_nulls_behavior"});

  // Make a vector with some null arrays, and some arrays containing nulls.
  auto arrayVector = makeVectorWithNullArrays<int32_t>(
      {std::nullopt, {{1, std::nullopt}}, {{std::nullopt, 2}}, {{1, 2, 3}}});

  // Check that default contains nulls behavior functions don't get called on a
  // null input.
  auto result = evaluate<SimpleVector<bool>>(
      "default_contains_nulls_behavior(c0)", makeRowVector({arrayVector}));
  auto expected = makeNullableFlatVector<bool>(
      {std::nullopt, std::nullopt, std::nullopt, true});
  assertEqualVectors(expected, result);
}

constexpr int32_t kCallNullable = 0;
constexpr int32_t kCallNullFree = 1;

// Test that function with non-default contains nulls behavior.
// Returns the original array prepended with 0 if callNullable
// is invoked or 1 if callNullFree is invoked.
template <typename T>
struct NonDefaultBehaviorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool callNullable(
      out_type<Array<int32_t>>& out,
      const arg_type<Array<int32_t>>* input) {
    out.append(kCallNullable);

    if (input) {
      for (auto i : *input) {
        if (i.has_value()) {
          out.append(i.value());
        }
      }
    }

    return true;
  }

  bool callNullFree(
      out_type<Array<int32_t>>& out,
      const null_free_arg_type<Array<int32_t>>& input) {
    out.append(kCallNullFree);

    for (auto i : input) {
      out.append(i);
    }

    return true;
  }
};

TEST_F(SimpleFunctionCallNullFreeTest, nonDefaultBehavior) {
  registerFunction<NonDefaultBehaviorFunction, Array<int32_t>, Array<int32_t>>(
      {"non_default_behavior"});

  // Make a vector with a NULL.
  auto arrayVectorWithNull = makeVectorWithNullArrays<int32_t>(
      {std::nullopt, {{1, 2}}, {{3, 2}}, {{2, 2, 3}}});
  // Make a vector with an array that contains NULL.
  auto arrayVectorContainsNull = makeVectorWithNullArrays<int32_t>(
      {{{4, std::nullopt}}, {{1, 2}}, {{3, 2}}, {{2, 2, 3}}});
  // Make a vector that's NULL-free.
  auto arrayVectorNullFree =
      makeArrayVector<int32_t>({{4, 5}, {1, 2}, {3, 2}, {2, 2, 3}});

  auto resultWithNull = evaluate<ArrayVector>(
      "non_default_behavior(c0)", makeRowVector({arrayVectorWithNull}));
  auto expectedWithNull = makeArrayVector<int32_t>(
      {{kCallNullable},
       {kCallNullable, 1, 2},
       {kCallNullable, 3, 2},
       {kCallNullable, 2, 2, 3}});
  assertEqualVectors(expectedWithNull, resultWithNull);

  auto resultContainsNull = evaluate<ArrayVector>(
      "non_default_behavior(c0)", makeRowVector({arrayVectorContainsNull}));
  auto expectedContainsNull = makeArrayVector<int32_t>(
      {{kCallNullable, 4},
       {kCallNullable, 1, 2},
       {kCallNullable, 3, 2},
       {kCallNullable, 2, 2, 3}});
  assertEqualVectors(expectedContainsNull, resultContainsNull);

  auto resultNullFree = evaluate<ArrayVector>(
      "non_default_behavior(c0)", makeRowVector({arrayVectorNullFree}));
  auto expectedNullFree = makeArrayVector<int32_t>(
      {{kCallNullFree, 4, 5},
       {kCallNullFree, 1, 2},
       {kCallNullFree, 3, 2},
       {kCallNullFree, 2, 2, 3}});
  assertEqualVectors(expectedNullFree, resultNullFree);
}

template <typename T>
struct DeeplyNestedInputTypeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool callNullFree(
      bool& out,
      const null_free_arg_type<
          Variadic<Row<Array<int32_t>, Map<int32_t, int32_t>>>>&) {
    out = true;

    return true;
  }
};

TEST_F(SimpleFunctionCallNullFreeTest, deeplyNestedInputType) {
  registerFunction<
      DeeplyNestedInputTypeFunction,
      bool,
      Variadic<Row<Array<int32_t>, Map<int32_t, int32_t>>>>(
      {"deeply_nested_input_type"});

  vector_size_t size = 11;
  auto arrayVector1 = makeArrayVector<int32_t>(
      size,
      [&](vector_size_t /* row */) { return 1; },
      [&](vector_size_t idx) { return idx; },
      // First array is NULL.
      [&](vector_size_t row) { return row == 0; },
      // Second array contains NULL.
      [&](vector_size_t idx) { return idx == 0; });
  auto mapVector1 = makeMapVector<int32_t, int32_t>(
      size,
      [&](vector_size_t /* row */) { return 1; },
      [&](vector_size_t idx) { return idx; },
      [&](vector_size_t idx) { return idx; },
      // Third map is NULL.
      [&](vector_size_t row) { return row == 2; },
      // Fourth map has a NULL value.
      [&](vector_size_t idx) { return idx == 2; });
  auto rowVector1 = makeRowVector(
      {"array", "map"},
      {arrayVector1, mapVector1},
      // Fifth row is NULL.
      [&](vector_size_t row) { return row == 4; });
  auto arrayVector2 = makeArrayVector<int32_t>(
      size,
      [&](vector_size_t /* row */) { return 1; },
      [&](vector_size_t idx) { return idx; },
      // Sixth array is NULL.
      [&](vector_size_t row) { return row == 5; },
      // Seventh array contains NULL.
      [&](vector_size_t idx) { return idx == 5; });
  auto mapVector2 = makeMapVector<int32_t, int32_t>(
      size,
      [&](vector_size_t /* row */) { return 1; },
      [&](vector_size_t idx) { return idx; },
      [&](vector_size_t idx) { return idx; },
      // Eigth map is NULL.
      [&](vector_size_t row) { return row == 7; },
      // Ninth map has a NULL value.
      [&](vector_size_t idx) { return idx == 7; });
  auto rowVector2 = makeRowVector(
      {"array", "map"},
      {arrayVector2, mapVector2},
      // Tenth row is NULL
      [&](vector_size_t row) { return row == 9; });

  auto result = evaluate<SimpleVector<bool>>(
      "deeply_nested_input_type(c0, c1)",
      makeRowVector({rowVector1, rowVector2}));
  auto expected = makeNullableFlatVector<bool>(
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       true});
  assertEqualVectors(expected, result);
}
} // namespace facebook::velox
