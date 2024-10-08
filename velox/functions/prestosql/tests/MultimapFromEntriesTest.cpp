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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions {
namespace {

class MultimapFromEntriesTest : public test::FunctionBaseTest {};

TEST_F(MultimapFromEntriesTest, basic) {
  auto row = [](std::vector<variant>&& inputs) {
    return variant::row(std::move(inputs));
  };

  auto input = makeArrayOfRowVector(
      ROW({INTEGER(), VARCHAR()}),
      {
          // No duplicate keys.
          {row({1, "red"}), row({2, "blue"}), row({3, "green"})},
          // Some duplicate keys.
          {row({1, "pink"}),
           row({1, "red"}),
           row({2, "blue"}),
           row({2, variant::null(TypeKind::VARCHAR)}),
           row({2, "lightblue"}),
           row({3, "green"})},
          // Empty array.
          {},
      });

  auto result = evaluate("multimap_from_entries(c0)", makeRowVector({input}));

  auto expectedKeys = makeFlatVector<int32_t>({1, 2, 3, 1, 2, 3});
  auto expectedValues = makeNullableArrayVector<std::string>({
      {"red"},
      {"blue"},
      {"green"},
      {"pink", "red"},
      {"blue", std::nullopt, "lightblue"},
      {"green"},
  });

  auto expectedMap = makeMapVector({0, 3, 6}, expectedKeys, expectedValues);
  assertEqualVectors(expectedMap, result);
}

TEST_F(MultimapFromEntriesTest, nullKey) {
  auto row = [](std::vector<variant>&& inputs) {
    return variant::row(std::move(inputs));
  };

  auto input = makeArrayOfRowVector(
      ROW({INTEGER(), VARCHAR()}),
      {
          {
              row({1, "red"}),
              row({variant::null(TypeKind::INTEGER), "blue"}),
              row({3, "green"}),
          },
      });

  VELOX_ASSERT_THROW(
      evaluate("multimap_from_entries(c0)", makeRowVector({input})),
      "map key cannot be null");
}

TEST_F(MultimapFromEntriesTest, nullEntryFailure) {
  RowVectorPtr rowVector = makeRowVector(
      {makeFlatVector(std::vector<int64_t>{1, 2, 3}),
       makeFlatVector(std::vector<int64_t>{4, 5, 6})});
  rowVector->setNull(0, true);

  ArrayVectorPtr input = makeArrayVector({0}, rowVector);

  VELOX_ASSERT_THROW(
      evaluate("multimap_from_entries(c0)", makeRowVector({input})),
      "map entry cannot be null");

  // Test that having a null entry in the base vector used to create the array
  // vector does not cause a failure.
  input = makeArrayVector({1}, rowVector);

  auto expectedKeys = makeFlatVector<int64_t>({2, 3});
  auto expectedValues = makeArrayVector<int64_t>({{5}, {6}});

  auto expectedMap = makeMapVector({0}, expectedKeys, expectedValues);
  auto result = evaluate("multimap_from_entries(c0)", makeRowVector({input}));
  assertEqualVectors(expectedMap, result);
}

} // namespace
} // namespace facebook::velox::functions
