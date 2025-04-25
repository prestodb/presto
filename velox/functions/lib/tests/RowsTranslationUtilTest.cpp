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
#include "velox/functions/lib/RowsTranslationUtil.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <velox/type/Type.h>

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions {

using namespace facebook::velox::test;

namespace {

SelectivityVector expectedRows(std::vector<bool> selectedRows) {
  SelectivityVector ret(selectedRows.size(), false);
  for (int i = 0; i < selectedRows.size(); i++) {
    ret.setValid(i, selectedRows[i]);
  }
  ret.updateBounds();
  return ret;
}

class RowsTranslationUtilTest : public test::FunctionBaseTest {};
TEST_F(RowsTranslationUtilTest, elementRowsIterator) {
  // Verify that the elementRowsIterator correctly partitions the rows among
  // iterations based on a set max row limit. We use an array vector and vary
  // the element size of each row and the max row limit to exercise various edge
  // cases.

  const vector_size_t maxElementRowsPerIteration = 3;
  {
    ArrayVectorPtr inputArrayVector = makeArrayVectorFromJson<int32_t>(
        {"[0, 1]", "[2, 3, 4]", "[5]", "[6,7]", "[8, 9]"});

    SelectivityVector rows(inputArrayVector->size(), true);
    // To be resued among test cases.
    SelectivityVector actualTopLevelRows(inputArrayVector->size(), false);
    SelectivityVector expectedTopLevelRows(inputArrayVector->size(), false);

    SelectivityVector actualElementRows(
        inputArrayVector->elements()->size(), false);
    SelectivityVector expectedElementRows(
        inputArrayVector->elements()->size(), false);
    ElementRowsIterator elementRowsIter(
        maxElementRowsPerIteration, rows, inputArrayVector.get());

    // Iteration ends in the middle of an array value
    EXPECT_TRUE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
    EXPECT_EQ(actualTopLevelRows, expectedRows({1, 1, 0, 0, 0}));
    EXPECT_EQ(actualElementRows, expectedRows({1, 1, 1, 1, 1, 0, 0, 0, 0, 0}));

    // Iteration ends at the end of an array value
    EXPECT_TRUE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
    EXPECT_EQ(actualTopLevelRows, expectedRows({0, 0, 1, 1, 0}));
    EXPECT_EQ(actualElementRows, expectedRows({0, 0, 0, 0, 0, 1, 1, 1, 0, 0}));

    // Iteration ends at the ends of elements vector without hitting the max
    // element limit
    EXPECT_TRUE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
    EXPECT_EQ(actualTopLevelRows, expectedRows({0, 0, 0, 0, 1}));
    EXPECT_EQ(actualElementRows, expectedRows({0, 0, 0, 0, 0, 0, 0, 0, 1, 1}));

    EXPECT_FALSE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
  }
  {
    ArrayVectorPtr inputArrayVector = makeArrayVectorFromJson<int32_t>(
        {"[0, 1]",
         "null", // null top level array
         "[2,3,4]",
         "[null]", // null array element
         "[5]",
         "[6]",
         "[]", // empty array
         "[7, 8, 9]"});

    SelectivityVector rows(inputArrayVector->size(), true);
    // To be resued among test cases.
    SelectivityVector actualTopLevelRows(inputArrayVector->size(), false);
    SelectivityVector expectedTopLevelRows(inputArrayVector->size(), false);

    SelectivityVector actualElementRows(
        inputArrayVector->elements()->size(), false);
    SelectivityVector expectedElementRows(
        inputArrayVector->elements()->size(), false);

    ElementRowsIterator elementRowsIter(
        maxElementRowsPerIteration, rows, inputArrayVector.get());
    // Iteration includes top level row which is null at the top level.
    EXPECT_TRUE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
    EXPECT_EQ(actualTopLevelRows, expectedRows({1, 1, 1, 0, 0, 0, 0, 0}));
    EXPECT_EQ(
        actualElementRows, expectedRows({1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0}));

    // Iteration includes element row which has null element inside array.
    EXPECT_TRUE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
    EXPECT_EQ(actualTopLevelRows, expectedRows({0, 0, 0, 1, 1, 1, 0, 0}));
    EXPECT_EQ(
        actualElementRows, expectedRows({0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0}));

    // Iteration includes top level row for an empty array.
    EXPECT_TRUE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
    EXPECT_EQ(actualTopLevelRows, expectedRows({0, 0, 0, 0, 0, 0, 1, 1}));
    EXPECT_EQ(
        actualElementRows, expectedRows({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1}));

    EXPECT_FALSE(elementRowsIter.next(actualElementRows, actualTopLevelRows));
  }
}
} // namespace
} // namespace facebook::velox::functions
