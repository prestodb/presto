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

#include <optional>
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;

class GreatestLeastTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  void runTest(
      const std::string& query,
      const std::vector<std::vector<T>>& inputs,
      const std::vector<T>& output,
      std::optional<size_t> stringBuffersExpectedCount = std::nullopt) {
    // Create input vectors
    auto vectorSize = inputs[0].size();
    std::vector<VectorPtr> inputColumns(inputs.size());
    for (auto i = 0; i < inputColumns.size(); ++i) {
      inputColumns[i] = makeFlatVector<T>(inputs[i]);
      for (auto j = 0; j < vectorSize; ++j) {
        inputColumns[i]->asFlatVector<T>()->set(j, inputs[i][j]);
      }
    }

    // Call evaluate to run the query on the created input
    auto result = evaluate<SimpleVector<T>>(query, makeRowVector(inputColumns));
    for (int32_t i = 0; i < vectorSize; ++i) {
      ASSERT_EQ(result->valueAt(i), output[i]);
    }

    if (stringBuffersExpectedCount.has_value()) {
      ASSERT_EQ(
          *stringBuffersExpectedCount,
          result->template asFlatVector<StringView>()
              ->getStringBuffers()
              .size());
    }
  }
};

TEST_F(GreatestLeastTest, leastDouble) {
  runTest<double>("least(c0)", {{0, 1.1, -1.1}}, {0, 1.1, -1.1});
  runTest<double>("least(c0, 1.0)", {{0, 1.1, -1.1}}, {0, 1, -1.1});
  runTest<double>(
      "least(c0, 1.0 , c1)", {{0, 1.1, -1.1}, {100, -100, 0}}, {0, -100, -1.1});
}

TEST_F(GreatestLeastTest, nanInput) {
  assertUserInvalidArgument(
      [&]() { runTest<double>("least(c0)", {{0.0 / 0.0}}, {0}); },
      "Invalid argument to least(): NaN");

  assertUserInvalidArgument(
      [&]() {
        runTest<double>("greatest(c0)", {1, {0.0 / 0.0}}, {1, 0});
      },
      "Invalid argument to greatest(): NaN");
}

TEST_F(GreatestLeastTest, greatestDouble) {
  runTest<double>("greatest(c0)", {{0, 1.1, -1.1}}, {0, 1.1, -1.1});
  runTest<double>("greatest(c0, 1.0)", {{0, 1.1, -1.1}}, {1, 1.1, 1});
  runTest<double>(
      "greatest(c0, 1.0 , c1)",
      {{0, 1.1, -1.1}, {100, -100, 0}},
      {100, 1.1, 1});
}

TEST_F(GreatestLeastTest, leastBigInt) {
  runTest<int64_t>("least(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>("least(c0, 1)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>(
      "least(c0, 1 , c1)", {{0, 1, -1}, {100, -100, 0}}, {0, -100, -1});
}

TEST_F(GreatestLeastTest, greatestBigInt) {
  runTest<int64_t>("greatest(c0)", {{0, 1, -1}}, {0, 1, -1});
  runTest<int64_t>("greatest(c0, 1)", {{0, 1, -1}}, {1, 1, 1});
  runTest<int64_t>(
      "greatest(c0, 1 , c1)", {{0, 1, -1}, {100, -100, 0}}, {100, 1, 1});
}

TEST_F(GreatestLeastTest, greatestVarchar) {
  runTest<StringView>(
      "greatest(c0)",
      {{StringView("a"), StringView("b"), StringView("c")}},
      {StringView("a"), StringView("b"), StringView("c")});

  runTest<StringView>(
      "greatest(c0, 'aaa')",
      {{StringView(""), StringView("abb")}},
      {StringView("aaa"), StringView("abb")});
}

TEST_F(GreatestLeastTest, leasstVarchar) {
  runTest<StringView>(
      "least(c0)",
      {{StringView("a"), StringView("b"), StringView("c")}},
      {StringView("a"), StringView("b"), StringView("c")});

  runTest<StringView>(
      "least(c0, 'aaa')",
      {{StringView(""), StringView("abb")}},
      {StringView(""), StringView("aaa")});
}

TEST_F(GreatestLeastTest, greatestTimeStamp) {
  runTest<Timestamp>(
      "greatest(c0, c1, c2)",
      {{Timestamp(0, 0), Timestamp(10, 100), Timestamp(100, 10)},
       {Timestamp(1, 0), Timestamp(10, 1), Timestamp(100, 10)},
       {Timestamp(0, 1), Timestamp(312, 100), Timestamp(100, 11)}},
      {Timestamp(1, 0), Timestamp(312, 100), Timestamp(100, 11)});
}

TEST_F(GreatestLeastTest, leastTimeStamp) {
  runTest<Timestamp>(
      "least(c0, c1, c2)",
      {{Timestamp(0, 0), Timestamp(10, 100), Timestamp(100, 10)},
       {Timestamp(1, 0), Timestamp(10, 1), Timestamp(100, 10)},
       {Timestamp(0, 1), Timestamp(312, 100), Timestamp(1, 10)}},
      {Timestamp(0, 0), Timestamp(10, 1), Timestamp(1, 10)});
}

TEST_F(GreatestLeastTest, stringBuffersMoved) {
  runTest<StringView>(
      "least(c0, c1)",
      {{StringView("aaaaaaaaaaaaaa"), StringView("bbbbbbbbbbbbbb")},
       {StringView("cccccccccccccc"), StringView("dddddddddddddd")}},
      {StringView("aaaaaaaaaaaaaa"), StringView("bbbbbbbbbbbbbb")},
      1);

  runTest<StringView>(
      "least(c0, c1, '')",
      {{StringView("aaaaaaaaaaaaaa"), StringView("bbbbbbbbbbbbbb")},
       {StringView("cccccccccccccc"), StringView("dddddddddddddd")}},
      {StringView(""), StringView("")},
      0);
}
