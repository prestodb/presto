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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

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
          result->template asFlatVector<StringView>()->stringBuffers().size());
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
      "greatest(c0)", {{"a"_sv, "b"_sv, "c"_sv}}, {"a"_sv, "b"_sv, "c"_sv});

  runTest<StringView>(
      "greatest(c0, 'aaa')", {{""_sv, "abb"_sv}}, {"aaa"_sv, "abb"_sv});
}

TEST_F(GreatestLeastTest, leasstVarchar) {
  runTest<StringView>(
      "least(c0)", {{"a"_sv, "b"_sv, "c"_sv}}, {"a"_sv, "b"_sv, "c"_sv});

  runTest<StringView>(
      "least(c0, 'aaa')", {{""_sv, "abb"_sv}}, {""_sv, "aaa"_sv});
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

TEST_F(GreatestLeastTest, greatestDate) {
  runTest<Date>(
      "greatest(c0, c1, c2)",
      {{Date(0), Date(5), Date(0)},
       {Date(1), Date(0), Date(-5)},
       {Date(5), Date(-5), Date(-10)}},
      {Date(5), Date(5), Date(0)});
}

TEST_F(GreatestLeastTest, leastDate) {
  runTest<Date>(
      "least(c0, c1, c2)",
      {{Date(0), Date(0), Date(5)},
       {Date(1), Date(-1), Date(-1)},
       {Date(5), Date(5), Date(-5)}},
      {Date(0), Date(-1), Date(-5)});
}

TEST_F(GreatestLeastTest, stringBuffersMoved) {
  runTest<StringView>(
      "least(c0, c1)",
      {{"aaaaaaaaaaaaaa"_sv, "bbbbbbbbbbbbbb"_sv},
       {"cccccccccccccc"_sv, "dddddddddddddd"_sv}},
      {"aaaaaaaaaaaaaa"_sv, "bbbbbbbbbbbbbb"_sv},
      1);

  runTest<StringView>(
      "least(c0, c1, '')",
      {{"aaaaaaaaaaaaaa"_sv, "bbbbbbbbbbbbbb"_sv},
       {"cccccccccccccc"_sv, "dddddddddddddd"_sv}},
      {""_sv, ""_sv},
      0);
}
