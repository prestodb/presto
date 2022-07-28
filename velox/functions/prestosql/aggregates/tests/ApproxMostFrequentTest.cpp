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

#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

namespace facebook::velox::aggregate::test {
namespace {

template <typename T>
struct ApproxMostFrequentTest : AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  std::shared_ptr<FlatVector<int>> makeGroupKeys() {
    return makeFlatVector<int>(3, [](auto row) { return row; });
  }

  std::shared_ptr<FlatVector<int>> makeKeys() {
    return makeFlatVector<int>(
        1000, [](auto row) { return static_cast<int>(std::sqrt(row)) % 3; });
  }

  std::shared_ptr<FlatVector<T>> makeValues() {
    return makeFlatVector<T>(1000, [](auto row) { return std::sqrt(row); });
  }

  std::shared_ptr<FlatVector<T>> makeValuesWithNulls() {
    auto values = makeValues();
    for (int i = 0; i < values->size(); ++i) {
      if (static_cast<int>(std::sqrt(i)) % 3 == 0) {
        values->setNull(i, true);
      }
    }
    return values;
  }

  MapVectorPtr makeGlobalExpected() {
    return makeMapVector<T, int64_t>({{{30, 61}, {29, 59}, {28, 57}}});
  }

  MapVectorPtr makeGroupedExpected() {
    return makeMapVector<T, int64_t>(
        {{{24, 49}, {27, 55}, {30, 61}},
         {{22, 45}, {25, 51}, {28, 57}},
         {{23, 47}, {26, 53}, {29, 59}}});
  }

  MapVectorPtr makeEmptyGroupExpected() {
    auto expected = makeGroupedExpected();
    expected->setNull(0, true);
    return expected;
  }
};

template <>
std::shared_ptr<FlatVector<StringView>>
ApproxMostFrequentTest<StringView>::makeValues() {
  std::string s[32];
  for (int i = 0; i < 32; ++i) {
    s[i] = std::to_string(i);
  }
  return makeFlatVector<StringView>(1000, [&](auto row) {
    return StringView(s[static_cast<int>(std::sqrt(row))]);
  });
}

template <>
MapVectorPtr ApproxMostFrequentTest<StringView>::makeGlobalExpected() {
  return makeMapVector<StringView, int64_t>(
      {{{"30", 61}, {"29", 59}, {"28", 57}}});
}

template <>
MapVectorPtr ApproxMostFrequentTest<StringView>::makeGroupedExpected() {
  return makeMapVector<StringView, int64_t>(
      {{{"24", 49}, {"27", 55}, {"30", 61}},
       {{"22", 45}, {"25", 51}, {"28", 57}},
       {{"23", 47}, {"26", 53}, {"29", 59}}});
}

using ValueTypes = ::testing::Types<int, StringView>;
TYPED_TEST_SUITE(ApproxMostFrequentTest, ValueTypes);

TYPED_TEST(ApproxMostFrequentTest, global) {
  auto values = this->makeValues();
  auto expected = this->makeGlobalExpected();
  this->testAggregations(
      {this->makeRowVector({values})},
      {},
      {"approx_most_frequent(3, c0, 31)"},
      {this->makeRowVector({expected})});
}

TYPED_TEST(ApproxMostFrequentTest, grouped) {
  auto values = this->makeValues();
  auto keys = this->makeKeys();
  auto groupKeys = this->makeGroupKeys();
  auto expected = this->makeGroupedExpected();
  this->testAggregations(
      {this->makeRowVector({keys, values})},
      {"c0"},
      {"approx_most_frequent(3, c1, 11)"},
      {this->makeRowVector({groupKeys, expected})});
}

// TODO This test crashes. Fix and re-enable.
TYPED_TEST(ApproxMostFrequentTest, DISABLED_emptyGroup) {
  auto values = this->makeValuesWithNulls();
  auto keys = this->makeKeys();
  auto groupKeys = this->makeGroupKeys();
  auto expected = this->makeEmptyGroupExpected();
  this->testAggregations(
      {this->makeRowVector({keys, values})},
      {"c0"},
      {"approx_most_frequent(3, c1, 11)"},
      {this->makeRowVector({groupKeys, expected})});
}

} // namespace
} // namespace facebook::velox::aggregate::test
