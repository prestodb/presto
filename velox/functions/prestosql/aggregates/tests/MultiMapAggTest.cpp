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
#include <cmath>
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

namespace facebook::velox::aggregate::prestosql {
namespace {

class MultiMapAggTest : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }
};

TEST_F(MultiMapAggTest, integerKeyGlobal) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 1}),
      makeNullableFlatVector<int64_t>({
          10,
          20,
          30,
          40,
          std::nullopt,
          std::nullopt,
          70,
          80,
          90,
          100,
          std::nullopt,
      }),
  });

  auto expected = makeRowVector({
      makeMapVector(
          {
              0,
          },
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeNullableArrayVector<int64_t>({
              {{{10, 20, std::nullopt}}},
              {{{30, 40}}},
              {{{std::nullopt, std::nullopt}}},
              {{{70, 80}}},
              {{{90, 100}}},
          })),
  });

  testAggregations(
      {data},
      {},
      {"multimap_agg(c0, c1)"},
      // Sort the result arrays to ensure deterministic results.
      {"transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});
}

TEST_F(MultiMapAggTest, integerKeyGroupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 1, 2, 2, 1, 1, 1, 2}),
      makeFlatVector<int32_t>({1, 1, 2, 2, 3, 3, 4, 4, 5, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70, 80, 90, 100}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeMapVector(
          {0, 4},
          makeFlatVector<int32_t>({1, 2, 4, 5, /**/ 1, 3, 5}),
          makeArrayVector<int64_t>({
              {10},
              {30, 40},
              {70, 80},
              {90},
              /**/
              {20},
              {50, 60},
              {100},
          })),
  });

  testAggregations(
      {data},
      {"c0"},
      {"multimap_agg(c1, c2)"},
      // Sort the result arrays to ensure deterministic results.
      {"c0", "transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});
}

TEST_F(MultiMapAggTest, stringKeyGlobal) {
  std::vector<std::string> s(10);
  for (auto i = 0; i < 10; ++i) {
    s[i] = std::string(StringView::kInlineSize + 5, '_') + fmt::format("{}", i);
  }
  auto data = makeRowVector({
      makeFlatVector<std::string>(
          {s[0], s[1], s[2], s[3], s[2], s[0], s[1], s[2], s[3], s[4]}),
      makeNullableFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
  });

  auto expected = makeRowVector({
      makeMapVector(
          {
              0,
          },
          makeFlatVector<std::string>({s[0], s[1], s[2], s[3], s[4]}),
          makeArrayVector<int64_t>({
              {0, 5},
              {1, 6},
              {2, 4, 7},
              {3, 8},
              {9},
          })),
  });

  testAggregations(
      {data},
      {},
      {"multimap_agg(c0, c1)"},
      // Sort the result arrays to ensure deterministic results.
      {"transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});
}

TEST_F(MultiMapAggTest, stringKeyGroupBy) {
  std::vector<std::string> s(10);
  for (auto i = 0; i < 10; ++i) {
    s[i] = std::string(StringView::kInlineSize + 5, '_') + fmt::format("{}", i);
  }

  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 1, 2, 2, 1, 2, 1, 1}),
      makeFlatVector<std::string>(
          {s[0], s[1], s[2], s[3], s[2], s[0], s[1], s[2], s[3], s[0]}),
      makeNullableFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeMapVector(
          {
              0,
              4,
          },
          makeFlatVector<std::string>(
              {s[0], s[1], s[2], s[3], /**/ s[0], s[1], s[2]}),
          makeArrayVector<int64_t>({
              {0, 9},
              {6},
              {2},
              {3, 8},
              /**/
              {5},
              {1},
              {4, 7},
          })),
  });

  testAggregations(
      {data},
      {"c0"},
      {"multimap_agg(c1, c2)"},
      // Sort the result arrays to ensure deterministic results.
      {"c0", "transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});
}

TEST_F(MultiMapAggTest, arrayKeyGlobal) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[]",
          "null",
          "[1, 2, 3, 4]",
          "[1, 2]",
          "[1, 2, 3]",
          "[1, 2]",
          "[]",
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8}),
  });

  auto expected = makeRowVector({
      makeMapVector(
          {0},
          makeArrayVectorFromJson<int32_t>({
              "[1, 2, 3]",
              "[]",
              "[1, 2, 3, 4]",
              "[1, 2]",
          }),
          makeArrayVectorFromJson<int64_t>({
              "[1, 6]",
              "[2, 8]",
              "[4]",
              "[5, 7]",
          })),
  });

  testAggregations(
      {data},
      {},
      {"multimap_agg(c0, c1)"},
      // Sort the result arrays to ensure deterministic results.
      {"transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});
}

TEST_F(MultiMapAggTest, arrayKeyGroupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1, 2}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[]",
          "null",
          "[1, 2, 3, 4]",
          "[1, 2]",
          "[1, 2, 3]",
          "[1, 2]",
          "[]",
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeMapVector(
          {0, 2},
          makeArrayVectorFromJson<int32_t>({
              "[1, 2, 3]",
              "[1, 2]",
              "[]",
              "[1, 2, 3]",
              "[1, 2, 3, 4]",
          }),
          makeArrayVectorFromJson<int64_t>({
              "[1]",
              "[5, 7]",
              "[2, 8]",
              "[6]",
              "[4]",
          })),
  });

  testAggregations(
      {data},
      {"c0"},
      {"multimap_agg(c1, c2)"},
      // Sort the result arrays to ensure deterministic results.
      {"c0", "transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});
}

TEST_F(MultiMapAggTest, doubleKeyGlobal) {
  // Verify that all NaN representations used as a map key are treated as equal
  static const double KNan1 = std::nan("1");
  static const double KNan2 = std::nan("2");
  auto data = makeRowVector({
      makeFlatVector<double>(
          {KNan1, KNan2, 1.1, 0.2, 23.0, 2.0, 23.0, 2.0, 1.1, 0.2, 23.0, 2.0}),
      makeNullableFlatVector<int64_t>(
          {-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
  });

  auto expected = makeRowVector({
      makeMapVector(
          {
              0,
          },
          makeFlatVector<double>({KNan1, 0.2, 1.1, 2.0, 23.0}),
          makeArrayVector<int64_t>({
              {-2, -1},
              {1, 7},
              {0, 6},
              {3, 5, 9},
              {2, 4, 8},
          })),
  });

  testAggregations(
      {data},
      {},
      {"multimap_agg(c0, c1)"},
      // Sort the result arrays to ensure deterministic results.
      {"transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});

  // Input keys are complex type (row).
  data = makeRowVector({
      makeRowVector(
          {makeFlatVector<double>(
               {KNan1,
                KNan2,
                1.1,
                0.2,
                23.0,
                2.0,
                23.0,
                2.0,
                1.1,
                0.2,
                23.0,
                2.0}),
           makeFlatVector<int64_t>(std::vector<int64_t>(13, 0))}),
      makeFlatVector<int64_t>({-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
  });

  expected = makeRowVector({
      makeMapVector(
          {
              0,
          },
          makeRowVector(
              {makeFlatVector<double>({KNan1, 0.2, 1.1, 2.0, 23.0}),
               makeFlatVector<int64_t>(std::vector<int64_t>(5, 0))}),
          makeArrayVector<int64_t>({
              {-2, -1},
              {1, 7},
              {0, 6},
              {3, 5, 9},
              {2, 4, 8},
          })),
  });

  testAggregations(
      {data},
      {},
      {"multimap_agg(c0, c1)"},
      // Sort the result arrays to ensure deterministic results.
      {"transform_values(a0, (k, v) -> array_sort(v))"},
      {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::prestosql
