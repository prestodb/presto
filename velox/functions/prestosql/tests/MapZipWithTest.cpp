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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class MapZipWithTest : public FunctionBaseTest {};

TEST_F(MapZipWithTest, basic) {
  auto data = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{2, 20}, {1, 10}, {3, 30}},
          {{3, 30}, {1, 10}, {7, 70}, {5, 50}},
          {},
          {{5, 50}},
          {},
      }),
      makeMapVector<int64_t, int64_t>({
          {{1, 11}, {2, 21}, {3, 31}},
          {{1, 11}, {3, 31}, {2, 21}},
          {{1, 11}, {2, 21}},
          {{2, 21}, {1, 11}, {3, 31}},
          {},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  // No capture. Default null behavior for the lambda: v1 + v2.
  auto expected = makeMapVector<int64_t, int64_t>({
      {{1, 21}, {2, 41}, {3, 61}},
      {{1, 21},
       {2, std::nullopt},
       {3, 61},
       {5, std::nullopt},
       {7, std::nullopt}},
      {{1, std::nullopt}, {2, std::nullopt}},
      {{1, std::nullopt},
       {2, std::nullopt},
       {3, std::nullopt},
       {5, std::nullopt}},
      {},
  });

  auto result = evaluate("map_zip_with(c0, c1, (k, v1, v2) -> v1 + v2)", data);
  assertEqualVectors(expected, result);

  result = evaluate("map_zip_with(c1, c0, (k, v1, v2) -> v1 + v2)", data);
  assertEqualVectors(expected, result);

  // No capture. Non-default null behavior for the lambda: coalesce(v1, 0) +
  // coalesce(v2, 0).
  expected = makeMapVector<int64_t, int64_t>({
      {{1, 21}, {2, 41}, {3, 61}},
      {{1, 21}, {2, 21}, {3, 61}, {5, 50}, {7, 70}},
      {{1, 11}, {2, 21}},
      {{1, 11}, {2, 21}, {3, 31}, {5, 50}},
      {},
  });

  result = evaluate(
      "map_zip_with(c0, c1, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))",
      data);
  assertEqualVectors(expected, result);

  result = evaluate(
      "map_zip_with(c1, c0, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))",
      data);
  assertEqualVectors(expected, result);

  // With capture.
  expected = makeMapVector<int64_t, int64_t>({
      {{1, 21}, {2, 41}, {3, 61}},
      {{1, 21 * 2},
       {2, std::nullopt},
       {3, 61 * 2},
       {5, std::nullopt},
       {7, std::nullopt}},
      {{1, std::nullopt}, {2, std::nullopt}},
      {{1, std::nullopt},
       {2, std::nullopt},
       {3, std::nullopt},
       {5, std::nullopt}},
      {},
  });

  result =
      evaluate("map_zip_with(c0, c1, (k, v1, v2) -> (v1 + v2) * c2)", data);
  assertEqualVectors(expected, result);

  result =
      evaluate("map_zip_with(c1, c0, (k, v1, v2) -> (v1 + v2) * c2)", data);
  assertEqualVectors(expected, result);

  expected = makeMapVector<int64_t, int64_t>({
      {{1, 21}, {2, 41}, {3, 61}},
      {{1, 21 * 2}, {2, 21 * 2}, {3, 61 * 2}, {5, 50 * 2}, {7, 70 * 2}},
      {{1, 11 * 3}, {2, 21 * 3}},
      {{1, 11 * 4}, {2, 21 * 4}, {3, 31 * 4}, {5, 50 * 4}},
      {},
  });

  result = evaluate(
      "map_zip_with(c0, c1, (k, v1, v2) -> c2 * (coalesce(v1, 0) + coalesce(v2, 0)))",
      data);
  assertEqualVectors(expected, result);

  result = evaluate(
      "map_zip_with(c1, c0, (k, v1, v2) -> c2 * (coalesce(v1, 0) + coalesce(v2, 0)))",
      data);
  assertEqualVectors(expected, result);
}

TEST_F(MapZipWithTest, nulls) {
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          {{{2, 20}, {1, 10}, {3, 30}}},
          {{{1, 10}, {2, 20}, {3, 30}}},
          std::nullopt,
          std::nullopt,
          {},
          {{{2, 20}, {1, std::nullopt}, {3, 30}}},
      }),
      makeNullableMapVector<int64_t, int64_t>({
          {{{3, 31}, {1, 11}, {2, 21}}},
          std::nullopt,
          {{{1, 11}, {2, 21}, {3, 31}}},
          std::nullopt,
          {},
          {{{2, std::nullopt}, {1, 11}, {3, 31}}},
      }),
  });

  auto expected = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 21}, {2, 41}, {3, 61}}},
      std::nullopt,
      std::nullopt,
      std::nullopt,
      {},
      {{{1, std::nullopt}, {2, std::nullopt}, {3, 61}}},
  });

  auto result = evaluate("map_zip_with(c0, c1, (k, v1, v2) -> v1 + v2)", data);
  assertEqualVectors(expected, result);

  result = evaluate("map_zip_with(c1, c0, (k, v1, v2) -> v1 + v2)", data);
  assertEqualVectors(expected, result);

  expected = makeNullableMapVector<int64_t, int64_t>({
      {{{1, 21}, {2, 41}, {3, 61}}},
      std::nullopt,
      std::nullopt,
      std::nullopt,
      {},
      {{{1, 11}, {2, 20}, {3, 61}}},
  });

  result = evaluate(
      "map_zip_with(c0, c1, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))",
      data);
  assertEqualVectors(expected, result);

  result = evaluate(
      "map_zip_with(c1, c0, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))",
      data);
  assertEqualVectors(expected, result);
}

TEST_F(MapZipWithTest, conditional) {
  auto data = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{2, 20}, {1, 10}, {3, 30}},
          {{3, 30}, {1, 10}, {7, 70}, {5, 50}},
          {},
          {{5, 50}},
          {},
      }),
      makeMapVector<int64_t, int64_t>({
          {{1, 11}, {2, 21}, {3, 31}},
          {{1, 11}, {3, 31}, {2, 21}},
          {{1, 11}, {2, 21}},
          {{2, 21}, {1, 11}, {3, 31}},
          {},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  auto result = evaluate(
      "if (c2 % 2 = 0, "
      "   map_zip_with(c0, c1, (k, v1, v2) -> v1 + v2), "
      "   map_zip_with(c0, c1, (k, v1, v2) -> v1 - v2))",
      data);
  auto expected = makeMapVector<int64_t, int64_t>({
      {{1, -1}, {2, -1}, {3, -1}},
      {{1, 21},
       {2, std::nullopt},
       {3, 61},
       {5, std::nullopt},
       {7, std::nullopt}},
      {{1, std::nullopt}, {2, std::nullopt}},
      {{1, std::nullopt},
       {2, std::nullopt},
       {3, std::nullopt},
       {5, std::nullopt}},
      {},
  });
  assertEqualVectors(expected, result);

  result = evaluate(
      "map_zip_with(c0, c1, "
      "   if (c2 % 2 = 0, (k, v1, v2) -> v1 + v2, (k, v1, v2) -> v1 - v2))",
      data);
  assertEqualVectors(expected, result);
}

TEST_F(MapZipWithTest, fuzz) {
  auto baseData = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{2, 20}, {1, 10}, {3, 30}},
          {{3, 30}, {1, 10}, {7, 70}, {5, 50}},
          {},
          {{5, 50}},
          {},
      }),
      makeMapVector<int64_t, int64_t>({
          {{1, 11}, {2, 21}, {3, 31}},
          {{1, 11}, {3, 31}, {2, 21}},
          {{1, 11}, {2, 21}},
          {{2, 21}, {1, 11}, {3, 31}},
          {},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  VectorFuzzer::Options options;
  options.vectorSize = 1024;
  options.nullRatio = 0.1;

  VectorFuzzer fuzzer(options, pool());

  for (auto i = 0; i < 10; ++i) {
    auto data = makeRowVector({
        fuzzer.fuzzDictionary(baseData->childAt(0), options.vectorSize),
        fuzzer.fuzzDictionary(baseData->childAt(1), options.vectorSize),
        fuzzer.fuzzDictionary(baseData->childAt(2), options.vectorSize),
    });
    auto flatData = flatten<RowVector>(data);

    auto result =
        evaluate("map_zip_with(c0, c1, (k, v1, v2) -> v1 + v2)", data);
    auto expected =
        evaluate("map_zip_with(c0, c1, (k, v1, v2) -> v1 + v2)", flatData);
    assertEqualVectors(expected, result);

    result =
        evaluate("map_zip_with(c0, c1, (k, v1, v2) -> c2 * (v1 + v2))", data);
    expected = evaluate(
        "map_zip_with(c0, c1, (k, v1, v2) -> c2 * (v1 + v2))", flatData);
    assertEqualVectors(expected, result);

    result = evaluate(
        "map_zip_with(c0, c1, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))",
        data);
    expected = evaluate(
        "map_zip_with(c0, c1, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))",
        flatData);
    assertEqualVectors(expected, result);

    result = evaluate(
        "map_zip_with(c0, c1, (k, v1, v2) -> c2 * (coalesce(v1, 0) + coalesce(v2, 0)))",
        data);
    expected = evaluate(
        "map_zip_with(c0, c1, (k, v1, v2) -> c2 * (coalesce(v1, 0) + coalesce(v2, 0)))",
        flatData);
    assertEqualVectors(expected, result);
  }
}

TEST_F(MapZipWithTest, try) {
  auto data = makeRowVector(
      {makeMapVector<int64_t, int64_t>(
           {{{1, 10}, {3, 30}, {2, 20}}, {{3, 30}, {4, 40}}, {{5, 50}}, {}}),
       makeMapVector<int64_t, int64_t>(
           {{{1, 1}, {2, 2}, {3, 0}},
            {{3, 30}, {4, 40}, {0, 0}},
            {},
            {{2, 2}}})});

  ASSERT_THROW(
      evaluate("map_zip_with(c0, c1, (k, v1, v2) -> v1 / v2)", data),
      std::exception);

  auto result =
      evaluate("try(map_zip_with(c0, c1, (k, v1, v2) -> v1 / v2))", data);

  auto expected = makeNullableMapVector<int64_t, int64_t>(
      {std::nullopt,
       {{{0, std::nullopt}, {3, 1}, {4, 1}}},
       {{{5, std::nullopt}}},
       {{{2, std::nullopt}}}});
  assertEqualVectors(expected, result);
}
} // namespace
