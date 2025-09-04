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
#include "velox/common/testutil/OptionalEmpty.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class MapFilterTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename K, typename V>
  void checkMapFilter(
      BaseVector* inputMap,
      const BaseVector& result,
      std::function<bool(K*, V*, vector_size_t, vector_size_t)> test) {
    auto resultMap = result.wrappedVector()->as<MapVector>();
    auto resultKeys = resultMap->mapKeys()->as<K>();
    auto resultValues = resultMap->mapValues()->as<V>();
    auto map = inputMap->wrappedVector()->as<MapVector>();
    auto keys = map->mapKeys()->as<K>();
    auto values = map->mapValues()->as<V>();
    for (auto i = 0; i < inputMap->size(); ++i) {
      bool expectedNull = inputMap->isNullAt(i);
      EXPECT_EQ(expectedNull, result.isNullAt(i));
      if (expectedNull) {
        continue;
      }
      auto mapIndex = inputMap->wrappedIndex(i);
      auto offset = map->offsetAt(mapIndex);
      auto size = map->sizeAt(mapIndex);

      auto resultIndex = result.wrappedIndex(i);
      auto resultOffset = resultMap->offsetAt(resultIndex);
      auto resultSize = resultMap->sizeAt(resultIndex);
      int32_t count = 0;
      for (auto j = offset; j < offset + size; ++j) {
        if (test(keys, values, j, i)) {
          auto resultElementIndex = resultOffset + count;
          ++count;
          EXPECT_LE(count, resultSize)
              << "at " << i << ": " << resultMap->toString(resultIndex);
          EXPECT_TRUE(keys->equalValueAt(resultKeys, j, resultElementIndex))
              << "at (" << i << ", " << j << "): " << keys->toString(j)
              << " vs. " << resultKeys->toString(resultElementIndex);
          EXPECT_TRUE(values->equalValueAt(resultValues, j, resultElementIndex))
              << "at (" << i << ", " << j << "): " << values->toString(j)
              << " vs. " << resultValues->toString(resultElementIndex);
        }
      }
      EXPECT_EQ(resultSize, count)
          << "at " << i << ": " << resultMap->toString(resultIndex);
    }
  }
};

TEST_F(MapFilterTest, filter) {
  {
    auto rowType =
        ROW({"long_val", "map_val"}, {BIGINT(), MAP(BIGINT(), INTEGER())});
    auto data = std::static_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));

    auto result =
        evaluate("map_filter(map_val, (k, v) -> (k > long_val))", data);
    auto* cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
    checkMapFilter<SimpleVector<int64_t>, SimpleVector<int32_t>>(
        data->childAt(1).get(),
        *result,
        [&](SimpleVector<int64_t>* keys,
            SimpleVector<int32_t>* values,
            vector_size_t elementRow,
            vector_size_t row) {
          return cutoff->isNullAt(row) || keys->isNullAt(elementRow)
              ? false
              : keys->valueAt(elementRow) > cutoff->valueAt(row);
        });
  }
  {
    const auto rowType = ROW({"map_val"}, {MAP(BIGINT(), INTEGER())});
    const auto map = makeMapVectorFromJson<int64_t, int32_t>(
        {"{1: \"1\", 2: \"2\", 5: \"5\"}",
         "{3: \"3\", 7: \"7\"}",
         "{4: \"4\", 5: \"5\"}",
         "{6: \"6\", 7: \"7\"}",
         "{1: \"1\", 8: \"8\"}",
         "{4: \"4\", 9: \"9\"}"});
    const auto data = makeRowVector({"map_val"}, {map});
    const auto result = evaluate(
        "map_filter(map_val, (k, v) -> (contains(ARRAY[1,2,7], k))) as map_val",
        data);
    const auto expectedMap = makeMapVectorFromJson<int64_t, int32_t>(
        {"{1: \"1\", 2: \"2\"}",
         "{7: \"7\"}",
         "{}",
         "{7: \"7\"}",
         "{1: \"1\"}",
         "{}"});
    assertEqualVectors(expectedMap, result);
  }
}

TEST_F(MapFilterTest, empty) {
  auto rowType =
      ROW({"long_val", "map_val"}, {BIGINT(), MAP(BIGINT(), INTEGER())});
  auto input = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));

  auto result =
      evaluate<MapVector>("map_filter(map_val, (k, v) -> (k = 11111))", input);

  EXPECT_EQ(result->size(), input->size());
  auto inputMap = input->childAt(1);
  for (auto i = 0; i < input->size(); ++i) {
    bool isNull = inputMap->isNullAt(i);
    EXPECT_EQ(isNull, result->isNullAt(i)) << "at " << i;
    if (!isNull) {
      EXPECT_EQ(result->sizeAt(i), 0)
          << "at " << i << ": " << result->toString(i);
    }
  }
}

TEST_F(MapFilterTest, dictionaryWithUniqueValues) {
  auto rowType =
      ROW({"long_val", "map_val"}, {BIGINT(), MAP(BIGINT(), INTEGER())});
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 10, *execCtx_.pool()));

  // Wrap the input in a dictionary.
  BufferPtr indices = makeIndicesInReverse(data->size());
  data->childAt(1) = wrapInDictionary(indices, data->size(), data->childAt(1));
  auto result = evaluate(
      "map_filter(map_filter(map_val, (k, v) -> (k > long_val)), (k, v) -> (v > 0))",
      data);
  auto* cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
  auto test = [&](SimpleVector<int64_t>* keys,
                  SimpleVector<int32_t>* values,
                  vector_size_t elementRow,
                  vector_size_t row) {
    if (cutoff->isNullAt(row) || keys->isNullAt(elementRow) ||
        values->isNullAt(elementRow)) {
      return false;
    }
    auto cutoffValue = cutoff->valueAt(row);
    return keys->valueAt(elementRow) > cutoffValue &&
        values->valueAt(elementRow) > 0;
  };
  checkMapFilter<SimpleVector<int64_t>, SimpleVector<int32_t>>(
      data->childAt(1).get(), *result, test);

  // Wrap both inputs in the same dictionary.
  data->childAt(0) = wrapInDictionary(indices, data->size(), data->childAt(0));
  result = evaluate(
      "map_filter(map_filter(map_val, (k, v) -> (k > long_val)), (k, v) -> (v > 0))",
      data);
  cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
  checkMapFilter<SimpleVector<int64_t>, SimpleVector<int32_t>>(
      data->childAt(1).get(), *result, test);
}

TEST_F(MapFilterTest, conditional) {
  auto rowType =
      ROW({"long_val", "map_val"}, {BIGINT(), MAP(BIGINT(), INTEGER())});
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));

  auto result = evaluate(
      "map_filter(map_val, "
      "  if (long_val < 0, (k, v) -> (v < long_val), (k, v) -> (k > long_val)))",
      data);

  auto* cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
  auto test = [&](SimpleVector<int64_t>* keys,
                  SimpleVector<int32_t>* values,
                  vector_size_t elementRow,
                  vector_size_t row) {
    if (cutoff->isNullAt(row)) {
      return false;
    }
    auto cutoffValue = cutoff->valueAt(row);
    if (cutoffValue < 0) {
      return keys->isNullAt(elementRow)
          ? false
          : keys->valueAt(elementRow) < cutoffValue;
    } else {
      return values->isNullAt(elementRow)
          ? false
          : values->valueAt(elementRow) > cutoffValue;
    }
  };

  checkMapFilter<SimpleVector<int64_t>, SimpleVector<int32_t>>(
      data->childAt(1).get(), *result, test);
}

TEST_F(MapFilterTest, dictionaryWithDuplicates) {
  vector_size_t size = 1'000;

  // make a map vector where each row repeats a few times
  auto sizeAt = [](vector_size_t row) { return row % 5; };
  auto baseMap = makeMapVector<int32_t, int64_t>(
      size / 2,
      sizeAt,
      [](vector_size_t row) { return row % 7; },
      [](vector_size_t row) { return row % 11; },
      nullEvery(11));

  BufferPtr indices = makeIndices(size, [](auto row) { return row / 2; });

  auto map = wrapInDictionary(indices, size, baseMap);

  // make a capture with unique values
  auto capture =
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });

  auto input = makeRowVector({capture, map});

  auto result =
      evaluate("map_filter(c1, (k, v) -> ((k + v + c0) % 7 < 3))", input);

  auto flatMap = flatten(map);
  input = makeRowVector({capture, flatMap});
  auto expectedResult =
      evaluate("map_filter(c1, (k, v) -> ((k + v + c0) % 7 < 3))", input);

  assertEqualVectors(expectedResult, result);
}

TEST_F(MapFilterTest, lambdaSelectivityVector) {
  auto data = makeRowVector({
      wrapInDictionary(
          makeIndices({0}),
          1,
          makeFlatVector<int64_t>(std::vector<int64_t>{10})),
  });

  // Our expression. Use large numbers to trigger asan if things go wrong.
  auto exprSet = compileExpression(
      "map_filter("
      "MAP(ARRAY[233439836560246536, 398885052601874414, 213334509704047604],"
      "ARRAY[c0, c0, c0]),"
      "(k, v) -> (v IS NOT NULL))",
      asRowType(data->type()));

  // Ensure that our context would have 'final selection' false.
  exec::EvalCtx context(&execCtx_, exprSet.get(), data.get());
  const SelectivityVector allRows(data->size());
  exec::ScopedFinalSelectionSetter scopedFinalSelectionSetter(
      context, &allRows);

  // Evaluate. Result would be overwritten.
  std::vector<VectorPtr> result = {
      makeFlatVector<int64_t>(std::vector<int64_t>{1})};
  exprSet->eval(allRows, context, result);

  auto expectedKeys = makeFlatVector<int64_t>(
      {233439836560246536, 398885052601874414, 213334509704047604});
  auto expectedValues = makeFlatVector<int64_t>({10, 10, 10});
  auto expected = makeMapVector({0}, expectedKeys, expectedValues);
  assertEqualVectors(expected, result[0]);
}

TEST_F(MapFilterTest, fuzzFlatMap) {
  auto options = VectorFuzzer::Options();
  options.allowFlatMapVector = true;
  VectorFuzzer fuzzer(options, execCtx_.pool());
  constexpr vector_size_t size = 100;
  auto num_iterations = 100;
  while (num_iterations--) {
    auto flatMap = fuzzer.fuzzFlatMap(INTEGER(), INTEGER(), size);

    // Convert to a regular map vector.
    auto map = flatMap->as<FlatMapVector>()->toMapVector();

    // Wrap in a row vector for evaluation.
    auto dataFlat = makeRowVector({"c0"}, {flatMap});
    auto dataMap = makeRowVector({"c0"}, {map});

    // Try a few different filters.
    std::vector<std::string> filters = {
        "(k, v) -> (v IS NOT NULL)",
        "(k, v) -> (k % 2 = 0)",
        "(k, v) -> (v > 50)",
        "(k, v) -> (k < 5 OR v IS NULL)",
    };

    for (const auto& filter : filters) {
      auto expr = fmt::format("map_filter(c0, {})", filter);
      auto resultFlat = evaluate(expr, dataFlat);
      auto resultMap = evaluate(expr, dataMap);
      assertEqualVectors(resultMap, resultFlat);
    }
  }
}

TEST_F(MapFilterTest, fromFlatMapEncodings) {
  // Case 1: Verify value filter
  assertEqualVectors(
      makeFlatMapVectorFromJson<int64_t, int32_t>({
          "{1:10, 2:20, 4:40, 5:50, 6:60}",
      }),
      evaluate(
          "map_filter(c0, (k, v) -> (v IS NOT NULL))",
          makeRowVector({
              makeFlatMapVectorFromJson<int64_t, int32_t>({
                  "{1:10, 2:20, 3:null, 4:40, 5:50, 6:60}",
              }),
          })));
  assertEqualVectors(
      makeFlatMapVectorFromJson<int64_t, int32_t>({
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 4:40}",
          "{}",
          "{2:20}",
      }),
      evaluate(
          "map_filter(c0, (k, v) -> (v IS NOT NULL))",
          makeRowVector({
              makeFlatMapVectorFromJson<int64_t, int32_t>({
                  "{1:10, 2:20, 3:null, 4:40, 5:50, 6:null}",
                  "{1:10, 2:null, 4:40, 5:null}",
                  "{}",
                  "{2:20, 4:null, 6:null}",
              }),
          })));
  assertEqualVectors(
      makeFlatMapVectorFromJson<int64_t, int32_t>({
          "{4:40, 5:50}",
          "{4:40}",
          "{}",
          "{}",
      }),
      evaluate(
          "map_filter(c0, (k, v) -> (v > 30))",
          makeRowVector({
              makeFlatMapVectorFromJson<int64_t, int32_t>({
                  "{1:10, 2:20, 3:null, 4:40, 5:50, 6:null}",
                  "{1:10, 2:null, 4:40, 5:null}",
                  "{}",
                  "{2:20, 4:null, 6:null}",
              }),
          })));

  // Case 2: Verify key filter
  assertEqualVectors(
      makeFlatMapVectorFromJson<int64_t, int32_t>({
          "{4:40, 5:50, 6:null}",
          "{4:40, 5:null}",
          "{}",
          "{4:null, 6:null}",
      }),
      evaluate(
          "map_filter(c0, (k, v) -> (k > 3))",
          makeRowVector({
              makeFlatMapVectorFromJson<int64_t, int32_t>({
                  "{1:10, 2:20, 3:null, 4:40, 5:50, 6:null}",
                  "{1:10, 2:null, 4:40, 5:null}",
                  "{}",
                  "{2:20, 4:null, 6:null}",
              }),
          })));

  // Case 3: Verify key and value filter
  assertEqualVectors(
      makeFlatMapVectorFromJson<int64_t, int32_t>({
          "{4:40, 5:50}",
          "{4:40}",
          "{}",
          "{}",
      }),
      evaluate(
          "map_filter(c0, (k, v) -> (k > 3 AND v is NOT NULL))",
          makeRowVector({
              makeFlatMapVectorFromJson<int64_t, int32_t>({
                  "{1:10, 2:20, 3:null, 4:40, 5:50, 6:null}",
                  "{1:10, 2:null, 4:40, 5:null}",
                  "{}",
                  "{2:20, 4:null, 6:null}",
              }),
          })));
  assertEqualVectors(
      makeFlatMapVectorFromJson<int64_t, int32_t>({
          "{}",
          "{}",
          "{}",
          "{}",
      }),
      evaluate(
          "map_filter(c0, (k, v) -> (k > 3 AND v < 30))",
          makeRowVector({
              makeFlatMapVectorFromJson<int64_t, int32_t>({
                  "{1:10, 2:20, 3:null, 4:40, 5:50, 6:null}",
                  "{1:10, 2:null, 4:40, 5:null}",
                  "{}",
                  "{2:20, 4:null, 6:null}",
              }),
          })));
}

TEST_F(MapFilterTest, fromFlatMapEncodingsWrappedInDictionary) {
  auto input = makeFlatMapVectorFromJson<int64_t, int32_t>({
      "{1:10, 2:20, 3:null, 4:40, 5:50}",
      "{1:10, 2:20, 3:null, 4:40, 5:50}",
      "{1:10, 2:null, 4:40, 5:null}",
      "{}",
      "{2:20, 4:null, 5:null}",
      "{2:20, 4:null, 5:50}",
      "{1:10, 4:null, 5:null}",
      "{2:20, 3:30, 5:null}",
      "{}",
      "{1:10, 2:20, 3:30, 4:40, 5:50}",
  });
  auto expected = makeFlatMapVectorFromJson<int64_t, int32_t>({
      "{1:10, 2:20, 4:40, 5:50}",
      "{1:10, 2:20, 4:40, 5:50}",
      "{1:10, 4:40}",
      "{}",
      "{2:20}",
      "{2:20, 5:50}",
      "{1:10}",
      "{2:20, 3:30}",
      "{}",
      "{1:10, 2:20, 3:30, 4:40, 5:50}",
  });

  auto same = [](vector_size_t row) { return row; };
  auto scattered = [](auto row) { return (row * 17 + 3) % 10; };

  // Flattening handled by preprocessing peeling logic
  assertEqualVectors(
      expected,
      evaluate(
          "map_filter(c0, (k, v) -> (v IS NOT NULL))",
          makeRowVector({BaseVector::wrapInDictionary(
              nullptr,
              makeIndices(input->size(), same),
              input->size(),
              input)})));

  // Flattened internally by our filter function. This will test our ability to
  // filter wrapped flat maps and preserve their indices. We will use our
  // scattered function above to produce shuffled indices. The second assert
  // uses the same function but half the indices to verify our logic on length
  // mismatched dictionary indices
  assertEqualVectors(
      wrapInDictionary(
          makeIndices(expected->size(), scattered), expected->size(), expected),
      evaluate(
          "map_filter(c1, (k, v) -> (v IS NOT NULL and c0 is not null))",
          makeRowVector(
              {makeFlatVector<int32_t>(input->size(), same),
               wrapInDictionary(
                   makeIndices(input->size(), scattered),
                   input->size(),
                   input)})));
  assertEqualVectors(
      wrapInDictionary(
          makeIndices(expected->size() / 2, scattered),
          expected->size() / 2,
          expected),
      evaluate(
          "map_filter(c1, (k, v) -> (v IS NOT NULL and c0 is not null))",
          makeRowVector(
              {makeFlatVector<int32_t>(input->size() / 2, same),
               wrapInDictionary(
                   makeIndices(input->size() / 2, scattered),
                   input->size() / 2,
                   input)})));
  assertEqualVectors(
      wrapInDictionary(
          makeIndices(expected->size() / 2, scattered),
          expected->size() / 2,
          makeFlatMapVectorFromJson<int64_t, int32_t>({
              "{4:40, 5:50}",
              "{4:40, 5:50}",
              "{4:40}",
              "{}",
              "{}",
              "{5:50}",
              "{}",
              "{}",
              "{}",
              "{4:40, 5:50}",
          })),
      evaluate(
          "map_filter(c1, (k, v) -> (v IS NOT NULL and v > 30 and c0 is not null))",
          makeRowVector(
              {makeFlatVector<int32_t>(input->size() / 2, same),
               wrapInDictionary(
                   makeIndices(input->size() / 2, scattered),
                   input->size() / 2,
                   input)})));
  assertEqualVectors(
      wrapInDictionary(
          makeIndices(expected->size() / 2, scattered),
          expected->size() / 2,
          makeFlatMapVectorFromJson<int64_t, int32_t>({
              "{5:50}",
              "{5:50}",
              "{}",
              "{}",
              "{}",
              "{5:50}",
              "{}",
              "{}",
              "{}",
              "{5:50}",
          })),
      evaluate(
          "map_filter(c1, (k, v) -> (v IS NOT NULL and v > 30 and k > 4 and c0 is not null))",
          makeRowVector(
              {makeFlatVector<int32_t>(input->size() / 2, same),
               wrapInDictionary(
                   makeIndices(input->size() / 2, scattered),
                   input->size() / 2,
                   input)})));
}

TEST_F(MapFilterTest, fromFlatMapEncodingsWrappedInConstant) {
  assertEqualVectors(
      makeFlatMapVectorFromJson<int64_t, int32_t>({
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{1:10, 2:20, 4:40, 5:50}",
      }),
      evaluate(
          "map_filter(c0, (k, v) -> (v IS NOT NULL))",
          makeRowVector({BaseVector::wrapInConstant(
              10,
              0,
              makeFlatMapVectorFromJson<int64_t, int32_t>({
                  "{1:10, 2:20, 3:null, 4:40, 5:50}",
                  "{1:10, 2:20, 3:null, 4:40, 5:50}",
                  "{1:10, 2:null, 4:40, 5:null}",
                  "{}",
                  "{2:20, 4:null, 5:null}",
                  "{2:20, 4:null, 5:50}",
                  "{1:10, 4:null, 5:null}",
                  "{2:20, 3:30, 5:null}",
                  "{}",
                  "{1:10, 2:20, 3:30, 4:40, 5:50}",
              }))})));
}

TEST_F(MapFilterTest, fromFlatMapEncodingsWithNullInMaps) {
  auto distinctKeys = makeFlatVector<int32_t>({1, 2, 3});
  std::vector<VectorPtr> mapValues(distinctKeys->size());
  mapValues[0] = makeFlatVector<int32_t>({10, 10, 10});
  mapValues[1] = makeFlatVector<int32_t>({20, 20, 20});
  mapValues[2] = makeFlatVector<int32_t>({30, 30, 30});
  std::vector<BufferPtr> inMaps(distinctKeys->size());
  inMaps[1] =
      AlignedBuffer::allocate<bool>(distinctKeys->size(), pool_.get(), 0);
  FlatMapVectorPtr flatMap = std::make_shared<FlatMapVector>(
      pool_.get(),
      MAP(INTEGER(), INTEGER()),
      nullptr,
      distinctKeys->size(),
      distinctKeys,
      mapValues,
      inMaps);

  // Will fail without inMap null check during
  // MapFilterFunction::buildInMapSelectivityVector
  assertEqualVectors(
      makeFlatMapVectorFromJson<int32_t, int32_t>({
          "{1:10, 3:30}",
          "{1:10, 3:30}",
          "{1:10, 3:30}",
      }),
      evaluate("map_filter(c0, (k, v) -> true)", makeRowVector({flatMap})));
}

TEST_F(MapFilterTest, try) {
  auto data = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{1, 2}, {2, 3}},
          {{3, 4}, {0, 1}, {5, 6}},
          {{6, 5}, {7, 8}},
          {{8, 7}},
      }),
  });

  VELOX_ASSERT_THROW(
      evaluate("map_filter(c0, (k, v) -> (v / k > 0))", data),
      "division by zero");

  auto result = evaluate("try(map_filter(c0, (k, v) -> (v / k > 0)))", data);
  auto expected = makeNullableMapVector<int64_t, int64_t>(
      {{{{1, 2}, {2, 3}}},
       std::nullopt,
       {{{7, 8}}},
       common::testutil::optionalEmpty});
  assertEqualVectors(expected, result);
}

TEST_F(MapFilterTest, unknown) {
  auto data = makeRowVector({makeAllNullMapVector(10, UNKNOWN(), BIGINT())});
  auto result = evaluate("map_filter(c0, (k, v) -> (v > 5))", data);
  assertEqualVectors(data->childAt(0), result);
}

TEST_F(MapFilterTest, selectiveFilter) {
  // Verify that a selective filter will ensure the underlying elements
  // vector is flattened before generating the result which is otherwise wrapped
  // in a dictionary with the filter results. This ensures large element
  // vectors are not passed along.
  auto data = makeRowVector({
      makeMapVector<int64_t, int64_t>(
          {{{1, 3},
            {2, 3},
            {3, 3},
            {4, 3},
            {5, 3},
            {6, 3},
            {7, 3},
            {8, 3},
            {9, 3},
            {10, 3},
            {11, 3},
            {12, 3},
            {13, 3},
            {14, 3},
            {15, 3},
            {16, 3}}}),
  });

  auto result = evaluate("map_filter(c0, (k, v) -> (k = 1))", data);
  auto base = result->as<MapVector>()->mapKeys();
  EXPECT_EQ(base->encoding(), VectorEncoding::Simple::FLAT);
  base = result->as<MapVector>()->mapValues();
  EXPECT_EQ(base->encoding(), VectorEncoding::Simple::FLAT);

  result = evaluate("map_filter(c0, (k, v) -> (k < 6))", data);
  base = result->as<MapVector>()->mapKeys();
  EXPECT_EQ(base->encoding(), VectorEncoding::Simple::DICTIONARY);
  base = result->as<MapVector>()->mapValues();
  EXPECT_EQ(base->encoding(), VectorEncoding::Simple::DICTIONARY);
}
