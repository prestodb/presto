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

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/expression/VarSetter.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class MapFilterTest : public functions::test::FunctionBaseTest {
 protected:
  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(std::move(expressions), &execCtx_);
  }

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
  auto rowType =
      ROW({"long_val", "map_val"}, {BIGINT(), MAP(BIGINT(), INTEGER())});
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));
  auto signature = ROW({"k", "v"}, {BIGINT(), INTEGER()});
  registerLambda("filter", signature, rowType, "k > long_val");
  auto result =
      evaluate<BaseVector>("map_filter(map_val, function('filter'))", data);
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

TEST_F(MapFilterTest, empty) {
  auto rowType =
      ROW({"long_val", "map_val"}, {BIGINT(), MAP(BIGINT(), INTEGER())});
  auto input = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));
  registerLambda(
      "filter", ROW({"k", "v"}, {BIGINT(), INTEGER()}), rowType, "k = 11111");

  auto result =
      evaluate<MapVector>("map_filter(map_val, function('filter'))", input);

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
  auto signature = ROW({"k", "v"}, {BIGINT(), INTEGER()});
  registerLambda("key_filter", signature, rowType, "k > long_val");
  registerLambda("value_filter", signature, rowType, "v > 0");
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 10, *execCtx_.pool()));

  // Wrap the input in a dictionary.
  BufferPtr indices = makeIndicesInReverse(data->size());
  data->childAt(1) = wrapInDictionary(indices, data->size(), data->childAt(1));
  auto result = evaluate<BaseVector>(
      "map_filter(map_filter(map_val, function('key_filter')), function('value_filter'))",
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
  result = evaluate<BaseVector>(
      "map_filter(map_filter(map_val, function('key_filter')), function('value_filter'))",
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
  auto signature = ROW({"k", "v"}, {BIGINT(), INTEGER()});
  registerLambda("gtCutoff", signature, rowType, "k > long_val");
  registerLambda("ltCutoff", signature, rowType, "v < long_val");

  auto result = evaluate<BaseVector>(
      "map_filter(map_val, "
      "  if (long_val < 0, function('ltCutoff'), function('gtCutoff')))",
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

  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    rawIndices[i] = i / 2;
  }

  auto map = wrapInDictionary(indices, size, baseMap);

  // make a capture with unique values
  auto capture =
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });

  auto input = makeRowVector({capture, map});

  auto signature = ROW(
      {"k", "v"}, {baseMap->mapKeys()->type(), baseMap->mapValues()->type()});
  registerLambda("filter", signature, input->type(), "(k + v + c0) % 7 < 3");

  auto result =
      evaluate<BaseVector>("map_filter(c1, function('filter'))", input);

  auto flatMap = flatten(map);
  input = makeRowVector({capture, flatMap});
  auto expectedResult =
      evaluate<BaseVector>("map_filter(c1, function('filter'))", input);

  assertEqualVectors(expectedResult, result);
}

TEST_F(MapFilterTest, lambdaSelectivityVector) {
  auto data = makeRowVector({
      wrapInDictionary(
          makeIndices({0}),
          1,
          makeFlatVector<int64_t>(std::vector<int64_t>{10})),
  });

  // Register lambda as DuckDb parser does not support converting it into Velox
  // lambda expression.
  parse::ParseOptions options;
  core::Expressions::registerLambda(
      "inn",
      ROW({"k", "v"}, {BIGINT(), BIGINT()}),
      ROW({"long_val", "map_val"}, {BIGINT(), MAP(BIGINT(), BIGINT())}),
      parse::parseExpr("v IS NOT NULL", options),
      pool_.get());

  // Our expression. Use large numbers to trigger asan if things go wrong.
  auto exprSet = compileExpression(
      "map_filter("
      "MAP(ARRAY[233439836560246536, 398885052601874414, 213334509704047604],"
      "ARRAY[c0, c0, c0]),"
      "function('inn'))",
      asRowType(data->type()));

  // Ensure that our context would have 'final selection' false.
  exec::EvalCtx context(&execCtx_, exprSet.get(), data.get());
  const SelectivityVector allRows(data->size());
  VarSetter finalSelection(context.mutableFinalSelection(), &allRows);
  VarSetter isFinalSelection(context.mutableIsFinalSelection(), false);

  // Evaluate. Result would be overwritten.
  std::vector<VectorPtr> result = {
      makeFlatVector<int64_t>(std::vector<int64_t>{1})};
  exprSet->eval(allRows, &context, &result);

  auto expectedKeys = makeFlatVector<int64_t>(
      {233439836560246536, 398885052601874414, 213334509704047604});
  auto expectedValues = makeFlatVector<int64_t>({10, 10, 10});
  auto expected = makeMapVector({0}, expectedKeys, expectedValues);
  assertEqualVectors(expected, result[0]);
}
