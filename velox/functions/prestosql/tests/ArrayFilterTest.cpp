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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class ArrayFilterTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  void checkArrayFilter(
      BaseVector* inputArray,
      const BaseVector& result,
      std::function<bool(T*, vector_size_t, vector_size_t)> test) {
    auto resultArray = result.wrappedVector()->as<ArrayVector>();
    auto resultElements = resultArray->elements()->as<SimpleVector<int64_t>>();
    auto array = inputArray->wrappedVector()->as<ArrayVector>();
    auto elements = array->elements()->as<T>();
    for (auto i = 0; i < inputArray->size(); ++i) {
      bool expectedNull = inputArray->isNullAt(i);
      EXPECT_EQ(expectedNull, result.isNullAt(i));
      if (expectedNull) {
        continue;
      }
      auto arrayIndex = inputArray->wrappedIndex(i);

      auto start = array->offsetAt(arrayIndex);
      auto size = array->sizeAt(arrayIndex);
      auto resultStart = resultArray->offsetAt(result.wrappedIndex(i));
      vector_size_t resultCount = 0;
      for (auto j = start; j < start + size; ++j) {
        if (test(elements, j, i)) {
          ASSERT_TRUE(elements->equalValueAt(
              resultElements, j, resultStart + resultCount))
              << "at " << i << ", " << j << ": " << elements->toString(j)
              << " vs. " << resultElements->toString(resultStart + resultCount);
          ++resultCount;
        }
      }
      ASSERT_EQ(resultArray->sizeAt(result.wrappedIndex(i)), resultCount);
    }
  }
};

TEST_F(ArrayFilterTest, filter) {
  auto rowType = ROW({"long_val", "array_val"}, {BIGINT(), ARRAY(BIGINT())});
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));
  registerLambda("lambda1", ROW({"x"}, {BIGINT()}), rowType, "x > long_val");
  auto result =
      evaluate<BaseVector>("filter(array_val, function('lambda1'))", data);
  auto* cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
  checkArrayFilter<SimpleVector<int64_t>>(
      data->childAt(1).get(),
      *result,
      [&](SimpleVector<int64_t>* elements,
          vector_size_t elementRow,
          vector_size_t row) {
        return cutoff->isNullAt(row) || elements->isNullAt(elementRow)
            ? false
            : elements->valueAt(elementRow) > cutoff->valueAt(row);
      });
}

TEST_F(ArrayFilterTest, empty) {
  auto rowType = ROW({"long_val", "array_val"}, {BIGINT(), ARRAY(BIGINT())});
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));
  registerLambda("eq_1111", ROW({"x"}, {BIGINT()}), rowType, "x = 1111");

  auto result =
      evaluate<ArrayVector>("filter(array_val, function('eq_1111'))", data);

  EXPECT_EQ(result->size(), data->size());
  auto inputArray = data->childAt(1);
  for (auto i = 0; i < data->size(); ++i) {
    bool isNull = inputArray->isNullAt(i);
    EXPECT_EQ(isNull, result->isNullAt(i)) << "at " << i;
    if (!isNull) {
      EXPECT_EQ(result->sizeAt(i), 0) << "at " << i;
    }
  }
}

TEST_F(ArrayFilterTest, dictionaryWithUniqueValues) {
  auto rowType = ROW({"long_val", "array_val"}, {BIGINT(), ARRAY(BIGINT())});
  registerLambda("lambda1", ROW({"x"}, {BIGINT()}), rowType, "x > long_val");
  registerLambda("lambda2", ROW({"x"}, {BIGINT()}), rowType, "x > 0");
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));

  // Wrap the input in a dictionary.
  BufferPtr indices = makeIndicesInReverse(data->size());
  data->childAt(1) = wrapInDictionary(indices, data->size(), data->childAt(1));
  auto result = evaluate<BaseVector>(
      "filter(filter(array_val, function('lambda2')), function('lambda1'))",
      data);
  auto* cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
  checkArrayFilter<SimpleVector<int64_t>>(
      data->childAt(1).get(),
      *result,
      [&](SimpleVector<int64_t>* elements,
          vector_size_t elementRow,
          vector_size_t row) {
        return cutoff->isNullAt(row) || elements->isNullAt(elementRow)
            ? false
            : elements->valueAt(elementRow) >
                std::max<int64_t>(0L, cutoff->valueAt(row));
      });

  // Wrap both inputs in the same dictionary.
  data->childAt(0) = wrapInDictionary(indices, data->size(), data->childAt(0));
  result = evaluate<BaseVector>(
      "filter(filter(array_val, function('lambda2')), function('lambda1'))",
      data);
  cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
  checkArrayFilter<SimpleVector<int64_t>>(
      data->childAt(1).get(),
      *result,
      [&](SimpleVector<int64_t>* elements,
          vector_size_t elementRow,
          vector_size_t row) {
        return cutoff->isNullAt(row) || elements->isNullAt(elementRow)
            ? false
            : elements->valueAt(elementRow) >
                std::max<int64_t>(0L, cutoff->valueAt(row));
      });
}

TEST_F(ArrayFilterTest, conditional) {
  auto rowType = ROW({"long_val", "array_val"}, {BIGINT(), ARRAY(BIGINT())});
  auto data = std::static_pointer_cast<RowVector>(
      BatchMaker::createBatch(rowType, 1'000, *execCtx_.pool()));
  registerLambda("gtCutoff", ROW({"x"}, {BIGINT()}), rowType, "x > long_val");
  registerLambda("ltCutoff", ROW({"x"}, {BIGINT()}), rowType, "x < long_val");

  auto result = evaluate<BaseVector>(
      "filter(array_val, "
      "  if (long_val < 0, function('ltCutoff'), function('gtCutoff')))",
      data);

  auto* cutoff = data->childAt(0)->as<SimpleVector<int64_t>>();
  // Function that selects values whose absolute value > absolute
  // value of cutoff.
  auto isAbsGreater = [&](SimpleVector<int64_t>* elements,
                          vector_size_t elementRow,
                          vector_size_t row) {
    if (cutoff->isNullAt(row) || elements->isNullAt(elementRow)) {
      return false;
    }
    auto cutoffValue = cutoff->valueAt(row);
    return cutoffValue < 0 ? elements->valueAt(elementRow) < cutoffValue
                           : elements->valueAt(elementRow) > cutoffValue;
  };

  checkArrayFilter<SimpleVector<int64_t>>(
      data->childAt(1).get(), *result, isAbsGreater);

  result = evaluate<BaseVector>(
      "if (long_val < 0,"
      "  filter(array_val, function('ltCutoff')), "
      "  filter(array_val, function('gtCutoff')))",
      data);

  checkArrayFilter<SimpleVector<int64_t>>(
      data->childAt(1).get(), *result, isAbsGreater);
}

TEST_F(ArrayFilterTest, dictionaryWithDuplicates) {
  vector_size_t size = 1'000;

  // make an array vector where each row repeats a few times
  auto sizeAt = [](vector_size_t row) { return row % 5; };
  auto baseArray = makeArrayVector<int32_t>(
      size / 2,
      sizeAt,
      [](vector_size_t row) { return row % 7; },
      nullEvery(11));

  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; ++i) {
    rawIndices[i] = i / 2;
  }

  auto array = wrapInDictionary(indices, size, baseArray);

  // make a capture with unique values
  auto capture =
      makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });

  auto input = makeRowVector({capture, array});

  auto signature = ROW({"x"}, {baseArray->elements()->type()});
  registerLambda("filter", signature, input->type(), "(x + c0) % 7 < 3");

  auto result = evaluate<BaseVector>("filter(c1, function('filter'))", input);

  auto flatArray = flatten(array);
  input = makeRowVector({capture, flatArray});
  auto expectedResult =
      evaluate<BaseVector>("filter(c1, function('filter'))", input);

  assertEqualVectors(expectedResult, result);
}
