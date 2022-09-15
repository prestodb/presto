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

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class MapKeysAndValuesTest : public FunctionBaseTest {
 protected:
  void checkResult(
      MapVectorPtr mapVector,
      VectorPtr mapKeys,
      vector_size_t row,
      ArrayVectorPtr result,
      VectorPtr resultElements) {
    auto size = mapVector->sizeAt(row);
    EXPECT_EQ(size, result->sizeAt(row)) << "at " << row;
    for (vector_size_t j = 0; j < size; j++) {
      EXPECT_TRUE(mapKeys->equalValueAt(
          resultElements->wrappedVector(),
          mapVector->offsetAt(row) + j,
          result->offsetAt(row) + j));
    }
  }

  void testMap(
      const std::string& expression,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt,
      std::function<VectorPtr(MapVectorPtr /*mapVector*/)> expectedFunc) {
    auto mapVector = makeMapVector<int32_t, int64_t>(
        numRows_, sizeAt, keyAt, valueAt, isNullAt);

    auto result = evaluate<ArrayVector>(expression, makeRowVector({mapVector}));
    ASSERT_EQ(result->typeKind(), TypeKind::ARRAY);

    auto expected = expectedFunc(mapVector);

    auto resultElements = result->elements();
    ASSERT_TRUE(resultElements->type()->kindEquals(expected->type()));

    EXPECT_EQ(numRows_, result->size());
    for (vector_size_t i = 0; i < numRows_; ++i) {
      EXPECT_EQ(result->isNullAt(i), mapVector->isNullAt(i)) << "at " << i;
      if (!mapVector->isNullAt(i)) {
        checkResult(mapVector, expected, i, result, resultElements);
      }
    }
  }

  void testConstantMap(
      const std::string& expression,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt,
      std::function<VectorPtr(MapVectorPtr /*mapVector*/)> expectedFunc) {
    auto mapVector = makeMapVector<int32_t, int64_t>(
        numRows_, sizeAt, keyAt, valueAt, isNullAt);

    auto result = evaluate<ConstantVector<ComplexType>>(
        expression, makeRowVector({mapVector}));
    ASSERT_EQ(result->typeKind(), TypeKind::ARRAY);

    auto expected = expectedFunc(mapVector);
    if (result->isNullAt(0)) {
      EXPECT_EQ(0, expected->size());
      return;
    }
    auto array = std::dynamic_pointer_cast<ArrayVector>(
        result->as<ConstantVector<ComplexType>>()->valueVector());
    auto elements = array->elements();
    ASSERT_TRUE(elements->type()->kindEquals(expected->type()));

    EXPECT_EQ(numRows_, result->size());
    for (vector_size_t i = 0; i < elements->size(); ++i) {
      EXPECT_EQ(result->isNullAt(i), mapVector->isNullAt(i)) << "at " << i;
      if (!mapVector->isNullAt(i)) {
        checkResult(mapVector, expected, i, array, elements);
      }
    }
  }

  void testMapPartiallyPopulated(
      const std::string& expression,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt,
      std::function<VectorPtr(MapVectorPtr /*mapVector*/)> expectedFunc) {
    auto a = makeMapVector<int32_t, int64_t>(
        numRows_, sizeAt, keyAt, valueAt, isNullAt);
    auto b = makeMapVector<int32_t, int64_t>(
        numRows_, sizeAt, valueAt, keyAt, isNullAt);
    auto c =
        makeFlatVector<int32_t>(numRows_, [](vector_size_t i) { return i; });

    auto result = evaluate<ArrayVector>(expression, makeRowVector({c, a, b}));
    ASSERT_EQ(result->typeKind(), TypeKind::ARRAY);

    auto aExpected = expectedFunc(a);
    auto bExpected = expectedFunc(b);

    auto resultElements = result->elements();
    ASSERT_TRUE(result->type()->childAt(0)->kindEquals(aExpected->type()));

    EXPECT_EQ(numRows_, result->size());
    for (vector_size_t i = 0; i < numRows_; ++i) {
      if (i % 2 == 0) {
        EXPECT_EQ(result->isNullAt(i), a->isNullAt(i)) << "at " << i;
        if (!result->isNullAt(i)) {
          checkResult(a, aExpected, i, result, resultElements);
        }
      } else {
        EXPECT_EQ(result->isNullAt(i), b->isNullAt(i)) << "at " << i;
        if (!result->isNullAt(i)) {
          checkResult(b, bExpected, i, result, resultElements);
        }
      }
    }
  }

  static inline int32_t keyAt(vector_size_t idx) {
    return idx;
  }

  static inline int64_t valueAt(vector_size_t idx) {
    return (idx + 1) * 11;
  }

 private:
  const vector_size_t numRows_ = 100;
};

class MapKeysTest : public MapKeysAndValuesTest {
 protected:
  void testMapKeys(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    testMap("map_keys(C0)", sizeAt, isNullAt, mapKeys);
  }

  void testConstantMapKeys(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    testConstantMap("map_keys(C0)", sizeAt, isNullAt, mapKeys);
  }

  void testMapKeysPartiallyPopulated(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    testMapPartiallyPopulated(
        "if(C0 % 2 = 0, map_keys(C1), map_keys(C2))",
        sizeAt,
        isNullAt,
        mapKeys);
  }

 private:
  static VectorPtr mapKeys(MapVectorPtr mapVector) {
    return mapVector->mapKeys();
  }
};

class MapValuesTest : public MapKeysAndValuesTest {
 protected:
  void testMapValues(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    testMap("map_values(C0)", sizeAt, isNullAt, mapValues);
  }

  void testConstantMapValues(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    testConstantMap("map_values(C0)", sizeAt, isNullAt, mapValues);
  }

  void testMapValuesPartiallyPopulated(
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt) {
    testMapPartiallyPopulated(
        "if(C0 % 2 = 0, map_values(C1), map_values(C2))",
        sizeAt,
        isNullAt,
        mapValues);
  }

 private:
  static VectorPtr mapValues(MapVectorPtr mapVector) {
    return mapVector->mapValues();
  }
};

} // namespace

TEST_F(MapKeysTest, noNulls) {
  auto sizeAt = [](vector_size_t row) { return row % 7; };
  testMapKeys(sizeAt, nullptr);
}

TEST_F(MapKeysTest, someNulls) {
  auto sizeAt = [](vector_size_t row) { return row % 7; };
  testMapKeys(sizeAt, nullEvery(5));
}

TEST_F(MapKeysTest, allNulls) {
  auto sizeAt = [](vector_size_t row) { return row % 7; };
  testConstantMapKeys(sizeAt, nullEvery(1));
}

TEST_F(MapKeysTest, partiallyPopulatedNoNulls) {
  auto sizeAt = [](vector_size_t /* row */) { return 1; };
  testMapKeysPartiallyPopulated(sizeAt, nullptr);
}

TEST_F(MapKeysTest, partiallyPopulatedSomeNulls) {
  auto sizeAt = [](vector_size_t /* row */) { return 1; };
  testMapKeysPartiallyPopulated(sizeAt, nullEvery(5));
}

TEST_F(MapKeysTest, constant) {
  vector_size_t size = 1'000;
  auto data = makeMapVector<int64_t, int64_t>({
      {
          {0, 0},
          {1, 10},
          {2, 20},
          {3, 30},
      },
      {
          {4, 40},
          {5, 50},
      },
      {
          {6, 60},
      },
  });

  auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "map_keys(c0)",
        makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
  };

  auto result = evaluateConstant(0, data);
  auto expected = makeConstantArray<int64_t>(size, {0, 1, 2, 3});
  test::assertEqualVectors(expected, result);

  result = evaluateConstant(1, data);
  expected = makeConstantArray<int64_t>(size, {4, 5});
  test::assertEqualVectors(expected, result);

  result = evaluateConstant(2, data);
  expected = makeConstantArray<int64_t>(size, {6});
  test::assertEqualVectors(expected, result);
}

TEST_F(MapValuesTest, noNulls) {
  auto sizeAt = [](vector_size_t row) { return row % 7; };
  testMapValues(sizeAt, nullptr);
}

TEST_F(MapValuesTest, someNulls) {
  auto sizeAt = [](vector_size_t row) { return row % 7; };
  testMapValues(sizeAt, nullEvery(5));
}

TEST_F(MapValuesTest, allNulls) {
  auto sizeAt = [](vector_size_t row) { return row % 7; };
  testConstantMapValues(sizeAt, nullEvery(1));
}

TEST_F(MapValuesTest, partiallyPopulatedNoNulls) {
  auto sizeAt = [](vector_size_t /* row */) { return 1; };
  testMapValuesPartiallyPopulated(sizeAt, nullptr);
}

TEST_F(MapValuesTest, partiallyPopulatedSomeNulls) {
  auto sizeAt = [](vector_size_t /* row */) { return 1; };
  testMapValuesPartiallyPopulated(sizeAt, nullEvery(5));
}

TEST_F(MapValuesTest, constant) {
  vector_size_t size = 1'000;
  auto data = makeMapVector<int64_t, int64_t>({
      {
          {0, 0},
          {1, 10},
          {2, 20},
          {3, 30},
      },
      {
          {4, 40},
          {5, 50},
      },
      {
          {6, 60},
      },
  });

  auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "map_values(c0)",
        makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
  };

  auto result = evaluateConstant(0, data);
  auto expected = makeConstantArray<int64_t>(size, {0, 10, 20, 30});
  test::assertEqualVectors(expected, result);

  result = evaluateConstant(1, data);
  expected = makeConstantArray<int64_t>(size, {40, 50});
  test::assertEqualVectors(expected, result);

  result = evaluateConstant(2, data);
  expected = makeConstantArray<int64_t>(size, {60});
  test::assertEqualVectors(expected, result);
}
