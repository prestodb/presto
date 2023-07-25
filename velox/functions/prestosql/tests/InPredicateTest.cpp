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
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

class InPredicateTest : public FunctionBaseTest {
 protected:
  template <typename T>
  void testIntegers() {
    std::shared_ptr<memory::MemoryPool> pool{
        memory::addDefaultLeafMemoryPool()};

    const vector_size_t size = 1'000;
    auto vector = makeFlatVector<T>(size, [](auto row) { return row % 17; });
    auto vectorWithNulls = makeFlatVector<T>(
        size, [](auto row) { return row % 17; }, nullEvery(7));
    auto rowVector = makeRowVector({vector, vectorWithNulls});

    // no nulls
    auto result = evaluate<SimpleVector<bool>>("c0 IN (1, 3, 5)", rowVector);
    auto expected = makeFlatVector<bool>(size, [](auto row) {
      auto n = row % 17;
      return n == 1 || n == 3 || n == 5;
    });

    assertEqualVectors(expected, result);

    // some nulls
    result = evaluate<SimpleVector<bool>>("c1 IN (1, 3, 5)", rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto row) {
          auto n = row % 17;
          return n == 1 || n == 3 || n == 5;
        },
        nullEvery(7));

    assertEqualVectors(expected, result);

    // null values in the in-list
    // The results can be either true or null, but not false.
    result = evaluate<SimpleVector<bool>>("c0 IN (1, 3, null, 5)", rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto /* row */) { return true; },
        [](auto row) {
          auto n = row % 17;
          return !(n == 1 || n == 3 || n == 5);
        });

    assertEqualVectors(expected, result);

    result = evaluate<SimpleVector<bool>>("c1 IN (1, 3, null, 5)", rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto /* row */) { return true; },
        [](auto row) {
          auto n = row % 17;
          return row % 7 == 0 || !(n == 1 || n == 3 || n == 5);
        });

    assertEqualVectors(expected, result);

    result = evaluate<SimpleVector<bool>>("c0 IN (2, null)", rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto /* row */) { return true; },
        [](auto row) {
          auto n = row % 17;
          return !(n == 2);
        });

    assertEqualVectors(expected, result);

    result = evaluate<SimpleVector<bool>>("c1 IN (2, null)", rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto /* row */) { return true; },
        [](auto row) {
          auto n = row % 17;
          return row % 7 == 0 || !(n == 2);
        });

    assertEqualVectors(expected, result);

    // Test on a dictionary with nulls.
    // Two dictionary elements would be nulls.
    BufferPtr nulls =
        AlignedBuffer::allocate<bool>(size, pool.get(), bits::kNotNull);
    uint64_t* rawNulls = nulls->asMutable<uint64_t>();
    bits::setNull(rawNulls, 20);
    bits::setNull(rawNulls, size - 1 - 2);

    // Build reverse indices.
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(size, pool.get());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; ++i) {
      rawIndices[i] = size - i - 1;
    }

    // Build the expected vector with a couple of nulls.
    expected = makeFlatVector<bool>(
        size,
        [&](auto row) {
          const auto inverseRow = size - row - 1;
          auto n = inverseRow % 17;
          return n == 2 or n == 5 or n == 9;
        },
        [&](vector_size_t row) {
          return (row == 20) or (row == (size - 1 - 2));
        });

    auto dict = BaseVector::wrapInDictionary(nulls, indices, size, vector);

    rowVector = makeRowVector({dict});
    result = evaluate<SimpleVector<bool>>("c0 IN (2, 5, 9)", rowVector);
    assertEqualVectors(expected, result);

    // an in list with nulls only is always null.
    result = evaluate<SimpleVector<bool>>("c0 IN (null)", rowVector);
    auto expectedConstant =
        BaseVector::createNullConstant(BOOLEAN(), size, pool_.get());
    assertEqualVectors(expectedConstant, result);
  }

  template <typename T>
  void testsIntegerConstant() {
    const vector_size_t size = 1'000;
    auto rowVector = makeRowVector(
        {makeConstant((T)123, size),
         makeNullConstant(CppToType<T>::create()->kind(), size)});

    auto constTrue = makeConstant(true, size);
    auto constFalse = makeConstant(false, size);
    auto constNull = makeNullConstant(TypeKind::BOOLEAN, size);

    // a miss
    auto result = evaluate<SimpleVector<bool>>("c0 IN (1, 3, 5)", rowVector);
    assertEqualVectors(constFalse, result);

    // a hit
    result = evaluate<SimpleVector<bool>>("c0 IN (1, 123, 5)", rowVector);
    assertEqualVectors(constTrue, result);

    // a miss that is a null
    result = evaluate<SimpleVector<bool>>("c0 IN (1, null, 5)", rowVector);
    assertEqualVectors(constNull, result);

    // null
    result = evaluate<SimpleVector<bool>>("c1 IN (1, 3, 5)", rowVector);
    assertEqualVectors(constNull, result);
  }
};

TEST_F(InPredicateTest, bigint) {
  testIntegers<int64_t>();
  testsIntegerConstant<int64_t>();
}

TEST_F(InPredicateTest, integer) {
  testIntegers<int32_t>();
  testsIntegerConstant<int32_t>();
}

TEST_F(InPredicateTest, smallint) {
  testIntegers<int16_t>();
  testsIntegerConstant<int16_t>();
}

TEST_F(InPredicateTest, tinyint) {
  testIntegers<int8_t>();
  testsIntegerConstant<int8_t>();
}

TEST_F(InPredicateTest, boolean) {
  auto input = makeRowVector({
      makeNullableFlatVector<bool>({true, false, std::nullopt}),
  });

  // Test predicate IN for bool constant list.
  auto expected = makeNullableFlatVector<bool>({true, true, std::nullopt});
  std::string predicate = "c0 IN (TRUE, FALSE, NULL)";
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  expected = makeNullableFlatVector<bool>({true, false, std::nullopt});
  predicate = "c0 IN (TRUE)";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  expected =
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt, std::nullopt});
  predicate = "c0 IN (NULL)";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, varchar) {
  const vector_size_t size = 1'000;

  std::vector<std::string> fruits = {
      "apple", "banana", "pear", "grapes", "mango", "grapefruit"};

  auto vector = makeFlatVector<StringView>(size, [&fruits](auto row) {
    return StringView(fruits[row % fruits.size()]);
  });
  auto vectorWithNulls = makeFlatVector<StringView>(
      size,
      [&fruits](auto row) { return StringView(fruits[row % fruits.size()]); },
      nullEvery(7));
  auto rowVector = makeRowVector({vector, vectorWithNulls});

  // no nulls
  auto result = evaluate<SimpleVector<bool>>(
      "c0 IN ('apple', 'pear', 'banana')", rowVector);
  auto expected = makeFlatVector<bool>(size, [&fruits](auto row) {
    auto fruit = fruits[row % fruits.size()];
    return fruit == "apple" || fruit == "pear" || fruit == "banana";
  });

  assertEqualVectors(expected, result);

  // some nulls
  result = evaluate<SimpleVector<bool>>(
      "c1 IN ('apple', 'pear', 'banana')", rowVector);
  expected = makeFlatVector<bool>(
      size,
      [&fruits](auto row) {
        auto fruit = fruits[row % fruits.size()];
        return fruit == "apple" || fruit == "pear" || fruit == "banana";
      },
      nullEvery(7));

  assertEqualVectors(expected, result);

  // null values in the in-list
  // The results can be either true or null, but not false.
  result = evaluate<SimpleVector<bool>>(
      "c0 IN ('apple', 'pear', null, 'banana')", rowVector);
  expected = makeFlatVector<bool>(
      size,
      [](auto /* row */) { return true; },
      [&fruits](auto row) {
        auto fruit = fruits[row % fruits.size()];
        return !(fruit == "apple" || fruit == "pear" || fruit == "banana");
      });

  assertEqualVectors(expected, result);

  result = evaluate<SimpleVector<bool>>(
      "c1 IN ('apple', 'pear', null, 'banana')", rowVector);
  expected = makeFlatVector<bool>(
      size,
      [](auto /* row */) { return true; },
      [&fruits](auto row) {
        auto fruit = fruits[row % fruits.size()];
        return row % 7 == 0 ||
            !(fruit == "apple" || fruit == "pear" || fruit == "banana");
      });

  assertEqualVectors(expected, result);

  result = evaluate<SimpleVector<bool>>("c0 IN ('banana', null)", rowVector);
  expected = makeFlatVector<bool>(
      size,
      [](auto /* row */) { return true; },
      [&fruits](auto row) {
        auto fruit = fruits[row % fruits.size()];
        return !(fruit == "banana");
      });

  assertEqualVectors(expected, result);

  result = evaluate<SimpleVector<bool>>("c1 IN ('banana', null)", rowVector);
  expected = makeFlatVector<bool>(
      size,
      [](auto /* row */) { return true; },
      [&fruits](auto row) {
        auto fruit = fruits[row % fruits.size()];
        return row % 7 == 0 || !(fruit == "banana");
      });

  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, varcharConstant) {
  const vector_size_t size = 1'000;
  auto rowVector = makeRowVector(
      {makeConstant("apple", size), makeNullConstant(TypeKind::VARCHAR, size)});

  auto constTrue = makeConstant(true, size);
  auto constFalse = makeConstant(false, size);
  auto constNull = makeNullConstant(TypeKind::BOOLEAN, size);

  // a miss
  auto result =
      evaluate<SimpleVector<bool>>("c0 IN ('pear', 'banana')", rowVector);
  assertEqualVectors(constFalse, result);

  // a hit
  result = evaluate<SimpleVector<bool>>(
      "c0 IN ('apple', 'pear', 'banana')", rowVector);
  assertEqualVectors(constTrue, result);

  // a miss that is a null
  result =
      evaluate<SimpleVector<bool>>("c0 IN ('pear', null, 'banana')", rowVector);
  assertEqualVectors(constNull, result);

  // null
  result = evaluate<SimpleVector<bool>>(
      "c1 IN ('apple', 'pear', 'banana')", rowVector);
  assertEqualVectors(constNull, result);
}

TEST_F(InPredicateTest, varbinary) {
  auto input = makeRowVector({
      makeNullableFlatVector<std::string>({"apple", "banana"}, VARBINARY()),
  });

  std::string predicate =
      "c0 IN (CAST('apple' as VARBINARY), CAST('banana' as VARBINARY))";
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(makeConstant(true, input->size()), result);
}

TEST_F(InPredicateTest, date) {
  auto dateValue = DATE()->toDays("2000-01-01");

  auto input = makeRowVector({
      makeNullableFlatVector<int32_t>({dateValue}, DATE()),
  });

  assertEqualVectors(
      makeConstant(true, input->size()),
      evaluate("c0 IN (DATE '2000-01-01')", input));

  assertEqualVectors(
      makeConstant(false, input->size()),
      evaluate("c0 IN (DATE '2000-02-01')", input));

  assertEqualVectors(
      makeConstant(false, input->size()),
      evaluate("c0 IN (DATE '2000-02-01', DATE '2000-03-04')", input));

  assertEqualVectors(
      makeConstant(true, input->size()),
      evaluate(
          "c0 IN (DATE '2000-02-01', DATE '2000-03-04', DATE '2000-01-01')",
          input));
}

TEST_F(InPredicateTest, reusableResult) {
  std::string predicate = "c0 IN (1, 2)";
  auto input = makeRowVector({makeNullableFlatVector<int32_t>({0, 1, 2, 3})});
  SelectivityVector rows(input->size());
  VectorPtr result =
      makeNullableFlatVector<bool>({false, true, std::nullopt, false});
  auto actual = evaluate<SimpleVector<bool>>(predicate, input, rows, result);
  auto expected = makeFlatVector<bool>({false, true, true, false});
  assertEqualVectors(expected, actual);
}

TEST_F(InPredicateTest, doubleWithZero) {
  // zero and negative zero, FloatingPointRange
  auto input = makeRowVector({
      makeNullableFlatVector<double>({0.0, -0.0}, DOUBLE()),
  });
  auto predicate = "c0 IN ( 0.0 )";
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  auto expected = makeNullableFlatVector<bool>({true, true});
  assertEqualVectors(expected, result);

  // zero and negative zero, BigintValuesUsingHashTable, 0 in valuesList
  input = makeRowVector({
      makeNullableFlatVector<double>({0.0, -0.0}, DOUBLE()),
  });
  predicate = "c0 IN ( 0.0, 1.2, 2.3 )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, true});
  assertEqualVectors(expected, result);

  // zero and negative zero, BigintValuesUsingHashTable, -0 in valuesList
  input = makeRowVector({
      makeNullableFlatVector<double>({0.0, -0.0}, DOUBLE()),
  });
  predicate = "c0 IN ( -0.0, 1.2, 2.3, null )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, true});
  assertEqualVectors(expected, result);

  // TODO : zero and negative zero, BigintValuesUsingBitmask, depending on
  // another fix
}

TEST_F(InPredicateTest, double) {
  // No Null
  auto input = makeRowVector({
      makeNullableFlatVector<double>({1.2, 2.3, 3.4}, DOUBLE()),
  });
  std::string predicate = "c0 IN ( 1.2, 2.3, 3.4 )";
  auto expected = makeConstant(true, input->size());
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // InList has Null
  // Since there is only one non-null float, it will use FloatingPointRange
  input = makeRowVector({
      makeNullableFlatVector<double>({1.2, 2.3, 3.4}, DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, null )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, std::nullopt, std::nullopt});
  assertEqualVectors(expected, result);

  // InList has Null
  // Multiple non-null, using BigintValuesUsingHashTable
  predicate = "c0 IN ( 1.2, 2.3, null )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, true, std::nullopt});
  assertEqualVectors(expected, result);

  // Value(input) has NULL
  input = makeRowVector({
      makeNullableFlatVector<double>({1.2, 1.3, std::nullopt}, DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 2.3 )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, false, std::nullopt});
  assertEqualVectors(expected, result);

  // NaN
  input = makeRowVector({
      makeNullableFlatVector<double>({std::nan("")}, DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 2.3 )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({false});
  assertEqualVectors(expected, result);

  predicate = "c0 IN ( 1.2, null )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({std::nullopt});
  assertEqualVectors(expected, result);

  // Infinity
  input = makeRowVector({
      makeNullableFlatVector<double>(
          {std::numeric_limits<double>::infinity()}, DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 2.3 )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({false});
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, float) {
  // No Null
  auto input = makeRowVector({
      makeNullableFlatVector<float>({1.2, 2.3, 3.4}, REAL()),
  });
  std::string predicate =
      "c0 IN ( CAST(1.2 AS REAL), CAST(2.3 AS REAL), CAST(3.4 AS REAL) )";
  auto expected = makeConstant(true, input->size());
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  /// InList has Null
  // Since there is only one non-null float, it will use FloatingPointRange
  predicate = "c0 IN ( CAST(1.2 AS REAL), null )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, std::nullopt, std::nullopt});
  assertEqualVectors(expected, result);

  // InList has Null
  // Multiple non-null, using BigintValuesUsingHashTable
  // TODO: CAST(1.2 AS REAL), CAST(1.2 AS REAL) captured a bug in
  // BigintValuesUsingBitmask, it will be fixed in separate diff
  predicate = "c0 IN ( CAST(1.2 AS REAL), CAST(1.3 AS REAL), null )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, std::nullopt, std::nullopt});
  assertEqualVectors(expected, result);

  // Value(input) has NULL
  input = makeRowVector({
      makeNullableFlatVector<float>({1.2, 2.3, std::nullopt}, REAL()),
  });
  predicate = "c0 IN ( CAST(1.2 AS REAL), CAST(1.3 AS REAL) )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, false, std::nullopt});
  assertEqualVectors(expected, result);

  // NaN
  input = makeRowVector({
      makeNullableFlatVector<float>({std::nan("")}, REAL()),
  });
  predicate = "c0 IN ( CAST(1.2 AS REAL), CAST(1.3 AS REAL) )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({false});
  assertEqualVectors(expected, result);

  predicate = "c0 IN ( CAST(1.2 AS REAL), null )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({std::nullopt});
  assertEqualVectors(expected, result);

  // Infinity
  input = makeRowVector({
      makeNullableFlatVector<float>(
          {std::numeric_limits<float>::infinity()}, REAL()),
  });
  predicate = "c0 IN ( CAST(1.2 AS REAL), CAST(1.3 AS REAL) )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({false});
  assertEqualVectors(expected, result);
}
