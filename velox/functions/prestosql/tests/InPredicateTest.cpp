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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions {
namespace {

class InPredicateTest : public FunctionBaseTest {
 protected:
  template <typename T>
  std::string getInList(
      std::vector<std::optional<T>> input,
      const TypePtr& type = CppToType<T>::create()) {
    FlatVectorPtr<T> flatVec = makeNullableFlatVector<T>(input, type);
    std::string inList;
    auto len = flatVec->size();
    auto toString = [&](vector_size_t idx) {
      if (type->isDecimal()) {
        if (flatVec->isNullAt(idx)) {
          return std::string("null");
        }
        return fmt::format(
            "cast({} as {})", flatVec->toString(idx), type->toString());
      }
      return flatVec->toString(idx);
    };

    for (auto i = 0; i < len - 1; i++) {
      inList += fmt::format("{}, ", toString(i));
    }
    inList += toString(len - 1);
    return inList;
  }

  template <typename T>
  void testValues(const TypePtr type = CppToType<T>::create()) {
    if (type->isDecimal()) {
      this->options_.parseDecimalAsDouble = false;
    }
    std::shared_ptr<memory::MemoryPool> pool{
        memory::memoryManager()->addLeafPool()};

    const vector_size_t size = 1'000;
    auto inList = getInList<T>({1, 3, 5}, type);

    auto vector = makeFlatVector<T>(
        size, [](auto row) { return row % 17; }, nullptr, type);
    auto vectorWithNulls = makeFlatVector<T>(
        size, [](auto row) { return row % 17; }, nullEvery(7), type);
    auto rowVector = makeRowVector({vector, vectorWithNulls});

    // no nulls
    auto result = evaluate<SimpleVector<bool>>(
        fmt::format("c0 IN ({})", inList), rowVector);
    auto expected = makeFlatVector<bool>(size, [](auto row) {
      auto n = row % 17;
      return n == 1 || n == 3 || n == 5;
    });

    assertEqualVectors(expected, result);

    // some nulls
    result = evaluate<SimpleVector<bool>>(
        fmt::format("c1 IN ({})", inList), rowVector);
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
    inList = getInList<T>({1, 3, std::nullopt, 5}, type);
    result = evaluate<SimpleVector<bool>>(
        fmt::format("c0 IN ({})", inList), rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto /* row */) { return true; },
        [](auto row) {
          auto n = row % 17;
          return !(n == 1 || n == 3 || n == 5);
        });

    assertEqualVectors(expected, result);

    result = evaluate<SimpleVector<bool>>(
        fmt::format("c1 IN ({})", inList), rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto /* row */) { return true; },
        [](auto row) {
          auto n = row % 17;
          return row % 7 == 0 || !(n == 1 || n == 3 || n == 5);
        });

    assertEqualVectors(expected, result);

    inList = getInList<T>({2, std::nullopt}, type);
    result = evaluate<SimpleVector<bool>>(
        fmt::format("c0 IN ({})", inList), rowVector);
    expected = makeFlatVector<bool>(
        size,
        [](auto /* row */) { return true; },
        [](auto row) {
          auto n = row % 17;
          return !(n == 2);
        });

    assertEqualVectors(expected, result);

    result = evaluate<SimpleVector<bool>>(
        fmt::format("c1 IN ({})", inList), rowVector);
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

    inList = getInList<T>({2, 5, 9}, type);
    result = evaluate<SimpleVector<bool>>(
        fmt::format("c0 IN ({})", inList), rowVector);
    assertEqualVectors(expected, result);

    // an in list with nulls only is always null.
    result = evaluate<SimpleVector<bool>>("c0 IN (null)", rowVector);
    auto expectedConstant =
        BaseVector::createNullConstant(BOOLEAN(), size, pool_.get());
    assertEqualVectors(expectedConstant, result);
  }

  template <typename T>
  void testConstantValues(const TypePtr type = CppToType<T>::create()) {
    const vector_size_t size = 1'000;
    auto rowVector = makeRowVector(
        {makeConstant(static_cast<T>(123), size, type),
         BaseVector::createNullConstant(type, size, pool())});
    auto inList = getInList<T>({1, 3, 5}, type);

    auto constTrue = makeConstant(true, size);
    auto constFalse = makeConstant(false, size);
    auto constNull = makeNullConstant(TypeKind::BOOLEAN, size);

    // a miss
    auto result = evaluate<SimpleVector<bool>>(
        fmt::format("c0 IN ({})", inList), rowVector);
    assertEqualVectors(constFalse, result);

    // null
    result = evaluate<SimpleVector<bool>>(
        fmt::format("c1 IN ({})", inList), rowVector);
    assertEqualVectors(constNull, result);

    // a hit
    inList = getInList<T>({1, 123, 5}, type);
    result = evaluate<SimpleVector<bool>>(
        fmt::format("c0 IN ({})", inList), rowVector);
    assertEqualVectors(constTrue, result);

    // a miss that is a null
    inList = getInList<T>({1, std::nullopt, 5}, type);
    result = evaluate<SimpleVector<bool>>(
        fmt::format("c0 IN ({})", inList), rowVector);
    assertEqualVectors(constNull, result);
  }

  static core::TypedExprPtr field(
      const TypePtr& type,
      const std::string& name) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  core::TypedExprPtr makeInExpression(const VectorPtr& values) {
    BufferPtr offsets = allocateOffsets(1, pool());
    BufferPtr sizes = allocateSizes(1, pool());
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    rawSizes[0] = values->size();

    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{
            field(values->type(), "c0"),
            std::make_shared<core::ConstantTypedExpr>(
                std::make_shared<ArrayVector>(
                    pool(),
                    ARRAY(values->type()),
                    nullptr,
                    1,
                    offsets,
                    sizes,
                    values)),
        },
        "in");
  }

  VectorPtr makeTimestampVector(const std::vector<int64_t>& millis) {
    std::vector<Timestamp> timestamps;
    timestamps.reserve(millis.size());
    for (auto n : millis) {
      timestamps.push_back(Timestamp::fromMillis(n));
    }

    return makeFlatVector(timestamps);
  }

  template <typename T>
  void testNaNs() {
    const T kNaN = std::numeric_limits<T>::quiet_NaN();
    const T kSNaN = std::numeric_limits<T>::signaling_NaN();
    TypePtr columnFloatType = CppToType<T>::create();

    // Constant In-list, primitive input.
    auto testInWithConstList = [&](std::vector<T> input,
                                   std::vector<T> inlist,
                                   std::vector<bool> expected) {
      auto expr = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              field(columnFloatType, "c0"),
              std::make_shared<core::ConstantTypedExpr>(
                  makeArrayVector<T>({inlist})),
          },
          "in");
      auto data = makeRowVector({
          makeFlatVector<T>(input),
      });
      auto expectedResults = makeFlatVector<bool>(expected);
      auto result = evaluate(expr, data);
      assertEqualVectors(expectedResults, result);
    };

    testInWithConstList({kNaN, kSNaN}, {kNaN, 1}, {true, true});
    testInWithConstList({kNaN, kSNaN}, {1, 2}, {false, false});
    // Need to specifically test in-list with a single element as it previously
    // had a seperate codepath.
    testInWithConstList({kNaN, kSNaN}, {kNaN}, {true, true});
    testInWithConstList({kNaN, kSNaN}, {1}, {false, false});

    {
      // Constant In-list, complex input(row).
      // In-list is [row{kNaN, 1}].
      auto inlist = makeArrayVector(
          {0},
          makeRowVector(
              {makeFlatVector<T>(std::vector<T>({kNaN})),
               makeFlatVector<int32_t>(std::vector<int32_t>({1}))}));
      auto expr = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              field(ROW({columnFloatType, INTEGER()}), "c0"),
              std::make_shared<core::ConstantTypedExpr>(inlist),
          },
          "in");
      // Input is [row{kNaN, 1}, row{kSNaN, 1}, row{kNaN, 2}].
      auto data = makeRowVector({makeRowVector(
          {makeFlatVector<T>(std::vector<T>({kNaN, kSNaN, kNaN})),
           makeFlatVector<int32_t>(std::vector<int32_t>({1, 1, 2}))})});
      auto expectedResults = makeFlatVector<bool>({true, true, false});
      auto result = evaluate(expr, data);
      assertEqualVectors(expectedResults, result);
    }

    {
      // Variable In-list, primitive input.
      auto data = makeRowVector({
          makeFlatVector<T>({kNaN, kSNaN, kNaN}),
          makeFlatVector<T>({kNaN, kNaN, 0}),
          makeFlatVector<T>({1, 1, 1}),
      });
      // Expression: c0 in (c1, c2)
      auto inWithVariableInList = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              field(columnFloatType, "c0"),
              field(columnFloatType, "c1"),
              field(columnFloatType, "c2"),
          },
          "in");
      auto expectedResults = makeFlatVector<bool>({
          true, // kNaN in (kNaN, 1)
          true, // kSNaN in (kNaN, 1)
          false, // kNaN in (kNaN, 0)
      });
      auto result = evaluate(inWithVariableInList, data);
      assertEqualVectors(expectedResults, result);
    }

    {
      // Variable In-list, complex input(row).
      // Input is:
      // c0: [row{kNaN, 1}, row{kSNaN, 1}, row{kNaN, 2}]
      // c1: [row{kNaN, 1}, row{kNaN, 1}, row{kNaN, 1}]
      auto data = makeRowVector(
          {makeRowVector(
               {makeFlatVector<T>(std::vector<T>({kNaN, kSNaN, kNaN})),
                makeFlatVector<int32_t>(std::vector<int32_t>({1, 1, 2}))}),
           makeRowVector(
               {makeFlatVector<T>(std::vector<T>({kNaN, kNaN, kNaN})),
                makeFlatVector<int32_t>(std::vector<int32_t>({1, 1, 1}))})});
      // Expression: c0 in (c1)
      auto inWithVariableInList = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{
              field(ROW({columnFloatType, INTEGER()}), "c0"),
              field(ROW({columnFloatType, INTEGER()}), "c1"),
          },
          "in");
      auto expectedResults = makeFlatVector<bool>({true, true, false});
      auto result = evaluate(inWithVariableInList, data);
      assertEqualVectors(expectedResults, result);
    }
  }
};

TEST_F(InPredicateTest, bigint) {
  testValues<int64_t>();
  testConstantValues<int64_t>();
}

TEST_F(InPredicateTest, integer) {
  testValues<int32_t>();
  testConstantValues<int32_t>();
}

TEST_F(InPredicateTest, smallint) {
  testValues<int16_t>();
  testConstantValues<int16_t>();
}

TEST_F(InPredicateTest, tinyint) {
  testValues<int8_t>();
  testConstantValues<int8_t>();
}

TEST_F(InPredicateTest, timestamp) {
  auto inValues = makeTimestampVector({0, 1, 1'133, 12'345});

  auto data =
      makeRowVector({makeTimestampVector({0, 2, 123, 1'133, 78, 12'345})});
  auto expected = makeFlatVector<bool>({true, false, false, true, false, true});

  auto result = evaluate(makeInExpression(inValues), {data});
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, shortDecimal) {
  testValues<int64_t>(DECIMAL(5, 2));
  testConstantValues<int64_t>(DECIMAL(5, 2));
}

TEST_F(InPredicateTest, longDecimal) {
  testValues<int128_t>(DECIMAL(19, 7));
  testConstantValues<int128_t>(DECIMAL(19, 7));
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
  auto dateValue = parseDate("2000-01-01");

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

  // duplicate 0
  input = makeRowVector({
      makeNullableFlatVector<double>({0.0, -0.0}, DOUBLE()),
  });
  predicate = "c0 IN ( 0.0, 0.0, -0.0 )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, true});
  assertEqualVectors(expected, result);

  input = makeRowVector({
      makeNullableFlatVector<double>({0.0, -0.0, 1.0, 2.0}, DOUBLE()),
  });
  predicate = "c0 IN ( 0.0 )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, true, false, false});
  assertEqualVectors(expected, result);

  input = makeRowVector({
      makeNullableFlatVector<double>({0.0, -0.0, 1.0, 2.0}, DOUBLE()),
  });
  predicate = "c0 IN ( -0.0 )";
  result = evaluate<SimpleVector<bool>>(predicate, input);
  expected = makeNullableFlatVector<bool>({true, true, false, false});
  assertEqualVectors(expected, result);
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

TEST_F(InPredicateTest, bigIntDuplicateWithBigintRange) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2}, BIGINT()),
  });
  std::string predicate = "c0 IN ( 2, 2, 2, 2 )";
  auto expected = makeNullableFlatVector<bool>({false, true});
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // NOT IN
  input = makeRowVector({
      makeFlatVector<int64_t>({3, 4, 5}, BIGINT()),
  });
  predicate = "c0 NOT IN ( 4, 4, 4, 4 )";
  expected = makeNullableFlatVector<bool>({true, false, true});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // NULL
  input = makeRowVector({
      makeFlatVector<int64_t>({3, 4, 5}, BIGINT()),
  });
  predicate = "c0 IN ( 4, 4, 4, 4, null )";
  expected = makeNullableFlatVector<bool>({std::nullopt, true, std::nullopt});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, bigIntDuplicateWithBigintValuesUsingBitmask) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 5}, BIGINT()),
  });
  std::string predicate = "c0 IN ( 1, 2, 2, 3, 4 )";
  auto expected = makeNullableFlatVector<bool>({true, true, false});
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // NULL
  input = makeRowVector({
      makeFlatVector<int64_t>({0, 4, 5}, BIGINT()),
  });
  predicate = "c0 IN ( 1, 3, 4, 4, 4, 5, null )";
  expected = makeNullableFlatVector<bool>({std::nullopt, true, true});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);
}

// Filter.cpp range check logic (uint64_t)range + 1 == values.size()
// will have correctness issue, if there is no dedupe logic
TEST_F(InPredicateTest, bigIntDuplicateWithContinuousBlock) {
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({3}, BIGINT()),
  });
  auto predicate = "c0 IN ( 1, 2, 2, 4 )";
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  auto expected = makeNullableFlatVector<bool>({false});
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, doubleDuplicateWithFloatingPointRange) {
  auto input = makeRowVector({
      makeFlatVector<double>({1.2, 2.3}, DOUBLE()),
  });
  std::string predicate = "c0 IN ( 1.2, 1.2, null )";
  auto expected = makeNullableFlatVector<bool>({true, std::nullopt});
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, doubleDuplicateWithBigintValuesUsingHashTable) {
  auto input = makeRowVector({
      makeFlatVector<double>({1.2, 4.5}, DOUBLE()),
  });
  std::string predicate = "c0 IN ( 1.2, 1.2, 2.3, 3.4 )";
  auto expected = makeNullableFlatVector<bool>({true, false});
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // NULL
  input = makeRowVector({
      makeFlatVector<double>({1.2, 4.5}, DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 1.2, 2.3, 3.4, null )";
  expected = makeNullableFlatVector<bool>({true, std::nullopt});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // NaN
  input = makeRowVector({
      makeFlatVector<double>({std::nan(""), std::nan("")}, DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 1.2, 2.3, 3.4 )";
  expected = makeNullableFlatVector<bool>({false, false});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // NaN with null
  input = makeRowVector({
      makeFlatVector<double>({std::nan(""), std::nan("")}, DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 1.2, 2.3, 3.4, null )";
  expected = makeNullableFlatVector<bool>({std::nullopt, std::nullopt});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // Inf, -Inf
  input = makeRowVector({
      makeFlatVector<double>(
          {std::numeric_limits<double>::infinity(),
           -std::numeric_limits<double>::infinity()},
          DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 1.2, 2.3, 3.4 )";
  expected = makeNullableFlatVector<bool>({false, false});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // Inf, -Inf with null
  input = makeRowVector({
      makeFlatVector<double>(
          {std::numeric_limits<double>::infinity(),
           -std::numeric_limits<double>::infinity()},
          DOUBLE()),
  });
  predicate = "c0 IN ( 1.2, 1.2, 2.3, 3.4, null )";
  expected = makeNullableFlatVector<bool>({std::nullopt, std::nullopt});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, floatDuplicateWithFloatingPointRange) {
  // No Null
  auto input = makeRowVector({
      makeNullableFlatVector<float>({1.2, 2.3}, REAL()),
  });
  std::string predicate = "c0 IN ( CAST(1.2 AS REAL), CAST(1.2 AS REAL) )";
  auto expected = makeNullableFlatVector<bool>({true, false});
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);

  // with null
  input = makeRowVector({
      makeNullableFlatVector<float>({1.2, 2.3}, REAL()),
  });
  predicate = "c0 IN ( CAST(1.2 AS REAL), CAST(1.2 AS REAL), null )";
  expected = makeNullableFlatVector<bool>({true, std::nullopt});
  result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, floatDuplicateWithBigintValuesUsingHashTable) {
  // No Null
  auto input = makeRowVector({
      makeNullableFlatVector<float>({1.2, 2.3}, REAL()),
  });
  std::string predicate =
      "c0 IN ( CAST(1.2 AS REAL), CAST(1.2 AS REAL), CAST(1.2 AS REAL), CAST(2.3 AS REAL) )";
  auto expected = makeNullableFlatVector<bool>({true, true});
  auto result = evaluate<SimpleVector<bool>>(predicate, input);
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, arrays) {
  auto inValues = makeArrayVector<int32_t>({
      {1},
      {1, 2},
      {1, 2, 3},
      {},
  });

  auto data = makeRowVector({
      makeNullableArrayVector<int32_t>({
          {{1, 2, 3}},
          {{}},
          {{1, 3}},
          std::nullopt,
          {{2, 4, 5, 6}},
          {{1, std::nullopt, 2}},
          {{1, 2, 3, 4}},
      }),
  });

  auto expected = makeNullableFlatVector<bool>({
      true,
      true,
      false,
      std::nullopt,
      false,
      std::nullopt,
      false,
  });
  auto result = evaluate(makeInExpression(inValues), {data});
  assertEqualVectors(expected, result);

  auto inValuesWithNulls = makeNullableArrayVector<int32_t>({
      {1},
      {1, std::nullopt, 2},
      {1, 2, 3},
      {},
  });

  expected = makeNullableFlatVector<bool>(
      {true,
       true,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt});
  result = evaluate(makeInExpression(inValuesWithNulls), {data});
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, maps) {
  auto inValues = makeMapVector<int32_t, int64_t>({
      {{1, 10}},
      {{1, 10}, {2, 20}},
      {},
  });

  auto inExpr = makeInExpression(inValues);

  auto data = makeRowVector({
      makeMapVector<int32_t, int64_t>({
          {{1, 10}},
          {{1, 10}, {2, 12}, {3, 13}},
          {{1, 10}, {2, 20}},
          {{1, 10}, {2, 20}, {3, 30}},
          {},
          {{1, 5}},
      }),
  });

  auto expected = makeFlatVector<bool>({true, false, true, false, true, false});
  auto result = evaluate(inExpr, {data});
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, structs) {
  auto inValues = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<std::string>({"a", "b", "c"}),
  });

  auto inExpr = makeInExpression(inValues);

  auto data = makeRowVector({
      makeRowVector({
          makeFlatVector<int32_t>({1, 1, 2, 2, 3, 4}),
          makeFlatVector<std::string>({"a", "zzz", "b", "abc", "c", "c"}),
      }),
  });

  auto expected = makeFlatVector<bool>({true, false, true, false, true, false});
  auto result = evaluate(inExpr, {data});
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, nonConstantInList) {
  auto data = makeRowVector({
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt}),
      makeNullableFlatVector<int32_t>({1, 1, 1, std::nullopt, 1}),
      makeNullableFlatVector<int32_t>({2, 3, std::nullopt, 2, 2}),
      makeNullableFlatVector<int32_t>({3, 5, 3, 3, 3}),
  });

  auto expected = makeNullableFlatVector<bool>({
      true, // 1 in (1, 2, 3)
      false, // 2 in (1, 3, 5)
      true, // 3 in (1, null, 3)
      std::nullopt, // 4 in (null, 2, 3)
      std::nullopt, // null in (1, 2, 3)
  });

  auto in = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{
          field(INTEGER(), "c0"),
          field(INTEGER(), "c1"),
          field(INTEGER(), "c2"),
          field(INTEGER(), "c3"),
      },
      "in");

  auto result = evaluate(in, data);
  assertEqualVectors(expected, result);
}

TEST_F(InPredicateTest, nans) {
  // Ensure that NaNs with different bit patterns are treated as equal.
  testNaNs<float>();
  testNaNs<double>();
}

} // namespace
} // namespace facebook::velox::functions
