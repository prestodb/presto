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
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

#include <fmt/format.h>
#include <cstdint>

using namespace facebook::velox;
using namespace facebook::velox::test;
using facebook::velox::functions::test::FunctionBaseTest;

namespace {

const std::unordered_set<TypeKind> kSupportedTypes = {
    TypeKind::BOOLEAN,
    TypeKind::TINYINT,
    TypeKind::SMALLINT,
    TypeKind::INTEGER,
    TypeKind::BIGINT,
    TypeKind::REAL,
    TypeKind::DOUBLE,
    TypeKind::VARCHAR,
    TypeKind::ARRAY,
    TypeKind::ROW};

using TestArrayType = std::vector<std::optional<StringView>>;
using TestRowType = variant;

class ArraySortTest : public FunctionBaseTest,
                      public testing::WithParamInterface<TypeKind> {
 protected:
  ArraySortTest() : numValues_(10), numVectors_(5) {}

  void SetUp() override;

  // Build a flat vector with numeric native type of T. The value in the
  // returned flat vector is in ascending order.
  template <typename T>
  FlatVectorPtr<T> buildScalarVector() {
    return makeFlatVector<T>(numValues_, [](auto row) { return row + 1; });
  }

  template <typename T>
  const FlatVector<T>* getScalarVector() {
    return dataVectorsByType_[CppToType<T>::typeKind]
        ->template asFlatVector<T>();
  }

  template <typename T>
  T dataAt(vector_size_t index) {
    EXPECT_LT(index, numValues_);
    return getScalarVector<T>()->valueAt(index);
  }

  template <typename T>
  ArrayVectorPtr arrayVector(const std::vector<std::optional<T>>& inputValues) {
    std::vector<std::vector<std::optional<T>>> inputVectors;
    inputVectors.reserve(numVectors_);
    for (int i = 0; i < numVectors_; ++i) {
      inputVectors.push_back(inputValues);
    }
    return makeNullableArrayVector<T>(inputVectors);
  }

  template <typename T>
  VectorPtr makeDataArray(const std::vector<std::optional<int32_t>>& indices) {
    std::vector<std::optional<T>> data;
    data.reserve(indices.size());
    for (auto i : indices) {
      if (i.has_value()) {
        data.push_back(dataAt<T>(i.value()));
      } else {
        data.push_back(std::nullopt);
      }
    }
    return arrayVector(data);
  }

  template <typename T>
  void test() {
    struct {
      const RowVectorPtr inputVector;
      const VectorPtr expectedResult;
      const VectorPtr expectedDescResult;

      const std::string debugString() const {
        return fmt::format(
            "\ntype: {}\ninputVector: {}\nexpectedResult: {}",
            GetParam(),
            inputVector->toString(0, inputVector->size()),
            expectedResult->toString(0, expectedResult->size()));
      }
    } testSettings[] = {
        {
            makeRowVector({makeDataArray<T>({2, 1, 0})}),
            makeDataArray<T>({0, 1, 2}),
            makeDataArray<T>({2, 1, 0}),
        },

        {
            makeRowVector({makeDataArray<T>({0, 1, 2})}),
            makeDataArray<T>({0, 1, 2}),
            makeDataArray<T>({2, 1, 0}),
        },

        {
            makeRowVector({makeDataArray<T>({0, 0, 0})}),
            makeDataArray<T>({0, 0, 0}),
            makeDataArray<T>({0, 0, 0}),
        },

        {
            makeRowVector({makeDataArray<T>({1, 0, 2})}),
            makeDataArray<T>({0, 1, 2}),
            makeDataArray<T>({2, 1, 0}),
        },

        {
            makeRowVector({makeDataArray<T>({std::nullopt, 1, 0, 2})}),
            makeDataArray<T>({0, 1, 2, std::nullopt}),
            makeDataArray<T>({2, 1, 0, std::nullopt}),
        },

        {
            makeRowVector(
                {makeDataArray<T>({std::nullopt, std::nullopt, 1, 0, 2})}),
            makeDataArray<T>({0, 1, 2, std::nullopt, std::nullopt}),
            makeDataArray<T>({2, 1, 0, std::nullopt, std::nullopt}),
        },

        {
            makeRowVector(
                {makeDataArray<T>({std::nullopt, 1, 0, std::nullopt, 2})}),
            makeDataArray<T>({0, 1, 2, std::nullopt, std::nullopt}),
            makeDataArray<T>({2, 1, 0, std::nullopt, std::nullopt}),
        },

        {
            makeRowVector(
                {makeDataArray<T>({1, std::nullopt, 0, 2, std::nullopt})}),
            makeDataArray<T>({0, 1, 2, std::nullopt, std::nullopt}),
            makeDataArray<T>({2, 1, 0, std::nullopt, std::nullopt}),
        },

        {
            makeRowVector({makeDataArray<T>(
                {std::nullopt,
                 std::nullopt,
                 std::nullopt,
                 std::nullopt,
                 std::nullopt})}),
            makeDataArray<T>(
                {std::nullopt,
                 std::nullopt,
                 std::nullopt,
                 std::nullopt,
                 std::nullopt}),
            makeDataArray<T>(
                {std::nullopt,
                 std::nullopt,
                 std::nullopt,
                 std::nullopt,
                 std::nullopt}),
        },
    };
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      auto actualResult = evaluate("array_sort(c0)", testData.inputVector);
      assertEqualVectors(testData.expectedResult, actualResult);

      auto descResult = evaluate("array_sort_desc(c0)", testData.inputVector);
      assertEqualVectors(testData.expectedDescResult, descResult);
    }
  }

  void runTest(TypeKind kind) {
    switch (kind) {
      case TypeKind::BOOLEAN:
        test<bool>();
        break;
      case TypeKind::TINYINT:
        test<int8_t>();
        break;
      case TypeKind::SMALLINT:
        test<int16_t>();
        break;
      case TypeKind::INTEGER:
        test<int32_t>();
        break;
      case TypeKind::BIGINT:
        test<int64_t>();
        break;
      case TypeKind::REAL:
        test<float>();
        break;
      case TypeKind::DOUBLE:
        test<double>();
        break;
      case TypeKind::VARCHAR:
        test<StringView>();
        break;
      case TypeKind::ARRAY:
        test<TestArrayType>();
        break;
      case TypeKind::ROW:
        test<TestRowType>();
        break;
      default:
        VELOX_FAIL(
            "Unsupported data type of sort_array: {}", mapTypeKindToName(kind));
    }
  }

  template <typename T>
  void testFloatingPoint() {
    // Verify that NaNs are treated as greater than infinity
    static const T kNaN = std::numeric_limits<T>::quiet_NaN();
    static const T kInfinity = std::numeric_limits<T>::infinity();
    static const T kNegativeInfinity = -1 * std::numeric_limits<T>::infinity();

    auto input = makeRowVector({makeNullableArrayVector<T>(
        {{kInfinity, -1, kNaN, 1, kNegativeInfinity, kNaN, 0}})});

    {
      auto expected = makeNullableArrayVector<T>(
          {{kNegativeInfinity, -1, 0, 1, kInfinity, kNaN, kNaN}});
      assertEqualVectors(expected, evaluate("try(array_sort(c0))", input));
    }

    {
      auto expected = makeNullableArrayVector<T>(
          {{kNaN, kNaN, kInfinity, 1, 0, -1, kNegativeInfinity}});
      assertEqualVectors(expected, evaluate("try(array_sort_desc(c0))", input));
    }
  }

  // Specify the number of values per each data vector in 'dataVectorsByType_'.
  const int numValues_;
  std::unordered_map<TypeKind, VectorPtr> dataVectorsByType_;
  // Specify the number of vectors in test.
  const int numVectors_;
};

// Build a flat vector with StringView. The value in the returned flat vector
// is in ascending order.
template <>
FlatVectorPtr<StringView> ArraySortTest::buildScalarVector() {
  std::string value;
  return makeFlatVector<StringView>(
      numValues_,
      [&, maxValueLen = (int)std::ceil((double)numValues_ / 26.0)](auto row) {
        const int valueLen = row % maxValueLen + 1;
        const char c = 'a' + row / maxValueLen;
        value = std::string(valueLen, c);
        return StringView(value);
      });
}

template <>
FlatVectorPtr<bool> ArraySortTest::buildScalarVector() {
  std::string value;
  return makeFlatVector<bool>(numValues_, [&](auto row) {
    return row < numValues_ / 2 ? false : true;
  });
}

template <>
TestArrayType ArraySortTest::dataAt<TestArrayType>(vector_size_t index) {
  EXPECT_LT(index, numValues_);
  TestArrayType array;
  const auto elementValue = getScalarVector<StringView>()->valueAt(index);
  for (int i = 0; i < numValues_; ++i) {
    array.push_back(elementValue);
  }
  return array;
}

template <>
TestRowType ArraySortTest::dataAt<TestRowType>(vector_size_t index) {
  EXPECT_LT(index, numValues_);
  return variant::row({getScalarVector<double>()->valueAt(index)});
}

template <>
ArrayVectorPtr ArraySortTest::arrayVector<TestArrayType>(
    const std::vector<std::optional<TestArrayType>>& inputValues) {
  std::vector<std::optional<std::vector<std::optional<TestArrayType>>>>
      inputVectors;
  inputVectors.reserve(numVectors_);
  for (int i = 0; i < numVectors_; ++i) {
    inputVectors.push_back(inputValues);
  }
  return makeNullableNestedArrayVector<StringView>(inputVectors);
}

template <>
ArrayVectorPtr ArraySortTest::arrayVector<TestRowType>(
    const std::vector<std::optional<TestRowType>>& inputValues) {
  std::vector<variant> inputVariants;
  inputVariants.reserve(inputValues.size());
  for (int i = 0; i < inputValues.size(); ++i) {
    if (inputValues[i].has_value()) {
      inputVariants.push_back(inputValues[i].value());
    } else {
      inputVariants.push_back(variant::null(TypeKind::ROW));
    }
  }

  std::vector<std::vector<variant>> inputVariantVectors;
  inputVariantVectors.reserve(numVectors_);
  for (int i = 0; i < numVectors_; ++i) {
    inputVariantVectors.push_back(inputVariants);
  }

  const auto rowType = ROW({DOUBLE()});
  return makeArrayOfRowVector(rowType, inputVariantVectors);
}

void ArraySortTest::SetUp() {
  for (const TypeKind type : kSupportedTypes) {
    switch (type) {
      case TypeKind::BOOLEAN:
        dataVectorsByType_.emplace(type, buildScalarVector<bool>());
        break;
      case TypeKind::TINYINT:
        dataVectorsByType_.emplace(type, buildScalarVector<int8_t>());
        break;
      case TypeKind::SMALLINT:
        dataVectorsByType_.emplace(type, buildScalarVector<int16_t>());
        break;
      case TypeKind::INTEGER:
        dataVectorsByType_.emplace(type, buildScalarVector<int32_t>());
        break;
      case TypeKind::BIGINT:
        dataVectorsByType_.emplace(type, buildScalarVector<int64_t>());
        break;
      case TypeKind::REAL:
        dataVectorsByType_.emplace(type, buildScalarVector<float>());
        break;
      case TypeKind::DOUBLE:
        dataVectorsByType_.emplace(type, buildScalarVector<double>());
        break;
      case TypeKind::VARCHAR:
        dataVectorsByType_.emplace(type, buildScalarVector<StringView>());
        break;
      case TypeKind::ARRAY:
      case TypeKind::ROW:
        // ARRAY and ROW will reuse the scalar data vectors built for DOUBLE and
        // VARCHAR respectively.
        break;
      default:
        VELOX_FAIL(
            "Unsupported data type of sort_array: {}", mapTypeKindToName(type));
    }
  }
  ASSERT_LE(dataVectorsByType_.size(), kSupportedTypes.size());
}

TEST_P(ArraySortTest, basic) {
  runTest(GetParam());
}

TEST_F(ArraySortTest, unknown) {
  auto input = makeNullableArrayVector<UnknownValue>({
      {std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt},
  });

  auto result = evaluate("array_sort(c0)", makeRowVector({input}));
  assertEqualVectors(input, result);

  input = makeArrayVectorFromJson<int32_t>({
      "[1, 2, 3]",
      "[1, 2]",
  });

  result = evaluate("array_sort(c0, x -> null)", makeRowVector({input}));
  assertEqualVectors(input, result);
}

TEST_F(ArraySortTest, constant) {
  vector_size_t size = 1'000;
  auto data =
      makeArrayVector<int64_t>({{1, 2, 3, 0}, {4, 5, 4, 5}, {6, 6, 6, 6}});

  auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "array_sort(c0)",
        makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
  };

  auto result = evaluateConstant(0, data);
  auto expected = makeConstantArray<int64_t>(size, {0, 1, 2, 3});
  assertEqualVectors(expected, result);

  result = evaluateConstant(1, data);
  expected = makeConstantArray<int64_t>(size, {4, 4, 5, 5});
  assertEqualVectors(expected, result);

  result = evaluateConstant(2, data);
  expected = makeConstantArray<int64_t>(size, {6, 6, 6, 6});
  assertEqualVectors(expected, result);
}

TEST_F(ArraySortTest, dictionaryEncodedElements) {
  auto elementVector = makeNullableFlatVector<int64_t>({3, 1, 2, 4, 5});
  auto dictionaryVector = BaseVector::wrapInDictionary(
      makeNulls(5, nullEvery(2)), makeIndicesInReverse(5), 5, elementVector);
  // Array vector with one array.
  auto arrayVector = makeArrayVector({0}, dictionaryVector);
  auto result = evaluate("array_sort(c0)", makeRowVector({arrayVector}));
  assertEqualVectors(
      result,
      makeNullableArrayVector<int64_t>(
          {{1, 4, std::nullopt, std::nullopt, std::nullopt}}));

  // Array vector with 2 arrays.
  arrayVector = makeArrayVector({0, 2}, dictionaryVector);
  result = evaluate("array_sort(c0)", makeRowVector({arrayVector}));
  assertEqualVectors(
      result,
      makeNullableArrayVector<int64_t>(
          {{4, std::nullopt}, {1, std::nullopt, std::nullopt}}));
}

// Test arrays with dictionary-encoded elements of complex type.
TEST_P(ArraySortTest, encodedElements) {
  // Base vector: [0, 10, 20, 30, 40, 50].
  // Dictionary reverses the order of rows, then repeats them:
  // [50, 40, 30, 20, 10, 0, 50, 40, 30, 20, 10, 0]
  // and adds nulls for even rows: [null, 40, null, 20, null, 0].
  auto elements = BaseVector::wrapInDictionary(
      makeNulls(12, nullEvery(2)),
      makeIndices({5, 4, 3, 2, 1, 0, 5, 4, 3, 2, 1, 0}),
      12,
      makeRowVector({
          makeFlatVector<int64_t>({0, 10, 20, 30, 40, 50}),
          makeFlatVector<int32_t>({0, -10, -20, -30, -40, -50}),
      }));

  // Make an array vector with 3, 3, 6 elements per row:
  // [[null, 40, null], [20, null, 0], [null, 40, null, 20, null, 0]].
  auto array = makeArrayVector({0, 3, 6}, elements);

  auto result = evaluate("array_sort(c0)", {makeRowVector({array})});

  // After sorting we expect
  //  [[40, null, null], [0, 20, null], [0, 20, 40, null, null, null]].
  auto expected = makeArrayVector(
      {0, 3, 6},
      makeRowVector(
          {
              makeFlatVector<int64_t>(
                  {40, -1, -1, 0, 20, -1, 0, 20, 40, -1, -1, -1}),
              makeFlatVector<int32_t>(
                  {-40, 1, 1, 0, -20, 1, 0, -20, -40, -1, -1, -1}),
          },
          // Nulls in rows 1, 2, 5, 9, 10, 11.
          [](auto row) {
            return row == 1 || row == 2 || row == 5 || row >= 9;
          }));
  assertEqualVectors(expected, result);

  // Apply sort twice.
  result = evaluate("array_sort(array_sort(c0))", {makeRowVector({array})});
  assertEqualVectors(expected, result);
}

TEST_F(ArraySortTest, wellFormedVectors) {
  // A test that make sure that offsets of unselected indices that appears in
  // the output are still valid (refer to addressable locations in the elements
  // vector) in the final output vector.
  auto base = makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

  auto makeBuffer = [&](const std::vector<vector_size_t>& values) {
    BufferPtr buffer = facebook::velox::allocateOffsets(values.size(), pool());
    auto rawBuffer = buffer->asMutable<vector_size_t>();

    for (int i = 0; i < values.size(); i++) {
      rawBuffer[i] = values[i];
    }
    return buffer;
  };

  // Make array of size 3 but with offset at position 2 > position 3.
  auto offsets = makeBuffer({0, 4, 1});
  auto sizes = makeBuffer({1, 5, 1});

  auto array = std::make_shared<ArrayVector>(
      pool(), ARRAY(BIGINT()), nullptr, 3, offsets, sizes, base);
  auto data = makeRowVector({array});
  const std::string expression = "array_sort(c0)";
  auto typedExpr = makeTypedExpr(expression, asRowType(data->type()));

  SelectivityVector rows(data->size(), false);

  std::vector<VectorPtr> results(1);
  exec::ExprSet exprSet({typedExpr}, &execCtx_);
  exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());

  // Evaluate and ensure middle row is not selected.
  rows.setValid(0, true);
  rows.setValid(2, true);
  rows.updateBounds();
  exprSet.eval(rows, evalCtx, results);
  VectorPtr result = results[0];

  // Ensure that array vector is addressable right.
  // That is all offset + size should be < element.size().
  // In https://github.com/facebookincubator/velox/issues/4754 we found a bug
  // that caused us to create element vectors with size < offsets + size.
  auto arrayVec = result->asUnchecked<ArrayVector>();
  EXPECT_TRUE(arrayVec);
  EXPECT_GT(arrayVec->offsetAt(2), 0);
  EXPECT_LE(
      arrayVec->offsetAt(1) + arrayVec->sizeAt(1),
      arrayVec->elements()->size());
}

TEST_F(ArraySortTest, lambda) {
  auto data = makeRowVector({makeNullableArrayVector<std::string>({
      {"abc123", "abc", std::nullopt, "abcd"},
      {std::nullopt, "x", "xyz123", "xyz"},
  })});

  auto sortedAsc = makeNullableArrayVector<std::string>({
      {"abc", "abcd", "abc123", std::nullopt},
      {"x", "xyz", "xyz123", std::nullopt},
  });

  auto sortedDesc = makeNullableArrayVector<std::string>({
      {"abc123", "abcd", "abc", std::nullopt},
      {"xyz123", "xyz", "x", std::nullopt},
  });

  auto testAsc = [&](const std::string& name, const std::string& lambdaExpr) {
    SCOPED_TRACE(name);
    SCOPED_TRACE(lambdaExpr);
    auto result = evaluate(fmt::format("{}(c0, {})", name, lambdaExpr), data);
    assertEqualVectors(sortedAsc, result);

    SelectivityVector firstRow(1);
    result =
        evaluate(fmt::format("{}(c0, {})", name, lambdaExpr), data, firstRow);
    assertEqualVectors(sortedAsc->slice(0, 1), result);
  };

  auto testDesc = [&](const std::string& name, const std::string& lambdaExpr) {
    SCOPED_TRACE(name);
    SCOPED_TRACE(lambdaExpr);
    auto result = evaluate(fmt::format("{}(c0, {})", name, lambdaExpr), data);
    assertEqualVectors(sortedDesc, result);

    SelectivityVector firstRow(1);
    result =
        evaluate(fmt::format("{}(c0, {})", name, lambdaExpr), data, firstRow);
    assertEqualVectors(sortedDesc->slice(0, 1), result);
  };

  // Different ways to sort by length ascending.
  testAsc("array_sort", "x -> length(x)");
  testAsc("array_sort_desc", "x -> length(x) * -1");
  testAsc(
      "array_sort",
      "(x, y) -> if(length(x) < length(y), -1, if(length(x) > length(y), 1, 0))");
  testAsc(
      "array_sort",
      "(x, y) -> if(length(x) < length(y), -1, if(length(x) = length(y), 0, 1))");

  // Different ways to sort by length descending.
  testDesc("array_sort", "x -> length(x) * -1");
  testDesc("array_sort_desc", "x -> length(x)");
  testDesc(
      "array_sort",
      "(x, y) -> if(length(x) < length(y), 1, if(length(x) > length(y), -1, 0))");
  testDesc(
      "array_sort",
      "(x, y) -> if(length(x) < length(y), 1, if(length(x) = length(y), 0, -1))");
}

TEST_F(ArraySortTest, unsupporteLambda) {
  auto data = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3, 4]",
          "[1, 2, 3]",
      }),
  });

  VELOX_ASSERT_THROW(
      evaluate("array_sort(c0, (a, b) -> 0)", data),
      "array_sort with comparator lambda that cannot be rewritten into a transform is not supported");
}

TEST_F(ArraySortTest, failOnMapTypeSort) {
  static const std::string kErrorMessage =
      "Scalar function signature is not supported"_sv;
  auto data = makeRowVector({BaseVector::createNullConstant(
      ARRAY(MAP(BIGINT(), VARCHAR())), 8, pool())});
  auto testFail = [&](const std::string& name) {
    VELOX_ASSERT_THROW(
        evaluate(fmt::format("{}(c0, x -> x)", name), data), kErrorMessage);
    VELOX_ASSERT_THROW(
        evaluate(fmt::format("{}(c0)", name), data), kErrorMessage);
  };

  testFail("array_sort");
  testFail("array_sort_desc");
}

TEST_F(ArraySortTest, failOnArrayNullCompare) {
  auto baseVector = makeArrayVectorFromJson<int32_t>({
      "[null, 1]",
      "[1, 1]",
      "[2, 2]",
      "[2, null]",
      "[4, 4]",
      "[5, null]",
      "null",
  });
  static const std::string kErrorMessage = "Ordering nulls is not supported";

  // [2, null] vs [4, 4], [5, null] vs null no throw.
  const auto noNullCompareBatch = makeRowVector({
      makeArrayVector({3, 5}, baseVector),
  });

  // [null, 1] vs [1, 1] throws.
  auto nullCompareBatch1 = makeRowVector({
      makeArrayVector({0, 3, 5}, baseVector),
  });

  // [2, 2] vs [2, null] throws.
  auto nullCompareBatch2 = makeRowVector({
      makeArrayVector({1, 4}, baseVector),
  });

  for (const auto& name : {"array_sort", "array_sort_desc"}) {
    evaluate(fmt::format("{}(c0)", name), noNullCompareBatch);
    VELOX_ASSERT_THROW(
        evaluate(fmt::format("{}(c0)", name), nullCompareBatch1),
        kErrorMessage);
    VELOX_ASSERT_THROW(
        evaluate(fmt::format("{}(c0)", name), nullCompareBatch2),
        kErrorMessage);
  }

  {
    auto expected = makeArrayVector({2, 3, 5}, baseVector);
    expected->setNull(0, true);
    assertEqualVectors(
        expected, evaluate("try(array_sort(c0))", nullCompareBatch1));
  }

  {
    auto expected = makeArrayVector({3, 4}, baseVector);
    expected->setNull(0, true);
    assertEqualVectors(
        expected, evaluate("try(array_sort(c0))", nullCompareBatch2));
  }
}

TEST_F(ArraySortTest, failOnRowNullCompare) {
  auto baseVector = makeRowVector({
      makeNullableFlatVector<int32_t>({std::nullopt, 1, 2, 2, 4, 5, 0}),
      makeNullableFlatVector<int32_t>(
          {1, 1, 2, std::nullopt, 4, std::nullopt, 0}),
  });
  baseVector->setNull(6, true);
  static const std::string kErrorMessage = "Ordering nulls is not supported";

  // (2, null) vs (4, 4), (5, null) vs null no throw.
  const auto noNullCompareBatch = makeRowVector({
      makeArrayVector({3, 5}, baseVector),
  });

  // (null, 1) vs (1, 1) throws.
  auto nullCompareBatch1 = makeRowVector({
      makeArrayVector({0, 3, 5}, baseVector),
  });

  // (2, 2) vs (2, null) throws.
  auto nullCompareBatch2 = makeRowVector({
      makeArrayVector({1, 4}, baseVector),
  });

  for (const auto& name : {"array_sort", "array_sort_desc"}) {
    evaluate(fmt::format("{}(c0)", name), noNullCompareBatch);
    VELOX_ASSERT_THROW(
        evaluate(fmt::format("{}(c0)", name), nullCompareBatch1),
        kErrorMessage);
    VELOX_ASSERT_THROW(
        evaluate(fmt::format("{}(c0)", name), nullCompareBatch2),
        kErrorMessage);
  }

  {
    auto expected = makeArrayVector({2, 3, 5}, baseVector);
    expected->setNull(0, true);
    assertEqualVectors(
        expected, evaluate("try(array_sort(c0))", nullCompareBatch1));
  }

  {
    auto expected = makeArrayVector({3, 4}, baseVector);
    expected->setNull(0, true);
    assertEqualVectors(
        expected, evaluate("try(array_sort(c0))", nullCompareBatch2));
  }
}

TEST_F(ArraySortTest, timestampWithTimezone) {
  auto testArraySort =
      [this](
          const std::vector<std::optional<int64_t>>& inputArray,
          const std::vector<std::optional<int64_t>>& expectedAscArray,
          const std::vector<std::optional<int64_t>>& expectedDescArray) {
        const auto input = makeRowVector({makeArrayVector(
            {0},
            makeNullableFlatVector<int64_t>(
                inputArray, TIMESTAMP_WITH_TIME_ZONE()))});
        const auto expectedAsc = makeArrayVector(
            {0},
            makeNullableFlatVector<int64_t>(
                expectedAscArray, TIMESTAMP_WITH_TIME_ZONE()));
        const auto expectedDesc = makeArrayVector(
            {0},
            makeNullableFlatVector<int64_t>(
                expectedDescArray, TIMESTAMP_WITH_TIME_ZONE()));

        auto resultAsc = evaluate("array_sort(c0)", input);
        assertEqualVectors(expectedAsc, resultAsc);

        auto resultDesc = evaluate("array_sort_desc(c0)", input);
        assertEqualVectors(expectedDesc, resultDesc);
      };

  testArraySort(
      {pack(2, 0), pack(1, 1), pack(0, 2)},
      {pack(0, 2), pack(1, 1), pack(2, 0)},
      {pack(2, 0), pack(1, 1), pack(0, 2)});
  testArraySort(
      {pack(0, 0), pack(1, 1), pack(2, 2)},
      {pack(0, 0), pack(1, 1), pack(2, 2)},
      {pack(2, 2), pack(1, 1), pack(0, 0)});
  testArraySort(
      {pack(0, 0), pack(0, 1), pack(0, 2)},
      {pack(0, 0), pack(0, 1), pack(0, 2)},
      {pack(0, 0), pack(0, 1), pack(0, 2)});
  testArraySort(
      {pack(1, 0), pack(0, 1), pack(2, 2)},
      {pack(0, 1), pack(1, 0), pack(2, 2)},
      {pack(2, 2), pack(1, 0), pack(0, 1)});
  testArraySort(
      {std::nullopt, pack(1, 0), pack(0, 1), pack(2, 2)},
      {pack(0, 1), pack(1, 0), pack(2, 2), std::nullopt},
      {pack(2, 2), pack(1, 0), pack(0, 1), std::nullopt});
  testArraySort(
      {std::nullopt, std::nullopt, pack(1, 2), pack(0, 1), pack(2, 0)},
      {pack(0, 1), pack(1, 2), pack(2, 0), std::nullopt, std::nullopt},
      {pack(2, 0), pack(1, 2), pack(0, 1), std::nullopt, std::nullopt});
  testArraySort(
      {std::nullopt, pack(1, 1), pack(0, 2), std::nullopt, pack(2, 0)},
      {pack(0, 2), pack(1, 1), pack(2, 0), std::nullopt, std::nullopt},
      {pack(2, 0), pack(1, 1), pack(0, 2), std::nullopt, std::nullopt});
  testArraySort(
      {pack(1, 1), std::nullopt, pack(0, 0), pack(2, 2), std::nullopt},
      {pack(0, 0), pack(1, 1), pack(2, 2), std::nullopt, std::nullopt},
      {pack(2, 2), pack(1, 1), pack(0, 0), std::nullopt, std::nullopt});
  testArraySort(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

template <typename T>
struct TimeZoneFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& ts) {
    result = unpackZoneKeyId(*ts);
  }
};

TEST_F(ArraySortTest, timestampWithTimezoneWithLambda) {
  registerFunction<TimeZoneFunction, int64_t, TimestampWithTimezone>(
      {"timezone"});

  auto testArraySort =
      [this](
          const std::vector<std::optional<int64_t>>& inputArray,
          const std::vector<std::optional<int64_t>>& expectedAscArray,
          const std::vector<std::optional<int64_t>>& expectedDescArray) {
        const auto input = makeRowVector({makeArrayVector(
            {0},
            makeNullableFlatVector<int64_t>(
                inputArray, TIMESTAMP_WITH_TIME_ZONE()))});
        const auto expectedAsc = makeArrayVector(
            {0},
            makeNullableFlatVector<int64_t>(
                expectedAscArray, TIMESTAMP_WITH_TIME_ZONE()));
        const auto expectedDesc = makeArrayVector(
            {0},
            makeNullableFlatVector<int64_t>(
                expectedDescArray, TIMESTAMP_WITH_TIME_ZONE()));

        auto resultAsc = evaluate("array_sort(c0, x -> timezone(x))", input);
        assertEqualVectors(expectedAsc, resultAsc);

        auto resultDesc =
            evaluate("array_sort_desc(c0, x -> timezone(x))", input);
        assertEqualVectors(expectedDesc, resultDesc);
      };

  testArraySort(
      {pack(2, 0), pack(1, 1), pack(0, 2)},
      {pack(2, 0), pack(1, 1), pack(0, 2)},
      {pack(0, 2), pack(1, 1), pack(2, 0)});
  testArraySort(
      {pack(0, 0), pack(1, 1), pack(2, 2)},
      {pack(0, 0), pack(1, 1), pack(2, 2)},
      {pack(2, 2), pack(1, 1), pack(0, 0)});
  testArraySort(
      {pack(0, 0), pack(0, 1), pack(0, 2)},
      {pack(0, 0), pack(0, 1), pack(0, 2)},
      {pack(0, 2), pack(0, 1), pack(0, 0)});
  testArraySort(
      {pack(1, 0), pack(0, 1), pack(2, 2)},
      {pack(1, 0), pack(0, 1), pack(2, 2)},
      {pack(2, 2), pack(0, 1), pack(1, 0)});
  testArraySort(
      {std::nullopt, pack(1, 0), pack(0, 1), pack(2, 2)},
      {pack(1, 0), pack(0, 1), pack(2, 2), std::nullopt},
      {pack(2, 2), pack(0, 1), pack(1, 0), std::nullopt});
  testArraySort(
      {std::nullopt, std::nullopt, pack(1, 2), pack(0, 1), pack(2, 0)},
      {pack(2, 0), pack(0, 1), pack(1, 2), std::nullopt, std::nullopt},
      {pack(1, 2), pack(0, 1), pack(2, 0), std::nullopt, std::nullopt});
  testArraySort(
      {std::nullopt, pack(1, 1), pack(0, 2), std::nullopt, pack(2, 0)},
      {pack(2, 0), pack(1, 1), pack(0, 2), std::nullopt, std::nullopt},
      {pack(0, 2), pack(1, 1), pack(2, 0), std::nullopt, std::nullopt});
  testArraySort(
      {pack(1, 1), std::nullopt, pack(0, 0), pack(2, 2), std::nullopt},
      {pack(0, 0), pack(1, 1), pack(2, 2), std::nullopt, std::nullopt},
      {pack(2, 2), pack(1, 1), pack(0, 0), std::nullopt, std::nullopt});
  testArraySort(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(ArraySortTest, floatingPointExtremes) {
  testFloatingPoint<float>();
  testFloatingPoint<double>();
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    ArraySortTest,
    ArraySortTest,
    testing::ValuesIn(kSupportedTypes));
} // namespace
