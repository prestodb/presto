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

#include <fmt/format.h>

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
    TypeKind::MAP,
    TypeKind::ARRAY,
    TypeKind::ROW};

using TestMapType = std::vector<std::pair<int32_t, std::optional<int32_t>>>;
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

  const MapVector* getMapVector() {
    return dynamic_cast<MapVector*>(dataVectorsByType_[TypeKind::MAP].get());
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

  MapVectorPtr buildMapVector() {
    return makeMapVector<int32_t, int32_t>(
        numValues_,
        [&](vector_size_t /*row*/) { return 1; },
        [&](vector_size_t row) { return row; },
        [&](vector_size_t row) { return row; });
  }

  template <typename T>
  void test() {
    struct {
      const RowVectorPtr inputVector;
      const VectorPtr expectedResult;

      const std::string debugString() const {
        return fmt::format(
            "\ntype: {}\ninputVector: {}\nexpectedResult: {}",
            GetParam(),
            inputVector->toString(0, inputVector->size()),
            expectedResult->toString(0, expectedResult->size()));
      }
    } testSettings[] = {
        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {dataAt<T>(2), dataAt<T>(1), dataAt<T>(0)}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)})},

        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)})},

        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {dataAt<T>(0), dataAt<T>(0), dataAt<T>(0)}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0), dataAt<T>(0), dataAt<T>(0)})},

        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {dataAt<T>(1), dataAt<T>(0), dataAt<T>(2)}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)})},

        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {std::nullopt, dataAt<T>(1), dataAt<T>(0), dataAt<T>(2)}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0), dataAt<T>(1), dataAt<T>(2), std::nullopt})},

        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {std::nullopt,
              std::nullopt,
              dataAt<T>(1),
              dataAt<T>(0),
              dataAt<T>(2)}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0),
             dataAt<T>(1),
             dataAt<T>(2),
             std::nullopt,
             std::nullopt})},

        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {std::nullopt,
              dataAt<T>(1),
              dataAt<T>(0),
              std::nullopt,
              dataAt<T>(2)}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0),
             dataAt<T>(1),
             dataAt<T>(2),
             std::nullopt,
             std::nullopt})},

        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {dataAt<T>(1),
              std::nullopt,
              dataAt<T>(0),
              dataAt<T>(2),
              std::nullopt}))}),
         arrayVector(std::vector<std::optional<T>>{
             dataAt<T>(0),
             dataAt<T>(1),
             dataAt<T>(2),
             std::nullopt,
             std::nullopt})},
        {makeRowVector({arrayVector(std::vector<std::optional<T>>(
             {std::nullopt,
              std::nullopt,
              std::nullopt,
              std::nullopt,
              std::nullopt}))}),
         arrayVector(std::vector<std::optional<T>>{
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt})}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      auto actualResult =
          evaluate<ArrayVector>("array_sort(c0)", testData.inputVector);
      assertEqualVectors(testData.expectedResult, actualResult);
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
      case TypeKind::MAP:
        test<TestMapType>();
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
TestMapType ArraySortTest::dataAt<TestMapType>(vector_size_t index) {
  EXPECT_LT(index, numValues_);
  const int32_t key =
      getMapVector()->mapKeys()->asFlatVector<int32_t>()->valueAt(index);
  const std::optional<int32_t> value =
      getMapVector()->mapValues()->asFlatVector<int32_t>()->valueAt(index);
  return TestMapType({std::pair{key, value}});
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
ArrayVectorPtr ArraySortTest::arrayVector<TestMapType>(
    const std::vector<std::optional<TestMapType>>& inputValues) {
  std::vector<std::vector<std::optional<TestMapType>>> inputVectors;
  inputVectors.reserve(numVectors_);
  for (int i = 0; i < numVectors_; ++i) {
    inputVectors.push_back(inputValues);
  }
  return makeArrayOfMapVector<int32_t, int32_t>(inputVectors);
}

template <>
ArrayVectorPtr ArraySortTest::arrayVector<TestArrayType>(
    const std::vector<std::optional<TestArrayType>>& inputValues) {
  std::vector<std::vector<std::optional<TestArrayType>>> inputVectors;
  inputVectors.reserve(numVectors_);
  for (int i = 0; i < numVectors_; ++i) {
    inputVectors.push_back(inputValues);
  }
  return makeNestedArrayVector<StringView>(inputVectors);
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
      case TypeKind::MAP:
        dataVectorsByType_.emplace(type, buildMapVector());
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

TEST_P(ArraySortTest, constant) {
  if (GetParam() != TypeKind::BIGINT) {
    GTEST_SKIP() << "Skipping constant test for non-bigint type";
  }

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

VELOX_INSTANTIATE_TEST_SUITE_P(
    ArraySortTest,
    ArraySortTest,
    testing::ValuesIn(kSupportedTypes));
} // namespace
