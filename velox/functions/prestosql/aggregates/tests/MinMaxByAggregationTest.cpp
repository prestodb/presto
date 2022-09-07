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
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <fmt/format.h>

using namespace facebook::velox::exec::test;
using facebook::velox::VectorFuzzer;
namespace facebook::velox::aggregate::test {

namespace {

struct TestParam {
  // Specify the value type of minmax_by in test.
  TypeKind valueType;
  // Specify the comparison value type of minmax_by in test.
  TypeKind comparisonType;
};

const std::unordered_set<TypeKind> kSupportedTypes = {
    TypeKind::TINYINT,
    TypeKind::SMALLINT,
    TypeKind::INTEGER,
    TypeKind::BIGINT,
    TypeKind::REAL,
    TypeKind::DOUBLE,
    TypeKind::VARCHAR,
    TypeKind::DATE};

std::vector<TestParam> getTestParams() {
  std::vector<TestParam> params;
  for (TypeKind valueType : kSupportedTypes) {
    for (TypeKind comparisonType : kSupportedTypes) {
      params.push_back({valueType, comparisonType});
    }
  }
  return params;
}

#define EXECUTE_TEST_BY_VALUE_TYPE(testFunc, valueType)              \
  do {                                                               \
    switch (GetParam().comparisonType) {                             \
      case TypeKind::TINYINT:                                        \
        testFunc<valueType, int8_t>();                               \
        break;                                                       \
      case TypeKind::SMALLINT:                                       \
        testFunc<valueType, int16_t>();                              \
        break;                                                       \
      case TypeKind::INTEGER:                                        \
        testFunc<valueType, int32_t>();                              \
        break;                                                       \
      case TypeKind::BIGINT:                                         \
        testFunc<valueType, int64_t>();                              \
        break;                                                       \
      case TypeKind::REAL:                                           \
        testFunc<valueType, float>();                                \
        break;                                                       \
      case TypeKind::DOUBLE:                                         \
        testFunc<valueType, double>();                               \
        break;                                                       \
      case TypeKind::VARCHAR:                                        \
        testFunc<valueType, StringView>();                           \
        break;                                                       \
      case TypeKind::DATE:                                           \
        testFunc<valueType, Date>();                                 \
        break;                                                       \
      default:                                                       \
        LOG(FATAL) << "Unsupported comparison type of minmax_by(): " \
                   << mapTypeKindToName(GetParam().comparisonType);  \
    }                                                                \
  } while (0);

#define EXECUTE_TEST(testFunc)                                  \
  do {                                                          \
    switch (GetParam().valueType) {                             \
      case TypeKind::TINYINT:                                   \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int8_t);           \
        break;                                                  \
      case TypeKind::SMALLINT:                                  \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int16_t);          \
        break;                                                  \
      case TypeKind::INTEGER:                                   \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int32_t);          \
        break;                                                  \
      case TypeKind::BIGINT:                                    \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int64_t);          \
        break;                                                  \
      case TypeKind::REAL:                                      \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, float);            \
        break;                                                  \
      case TypeKind::DOUBLE:                                    \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, double);           \
        break;                                                  \
      case TypeKind::VARCHAR:                                   \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, StringView);       \
        break;                                                  \
      case TypeKind::DATE:                                      \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, Date);             \
        break;                                                  \
      default:                                                  \
        LOG(FATAL) << "Unsupported value type of minmax_by(): " \
                   << mapTypeKindToName(GetParam().valueType);  \
    }                                                           \
  } while (0);

class MinMaxByAggregationTestBase : public AggregationTestBase {
 protected:
  MinMaxByAggregationTestBase() : numValues_(6) {}

  void SetUp() override;

  // Build a flat vector with numeric native type of T. The value in the
  // returned flat vector is in ascending order.
  template <typename T>
  FlatVectorPtr<T> buildDataVector(
      vector_size_t size,
      folly::Range<const int*> values = {}) {
    if (values.empty()) {
      return makeFlatVector<T>(size, [](auto row) { return row - 3; });
    } else {
      VELOX_CHECK_EQ(values.size(), size);
      return makeFlatVector<T>(size, [&](auto row) { return values[row]; });
    }
  }

  template <typename T>
  const FlatVector<T>* getDataVector() {
    return dataVectorsByType_[CppToType<T>::typeKind]
        ->template asFlatVector<T>();
  }

  template <typename T>
  T dataAt(vector_size_t index) {
    EXPECT_LT(index, numValues_);
    return getDataVector<T>()->valueAt(index);
  }

  // Get the column name in 'rowType_' for the given 'kind'.
  std::string getColumnName(TypeKind kind) const {
    for (int childIndex = 0; childIndex < rowType_->size(); ++childIndex) {
      const auto& childType = rowType_->childAt(childIndex);
      if (childType->kind() == kind) {
        return rowType_->nameOf(childIndex);
      }
    }
    VELOX_FAIL(
        "Type {} is not found in rowType_: ",
        mapTypeKindToName(kind),
        rowType_->toString());
  }

  VectorPtr buildDataVector(
      TypeKind kind,
      vector_size_t size,
      folly::Range<const int*> values);

  const RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
          {TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           DATE()})};
  // Specify the number of values in each typed data vector in
  // 'dataVectorsByType_'.
  const int numValues_;
  std::unordered_map<TypeKind, VectorPtr> dataVectorsByType_;
  std::vector<RowVectorPtr> rowVectors_;
};

// Build a flat vector with StringView. The value in the returned flat vector
// is in ascending order.
template <>
FlatVectorPtr<StringView> MinMaxByAggregationTestBase::buildDataVector(
    vector_size_t size,
    folly::Range<const int*> values) {
  std::string value;
  if (values.empty()) {
    return makeFlatVector<StringView>(
        size, [&, maxValueLen = (int)std::ceil((double)size / 26.0)](auto row) {
          const int valueLen = row % maxValueLen + 1;
          const char c = 'a' + row / maxValueLen;
          value = std::string(valueLen, c);
          return StringView(value);
        });
  } else {
    VELOX_CHECK_EQ(values.size(), size);
    return makeFlatVector<StringView>(size, [&](auto row) {
      value = std::to_string(values[row]);
      return StringView(value);
    });
  }
}

VectorPtr MinMaxByAggregationTestBase::buildDataVector(
    TypeKind kind,
    vector_size_t size,
    folly::Range<const int*> values) {
  switch (kind) {
    case TypeKind::TINYINT:
      return buildDataVector<int8_t>(size, values);
    case TypeKind::SMALLINT:
      return buildDataVector<int16_t>(size, values);
    case TypeKind::INTEGER:
      return buildDataVector<int32_t>(size, values);
    case TypeKind::BIGINT:
      return buildDataVector<int64_t>(size, values);
    case TypeKind::REAL:
      return buildDataVector<float>(size, values);
    case TypeKind::DOUBLE:
      return buildDataVector<double>(size, values);
    case TypeKind::VARCHAR:
      return buildDataVector<StringView>(size, values);
    case TypeKind::DATE:
      return buildDataVector<Date>(size, values);
    default:
      LOG(FATAL) << "Unsupported value/comparison type of minmax_by(): "
                 << mapTypeKindToName(kind);
  }
}

void MinMaxByAggregationTestBase::SetUp() {
  AggregationTestBase::SetUp();
  AggregationTestBase::disallowInputShuffle();

  for (const TypeKind type : kSupportedTypes) {
    switch (type) {
      case TypeKind::TINYINT:
        dataVectorsByType_.emplace(type, buildDataVector<int8_t>(numValues_));
        break;
      case TypeKind::SMALLINT:
        dataVectorsByType_.emplace(type, buildDataVector<int16_t>(numValues_));
        break;
      case TypeKind::INTEGER:
        dataVectorsByType_.emplace(type, buildDataVector<int32_t>(numValues_));
        break;
      case TypeKind::BIGINT:
        dataVectorsByType_.emplace(type, buildDataVector<int64_t>(numValues_));
        break;
      case TypeKind::REAL:
        dataVectorsByType_.emplace(type, buildDataVector<float>(numValues_));
        break;
      case TypeKind::DOUBLE:
        dataVectorsByType_.emplace(type, buildDataVector<double>(numValues_));
        break;
      case TypeKind::DATE:
        dataVectorsByType_.emplace(type, buildDataVector<Date>(numValues_));
        break;
      case TypeKind::VARCHAR:
        dataVectorsByType_.emplace(
            type, buildDataVector<StringView>(numValues_));
        break;
      default:
        LOG(FATAL) << "Unsupported data type: " << mapTypeKindToName(type);
    }
  }
  ASSERT_EQ(dataVectorsByType_.size(), kSupportedTypes.size());
  rowVectors_ = makeVectors(rowType_, 5, 10);
  createDuckDbTable(rowVectors_);
};

class MinMaxByGlobalByAggregationTest
    : public MinMaxByAggregationTestBase,
      public testing::WithParamInterface<TestParam> {
 public:
  MinMaxByGlobalByAggregationTest() : MinMaxByAggregationTestBase() {}

 protected:
  void testGlobalAggregation(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& aggName,
      const std::string& valueColumnName,
      const std::string& comparisonColumnName) {
    const std::string funcName = aggName == kMaxBy ? "max" : "min";
    const std::string verifyDuckDbSql = fmt::format(
        "SELECT {} FROM tmp WHERE {} = ( SELECT {} ({}) FROM tmp) LIMIT 1",
        valueColumnName,
        comparisonColumnName,
        funcName,
        comparisonColumnName);
    const std::string aggregate = fmt::format(
        "{}({}, {})", aggName, valueColumnName, comparisonColumnName);
    SCOPED_TRACE(
        fmt::format("{}\nverifyDuckDbSql: {}", aggregate, verifyDuckDbSql));
    testAggregations(vectors, {}, {aggregate}, {}, verifyDuckDbSql);
  }

  template <typename T, typename U>
  void minByGlobalByTest() {
    struct {
      const RowVectorPtr inputRowVector;
      const std::string verifyDuckDbSql;

      const std::string debugString() const {
        return fmt::format(
            "\ninputRowVector: {}\n{}\nverifyDuckDbSql: {}",
            inputRowVector->toString(),
            inputRowVector->toString(0, inputRowVector->size()),
            verifyDuckDbSql);
      }
    } testSettings[] = {
        // Const vector cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 5),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        // All null cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 10),
              makeNullConstant(GetParam().comparisonType, 10)}),
         "SELECT null"},

        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 10),
              makeConstant(std::optional<U>(dataAt<U>(0)), 10)}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(0), std::nullopt, dataAt<U>(1), dataAt<U>(2)})}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(0), std::nullopt, dataAt<U>(1), dataAt<U>(2)})}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(4))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(0), dataAt<U>(3)})}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), std::nullopt, dataAt<T>(3), dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(0), dataAt<U>(3)})}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(3))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      testAggregations(
          {testData.inputRowVector},
          {},
          {"min_by(c0, c1)"},
          {},
          testData.verifyDuckDbSql);
    }
  }

  template <typename T, typename U>
  void maxByGlobalByTest() {
    struct {
      const RowVectorPtr inputRowVector;
      const std::string verifyDuckDbSql;

      const std::string debugString() const {
        return fmt::format(
            "\ninputRowVector: {}\n{}\nverifyDuckDbSql: {}",
            inputRowVector->toString(),
            inputRowVector->toString(0, inputRowVector->size()),
            verifyDuckDbSql);
      }
    } testSettings[] = {
        // Const vector cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 5),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        // All null cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 10),
              makeNullConstant(GetParam().comparisonType, 10)}),
         "SELECT null"},

        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 10),
              makeConstant(std::optional<U>(dataAt<U>(0)), 10)}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(0), std::nullopt, dataAt<U>(1), dataAt<U>(2)})}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(4))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(3), dataAt<U>(0)})}),
         "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), std::nullopt, dataAt<T>(3), dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(3), dataAt<U>(0)})}),
         fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(3))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      testAggregations(
          {testData.inputRowVector},
          {},
          {"max_by(c0, c1)"},
          {},
          testData.verifyDuckDbSql);
    }
  }
};

TEST_P(MinMaxByGlobalByAggregationTest, minByFinalGlobalBy) {
  EXECUTE_TEST(minByGlobalByTest);
}

TEST_P(MinMaxByGlobalByAggregationTest, maxByFinalGlobalBy) {
  EXECUTE_TEST(maxByGlobalByTest);
}

TEST_P(MinMaxByGlobalByAggregationTest, randomMinByGlobalBy) {
  testGlobalAggregation(
      rowVectors_,
      kMinBy,
      getColumnName(GetParam().valueType),
      getColumnName(GetParam().comparisonType));
}

TEST_P(MinMaxByGlobalByAggregationTest, randomMaxByGlobalBy) {
  testGlobalAggregation(
      rowVectors_,
      kMaxBy,
      getColumnName(GetParam().valueType),
      getColumnName(GetParam().comparisonType));
}

TEST_P(
    MinMaxByGlobalByAggregationTest,
    randomMaxByGlobalByWithDistinctCompareValue) {
  // Enable disk spilling test with distinct comparison values.
  AggregationTestBase::allowInputShuffle();

  auto rowType =
      ROW({"c0", "c1"},
          {fromKindToScalerType(GetParam().valueType),
           fromKindToScalerType(GetParam().comparisonType)});

  const bool isMallInt = GetParam().comparisonType == TypeKind::TINYINT ||
      GetParam().comparisonType == TypeKind::SMALLINT;
  const int kBatchSize = isMallInt ? 1 << 4 : 1 << 10;
  const int kNumBatches = isMallInt ? 4 : 10;
  const int kNumValues = kNumBatches * kBatchSize;
  std::vector<int> values(kNumValues);
  for (int i = 0; i < kNumValues; ++i) {
    values[i] = i;
  }
  std::shuffle(values.begin(), values.end(), std::default_random_engine(1));
  std::vector<RowVectorPtr> rowVectors;
  const auto* rawValues = values.data();

  VectorFuzzer::Options options;
  options.nullRatio = 0;
  options.vectorSize = kBatchSize;
  VectorFuzzer fuzzer(options, pool_.get(), 0);

  for (int i = 0; i < kNumBatches; ++i) {
    auto valueVector = fuzzer.fuzz(fromKindToScalerType(GetParam().valueType));
    auto comparisonVector = buildDataVector(
        GetParam().comparisonType,
        kBatchSize,
        folly::range<const int*>(rawValues, rawValues + kBatchSize));
    rawValues += kBatchSize;
    rowVectors.push_back(makeRowVector({valueVector, comparisonVector}));
  }
  createDuckDbTable(rowVectors);

  testGlobalAggregation(rowVectors, kMaxBy, "c0", "c1");

  testGlobalAggregation(rowVectors, kMaxBy, "c0", "c1");
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MinMaxByAggregationTest,
    MinMaxByGlobalByAggregationTest,
    testing::ValuesIn(getTestParams()));

class MinMaxByGroupByAggregationTest
    : public MinMaxByAggregationTestBase,
      public testing::WithParamInterface<TestParam> {
 public:
  MinMaxByGroupByAggregationTest() : MinMaxByAggregationTestBase() {}

  void testGroupByAggregation(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& aggName,
      const std::string& valueColumnName,
      const std::string& comparisonColumnName,
      const std::string& groupByColumnName) {
    const std::string funcName = aggName == kMaxBy ? "max" : "min";
    const std::string aggregate = fmt::format(
        "{}({}, {})", aggName, valueColumnName, comparisonColumnName);
    const std::string verifyDuckDbSql = fmt::format(
        "SELECT {}, {} FROM tmp GROUP BY {}",
        groupByColumnName,
        aggregate,
        groupByColumnName);
    SCOPED_TRACE(fmt::format(
        "{} GROUP BY {}\nverifyDuckDbSql: {}",
        aggregate,
        groupByColumnName,
        verifyDuckDbSql));
    testAggregations(
        vectors, {groupByColumnName}, {aggregate}, {}, verifyDuckDbSql);
  }

  template <typename T, typename U>
  void testMinByGroupBy() {
    struct {
      const RowVectorPtr inputRowVector;
      const std::string verifyDuckDbSql;

      const std::string debugString() const {
        return fmt::format(
            "\ninputRowVector: {}\n{}\nverifyDuckDbSql: {}",
            inputRowVector->toString(),
            inputRowVector->toString(0, inputRowVector->size()),
            verifyDuckDbSql);
      }
    } testSettings[] = {
        // Const vector cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 6),
              makeConstant(std::optional<U>(dataAt<U>(0)), 6),
              makeConstant(std::optional<int32_t>(dataAt<int32_t>(0)), 6)}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', '{}')) AS t",
             dataAt<int32_t>(0),
             dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(0),
                   dataAt<T>(1),
                   dataAt<T>(2),
                   dataAt<T>(3),
                   dataAt<T>(4)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 6),
              makeConstant(std::optional<int32_t>(dataAt<int32_t>(0)), 6)}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL)) AS t", dataAt<int32_t>(0))},

        // All null cases.
        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 6),
              makeNullableFlatVector<U>(
                  {dataAt<U>(4),
                   dataAt<U>(5),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(2),
                   dataAt<U>(0)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL), ('{}', NULL), ('{}', NULL)) AS t",
             dataAt<int32_t>(0),
             dataAt<int32_t>(1),
             dataAt<int32_t>(2))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(2),
                   std::nullopt,
                   dataAt<T>(1),
                   std::nullopt,
                   dataAt<T>(0)}),
              makeNullConstant(GetParam().valueType, 6),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL), ('{}', NULL), ('{}', NULL)) AS t",
             dataAt<int32_t>(0),
             dataAt<int32_t>(1),
             dataAt<int32_t>(2))},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(2),
                   std::nullopt,
                   dataAt<T>(1),
                   std::nullopt,
                   dataAt<T>(0)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(4),
                   dataAt<U>(5),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(2),
                   dataAt<U>(0)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(0)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', '{}'), ('{}', NULL), ('{}', '{}')) AS t",
             dataAt<int32_t>(0),
             dataAt<T>(0),
             dataAt<int32_t>(1),
             dataAt<int32_t>(2),
             dataAt<T>(1))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(2),
                   std::nullopt,
                   dataAt<T>(1),
                   std::nullopt,
                   dataAt<T>(0)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(4),
                   dataAt<U>(5),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(2),
                   dataAt<U>(0)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL), ('{}', '{}'), ('{}', '{}')) AS t",
             dataAt<int32_t>(0),
             dataAt<int32_t>(1),
             dataAt<T>(1),
             dataAt<int32_t>(2),
             dataAt<T>(0))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      testAggregations(
          {testData.inputRowVector},
          {"c2"},
          {"min_by(c0, c1)"},
          {},
          testData.verifyDuckDbSql);
    }
  }

  template <typename T, typename U>
  void testMaxByGroupBy() {
    struct {
      const RowVectorPtr inputRowVector;
      const std::string verifyDuckDbSql;

      const std::string debugString() const {
        return fmt::format(
            "\ninputRowVector: {}\n{}\nverifyDuckDbSql: {}",
            inputRowVector->toString(),
            inputRowVector->toString(0, inputRowVector->size()),
            verifyDuckDbSql);
      }
    } testSettings[] = {
        // Const vector cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 6),
              makeConstant(std::optional<U>(dataAt<U>(0)), 6),
              makeConstant(std::optional<int32_t>(dataAt<int32_t>(0)), 6)}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', '{}')) AS t",
             dataAt<int32_t>(0),
             dataAt<T>(0))},
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(0),
                   dataAt<T>(1),
                   dataAt<T>(2),
                   dataAt<T>(3),
                   dataAt<T>(4)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 6),
              makeConstant(std::optional<int32_t>(dataAt<int32_t>(0)), 6)}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL)) AS t", dataAt<int32_t>(0))},

        // All null cases.
        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 6),
              makeNullableFlatVector<U>(
                  {dataAt<U>(4),
                   dataAt<U>(5),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(2),
                   dataAt<U>(0)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL), ('{}', NULL), ('{}', NULL)) AS t",
             dataAt<int32_t>(0),
             dataAt<int32_t>(1),
             dataAt<int32_t>(2))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(2),
                   std::nullopt,
                   dataAt<T>(1),
                   std::nullopt,
                   dataAt<T>(0)}),
              makeNullConstant(GetParam().valueType, 6),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL), ('{}', NULL), ('{}', NULL)) AS t",
             dataAt<int32_t>(0),
             dataAt<int32_t>(1),
             dataAt<int32_t>(2))},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(2),
                   std::nullopt,
                   dataAt<T>(1),
                   std::nullopt,
                   dataAt<T>(0)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(4),
                   dataAt<U>(5),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(2),
                   dataAt<U>(0)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(0)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL), ('{}', '{}'), ('{}', '{}')) AS t",
             dataAt<int32_t>(0),
             dataAt<int32_t>(1),
             dataAt<T>(2),
             dataAt<int32_t>(2),
             dataAt<T>(1))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(2),
                   std::nullopt,
                   dataAt<T>(1),
                   std::nullopt,
                   dataAt<T>(0)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(5),
                   dataAt<U>(4),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(0),
                   dataAt<U>(2)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "SELECT * FROM( VALUES ('{}', NULL), ('{}', '{}'), ('{}', '{}')) AS t",
             dataAt<int32_t>(0),
             dataAt<int32_t>(1),
             dataAt<T>(1),
             dataAt<int32_t>(2),
             dataAt<T>(0))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      testAggregations(
          {testData.inputRowVector},
          {"c2"},
          {"max_by(c0, c1)"},
          {},
          testData.verifyDuckDbSql);
    }
  }
};

TEST_P(MinMaxByGroupByAggregationTest, minByPartialGroupBy) {
  EXECUTE_TEST(testMinByGroupBy);
}

TEST_P(MinMaxByGroupByAggregationTest, maxByPartialGroupBy) {
  EXECUTE_TEST(testMaxByGroupBy);
}

TEST_P(MinMaxByGroupByAggregationTest, minByFinalGroupBy) {
  EXECUTE_TEST(testMinByGroupBy);
}

TEST_P(MinMaxByGroupByAggregationTest, maxByFinalGroupBy) {
  EXECUTE_TEST(testMaxByGroupBy);
}

TEST_P(MinMaxByGroupByAggregationTest, randomMinByGroupBy) {
  testGroupByAggregation(
      rowVectors_,
      kMinBy,
      getColumnName(GetParam().valueType),
      getColumnName(GetParam().comparisonType),
      getColumnName(TypeKind::INTEGER));
}

TEST_P(MinMaxByGroupByAggregationTest, randomMaxByGroupBy) {
  testGroupByAggregation(
      rowVectors_,
      kMaxBy,
      getColumnName(GetParam().valueType),
      getColumnName(GetParam().comparisonType),
      getColumnName(TypeKind::INTEGER));
}

TEST_P(
    MinMaxByGroupByAggregationTest,
    randomMinMaxByGroupByWithDistinctCompareValue) {
  // Enable disk spilling test with distinct comparison values.
  AggregationTestBase::allowInputShuffle();

  auto rowType =
      ROW({"c0", "c1", "c2"},
          {fromKindToScalerType(GetParam().valueType),
           fromKindToScalerType(GetParam().comparisonType),
           INTEGER()});

  const bool isMallInt = GetParam().comparisonType == TypeKind::TINYINT ||
      GetParam().comparisonType == TypeKind::SMALLINT;
  const int kBatchSize = isMallInt ? 1 << 4 : 1 << 10;
  const int kNumBatches = isMallInt ? 3 : 10;
  const int kNumValues = kNumBatches * kBatchSize;
  std::vector<int> values(kNumValues);
  for (int i = 0; i < kNumValues; ++i) {
    values[i] = i;
  }
  std::shuffle(values.begin(), values.end(), std::default_random_engine(1));
  std::vector<RowVectorPtr> rowVectors;
  const auto* rawValues = values.data();

  VectorFuzzer::Options options;
  options.nullRatio = 0;
  options.vectorSize = kBatchSize;
  VectorFuzzer fuzzer(options, pool_.get(), 0);

  for (int i = 0; i < kNumBatches; ++i) {
    auto valueVector = fuzzer.fuzz(fromKindToScalerType(GetParam().valueType));
    auto groupByVector = makeFlatVector<int32_t>(kBatchSize);
    auto comparisonVector = buildDataVector(
        GetParam().comparisonType,
        kBatchSize,
        folly::range<const int*>(rawValues, rawValues + kBatchSize));
    rawValues += kBatchSize;
    rowVectors.push_back(
        makeRowVector({valueVector, comparisonVector, groupByVector}));
  }
  createDuckDbTable(rowVectors);

  testGroupByAggregation(rowVectors, kMaxBy, "c0", "c1", "c2");

  testGroupByAggregation(rowVectors, kMaxBy, "c0", "c1", "c2");
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MinMaxByAggregationTest,
    MinMaxByGroupByAggregationTest,
    testing::ValuesIn(getTestParams()));

} // namespace
} // namespace facebook::velox::aggregate::test
