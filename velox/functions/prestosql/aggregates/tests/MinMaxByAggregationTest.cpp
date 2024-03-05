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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <fmt/format.h>

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;
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
    TypeKind::BOOLEAN,
    TypeKind::TINYINT,
    TypeKind::SMALLINT,
    TypeKind::INTEGER,
    TypeKind::BIGINT,
    TypeKind::REAL,
    TypeKind::DOUBLE,
    TypeKind::VARCHAR,
    TypeKind::TIMESTAMP};

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
      case TypeKind::BOOLEAN:                                        \
        testFunc<valueType, bool>();                                 \
        break;                                                       \
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
      case TypeKind::TIMESTAMP:                                      \
        testFunc<valueType, Timestamp>();                            \
        break;                                                       \
      default:                                                       \
        LOG(FATAL) << "Unsupported comparison type of minmax_by(): " \
                   << mapTypeKindToName(GetParam().comparisonType);  \
    }                                                                \
  } while (0);

#define EXECUTE_TEST(testFunc)                                  \
  do {                                                          \
    switch (GetParam().valueType) {                             \
      case TypeKind::BOOLEAN:                                   \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, bool);             \
        break;                                                  \
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
      case TypeKind::TIMESTAMP:                                 \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, Timestamp);        \
        break;                                                  \
      default:                                                  \
        LOG(FATAL) << "Unsupported value type of minmax_by(): " \
                   << mapTypeKindToName(GetParam().valueType);  \
    }                                                           \
  } while (0);

template <typename T>
struct ExtremeValueTrait : public std::numeric_limits<T> {};

template <typename T>
const T kMax = ExtremeValueTrait<T>::max();

template <typename T>
const T kLowest = ExtremeValueTrait<T>::lowest();

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
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
          {
              BOOLEAN(),
              TINYINT(),
              SMALLINT(),
              INTEGER(),
              BIGINT(),
              REAL(),
              DOUBLE(),
              VARCHAR(),
              DATE(),
              TIMESTAMP(),
          })};
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

template <>
FlatVectorPtr<Timestamp> MinMaxByAggregationTestBase::buildDataVector(
    vector_size_t size,
    folly::Range<const int*> values) {
  if (values.empty()) {
    return makeFlatVector<Timestamp>(
        size, [](auto row) { return Timestamp(row - 3, 123'000'000); });
  } else {
    VELOX_CHECK_EQ(values.size(), size);
    return makeFlatVector<Timestamp>(
        size, [&](auto row) { return Timestamp(values[row], 123'000'000); });
  }
}

template <>
FlatVectorPtr<bool> MinMaxByAggregationTestBase::buildDataVector(
    vector_size_t size,
    folly::Range<const int*> values) {
  if (values.empty()) {
    return makeFlatVector<bool>(
        size, [](auto row) { return row % 2 == 0 ? true : false; });
  } else {
    VELOX_CHECK_EQ(values.size(), size);
    return makeFlatVector<bool>(
        size, [&](auto row) { return values[row] % 2 == 0 ? true : false; });
  }
}

VectorPtr MinMaxByAggregationTestBase::buildDataVector(
    TypeKind kind,
    vector_size_t size,
    folly::Range<const int*> values) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return buildDataVector<bool>(size, values);
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
    case TypeKind::TIMESTAMP:
      return buildDataVector<Timestamp>(size, values);
    default:
      LOG(FATAL) << "Unsupported value/comparison type of minmax_by(): "
                 << mapTypeKindToName(kind);
  }
}

template <typename T>
std::string asSql(T value) {
  return fmt::format("'{}'", value);
}

template <>
std::string asSql(Timestamp value) {
  return fmt::format("epoch_ms({})", value.toMillis());
}

template <>
std::string asSql(bool value) {
  return value ? "true" : "false";
}

void MinMaxByAggregationTestBase::SetUp() {
  AggregationTestBase::SetUp();
  AggregationTestBase::disallowInputShuffle();

  for (const TypeKind type : kSupportedTypes) {
    switch (type) {
      case TypeKind::BOOLEAN:
        dataVectorsByType_.emplace(type, buildDataVector<bool>(numValues_));
        break;
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
      case TypeKind::TIMESTAMP:
        dataVectorsByType_.emplace(
            type, buildDataVector<Timestamp>(numValues_));
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
         fmt::format("SELECT {}", asSql(dataAt<T>(0)))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         "SELECT NULL"},

        // All null cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 10),
              makeNullConstant(GetParam().comparisonType, 10)}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 10),
              makeConstant(std::optional<U>(dataAt<U>(0)), 10)}),
         "SELECT NULL"},

        // Extreme value cases.
        {makeRowVector(
             {makeNullableFlatVector<T>({std::nullopt, dataAt<T>(4)}),
              makeConstant(std::optional<U>(kMax<U>), 2)}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullableFlatVector<T>({dataAt<T>(4), std::nullopt}),
              makeConstant(std::optional<U>(kMax<U>), 2)}),
         fmt::format("SELECT {}", asSql(dataAt<T>(4)))},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(1), std::nullopt, dataAt<U>(2), dataAt<U>(3)})}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(1), std::nullopt, dataAt<U>(2), dataAt<U>(3)})}),
         fmt::format("SELECT {}", asSql(dataAt<T>(0)))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(4), std::nullopt, dataAt<U>(2), dataAt<U>(1)})}),
         fmt::format("SELECT {}", asSql(dataAt<T>(4)))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(4)})}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), std::nullopt, dataAt<T>(3), dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(4)})}),
         fmt::format("SELECT {}", asSql(dataAt<T>(3)))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      // Skip testing with TableScan because the result for some testData
      // depends on input order that is non-deterministic with two splits.
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
         fmt::format("SELECT {}", asSql(dataAt<T>(0)))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         "SELECT NULL"},

        // All null cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 10),
              makeNullConstant(GetParam().comparisonType, 10)}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 10),
              makeConstant(std::optional<U>(dataAt<U>(0)), 10)}),
         "SELECT NULL"},

        // Extreme value cases.
        {makeRowVector(
             {makeNullableFlatVector<T>({std::nullopt, dataAt<T>(4)}),
              makeConstant(std::optional<U>(kLowest<U>), 2)}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullableFlatVector<T>({dataAt<T>(4), std::nullopt}),
              makeConstant(std::optional<U>(kLowest<U>), 2)}),
         fmt::format("SELECT {}", asSql(dataAt<T>(4)))},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         fmt::format("SELECT {}", asSql(dataAt<T>(0)))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(1), std::nullopt, dataAt<U>(3), dataAt<U>(4)})}),
         fmt::format("SELECT {}", asSql(dataAt<T>(4)))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(1), std::nullopt, dataAt<U>(4), dataAt<U>(0)})}),
         "SELECT NULL"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), std::nullopt, dataAt<T>(3), dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(1), std::nullopt, dataAt<U>(4), dataAt<U>(0)})}),
         fmt::format("SELECT {}", asSql(dataAt<T>(3)))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      // Skip testing with TableScan because the result for some testData
      // depends on input order that is non-deterministic with two splits.
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
  // randomXxx tests do not work for timestamp values because makeVectors
  // generates Timestamps with nanoseconds precision, but DuckDB only support
  // microseconds precision. We need to update makeVectors to use VectorFuzzer,
  // which allows to specify timestamp precision, and also update VectorFuzzer
  // to generate only valid Timestamp values. Currently, VectorFuzzer may
  // generate values that are too large (and therefore are not supported by
  // DuckDB).
  if (GetParam().comparisonType == TypeKind::TIMESTAMP ||
      GetParam().valueType == TypeKind::TIMESTAMP) {
    return;
  }

  testGlobalAggregation(
      rowVectors_,
      kMinBy,
      getColumnName(GetParam().valueType),
      getColumnName(GetParam().comparisonType));
}

TEST_P(MinMaxByGlobalByAggregationTest, randomMaxByGlobalBy) {
  if (GetParam().comparisonType == TypeKind::TIMESTAMP ||
      GetParam().valueType == TypeKind::TIMESTAMP) {
    return;
  }

  testGlobalAggregation(
      rowVectors_,
      kMaxBy,
      getColumnName(GetParam().valueType),
      getColumnName(GetParam().comparisonType));
}

TEST_P(
    MinMaxByGlobalByAggregationTest,
    randomMaxByGlobalByWithDistinctCompareValue) {
  if (GetParam().comparisonType == TypeKind::TIMESTAMP ||
      GetParam().valueType == TypeKind::TIMESTAMP ||
      GetParam().comparisonType == TypeKind::BOOLEAN) {
    return;
  }

  // Enable disk spilling test with distinct comparison values.
  AggregationTestBase::allowInputShuffle();

  auto rowType =
      ROW({"c0", "c1"},
          {fromKindToScalerType(GetParam().valueType),
           fromKindToScalerType(GetParam().comparisonType)});

  const bool isSmallInt = GetParam().comparisonType == TypeKind::TINYINT ||
      GetParam().comparisonType == TypeKind::SMALLINT;
  const int kBatchSize = isSmallInt ? 1 << 4 : 1 << 10;
  const int kNumBatches = isSmallInt ? 4 : 10;
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
    // Generate a non-lazy vector so that it can be written out as a duckDB
    // table.
    auto valueVector = fuzzer.fuzz(fromKindToScalerType(GetParam().valueType));
    auto comparisonVector = buildDataVector(
        GetParam().comparisonType,
        kBatchSize,
        folly::range<const int*>(rawValues, rawValues + kBatchSize));
    rawValues += kBatchSize;
    rowVectors.push_back(makeRowVector({valueVector, comparisonVector}));
  }
  createDuckDbTable(rowVectors);

  testGlobalAggregation(rowVectors, kMinBy, "c0", "c1");

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
    std::string verifyDuckDbSql;
    if (GetParam().valueType == TypeKind::BOOLEAN) {
      verifyDuckDbSql = fmt::format(
          "SELECT {}, CAST({} as BOOLEAN) FROM tmp GROUP BY {}",
          groupByColumnName,
          aggregate,
          groupByColumnName);
    } else {
      verifyDuckDbSql = fmt::format(
          "SELECT {}, {} FROM tmp GROUP BY {}",
          groupByColumnName,
          aggregate,
          groupByColumnName);
    }
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
             "SELECT {}, {}", asSql(dataAt<int32_t>(0)), asSql(dataAt<T>(0)))},

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
         fmt::format("SELECT {}, NULL", asSql(dataAt<int32_t>(0)))},

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
             "VALUES ({}, NULL), ({}, NULL), ({}, NULL)",
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
             "VALUES ({}, NULL), ({}, NULL), ({}, NULL)",
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
                   dataAt<U>(4),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(1),
                   dataAt<U>(3)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(0)})}),
         fmt::format(
             "VALUES ({}, {}), ({}, NULL), ({}, {})",
             asSql(dataAt<int32_t>(0)),
             asSql(dataAt<T>(0)),
             asSql(dataAt<int32_t>(1)),
             asSql(dataAt<int32_t>(2)),
             asSql(dataAt<T>(1)))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt,
                   dataAt<T>(2),
                   std::nullopt,
                   dataAt<T>(1),
                   std::nullopt,
                   dataAt<T>(0)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(3),
                   dataAt<U>(4),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(2),
                   dataAt<U>(1)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "VALUES ({}, NULL), ({}, {}), ({}, {})",
             asSql(dataAt<int32_t>(0)),
             asSql(dataAt<int32_t>(1)),
             asSql(dataAt<T>(1)),
             asSql(dataAt<int32_t>(2)),
             asSql(dataAt<T>(0)))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      // Skip testing with TableScan because the result for some testData
      // depends on input order that is non-deterministic with two splits.
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
             "SELECT {}, {}", asSql(dataAt<int32_t>(0)), asSql(dataAt<T>(0)))},
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
         fmt::format("SELECT {}, NULL", asSql(dataAt<int32_t>(0)))},

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
             "VALUES ({}, NULL), ({}, NULL), ({}, NULL)",
             asSql(dataAt<int32_t>(0)),
             asSql(dataAt<int32_t>(1)),
             asSql(dataAt<int32_t>(2)))},

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
             "VALUES ({}, NULL), ({}, NULL), ({}, NULL)",
             asSql(dataAt<int32_t>(0)),
             asSql(dataAt<int32_t>(1)),
             asSql(dataAt<int32_t>(2)))},

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
                   dataAt<U>(4),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(3),
                   dataAt<U>(3)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(0)})}),
         fmt::format(
             "VALUES ({}, NULL), ({}, {}), ({}, {})",
             asSql(dataAt<int32_t>(0)),
             asSql(dataAt<int32_t>(1)),
             asSql(dataAt<T>(2)),
             asSql(dataAt<int32_t>(2)),
             asSql(dataAt<T>(1)))},

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
                   dataAt<U>(3),
                   std::nullopt,
                   dataAt<U>(1),
                   dataAt<U>(1),
                   dataAt<U>(2)}),
              makeNullableFlatVector<int32_t>(
                  {dataAt<int32_t>(0),
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(1),
                   dataAt<int32_t>(2),
                   dataAt<int32_t>(2)})}),
         fmt::format(
             "VALUES ({}, NULL), ({}, {}), ({}, {})",
             asSql(dataAt<int32_t>(0)),
             asSql(dataAt<int32_t>(1)),
             asSql(dataAt<T>(1)),
             asSql(dataAt<int32_t>(2)),
             asSql(dataAt<T>(0)))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(testData.debugString());
      // Skip testing with TableScan because the result for some testData
      // depends on input order that is non-deterministic with two splits.
      testAggregations(
          {testData.inputRowVector},
          {"c2"},
          {"max_by(c0, c1)"},
          {},
          testData.verifyDuckDbSql);
    }
  }
};

TEST_P(MinMaxByGroupByAggregationTest, minByGroupBy) {
  EXECUTE_TEST(testMinByGroupBy);
}

TEST_P(MinMaxByGroupByAggregationTest, maxByGroupBy) {
  EXECUTE_TEST(testMaxByGroupBy);
}

TEST_P(MinMaxByGroupByAggregationTest, randomMinByGroupBy) {
  if (GetParam().comparisonType == TypeKind::TIMESTAMP ||
      GetParam().valueType == TypeKind::TIMESTAMP) {
    return;
  }

  testGroupByAggregation(
      rowVectors_,
      kMinBy,
      getColumnName(GetParam().valueType),
      getColumnName(GetParam().comparisonType),
      getColumnName(TypeKind::INTEGER));
}

TEST_P(MinMaxByGroupByAggregationTest, randomMaxByGroupBy) {
  if (GetParam().comparisonType == TypeKind::TIMESTAMP ||
      GetParam().valueType == TypeKind::TIMESTAMP) {
    return;
  }

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
  if (GetParam().comparisonType == TypeKind::TIMESTAMP ||
      GetParam().valueType == TypeKind::TIMESTAMP ||
      GetParam().comparisonType == TypeKind::BOOLEAN) {
    return;
  }

  // Enable disk spilling test with distinct comparison values.
  AggregationTestBase::allowInputShuffle();

  auto rowType =
      ROW({"c0", "c1", "c2"},
          {fromKindToScalerType(GetParam().valueType),
           fromKindToScalerType(GetParam().comparisonType),
           INTEGER()});

  const bool isSmallInt = GetParam().comparisonType == TypeKind::TINYINT ||
      GetParam().comparisonType == TypeKind::SMALLINT;
  const int kBatchSize = isSmallInt ? 1 << 4 : 1 << 10;
  const int kNumBatches = isSmallInt ? 3 : 10;
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
    // Generate a non-lazy vector so that it can be written out as a duckDB
    // table.
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

  testGroupByAggregation(rowVectors, kMinBy, "c0", "c1", "c2");

  testGroupByAggregation(rowVectors, kMaxBy, "c0", "c1", "c2");
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    MinMaxByAggregationTest,
    MinMaxByGroupByAggregationTest,
    testing::ValuesIn(getTestParams()));

class MinMaxByComplexTypes : public AggregationTestBase {};

TEST_F(MinMaxByComplexTypes, array) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {},
          {6, 7, 8},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });

  // DuckDB doesn't support min_by and max_by with complex types properly.
  // min_by(ARRAY, x) returns result of type VARCHAR, not ARRAY.
  // Assertion failed: (value.type().InternalType() == PhysicalType::LIST),
  // function GetChildren, file duckdb-2.cpp, line 4896.

  auto expected = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1)", "max_by(c0, c1)"}, {expected});
}

TEST_F(MinMaxByComplexTypes, arrayGroupBy) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({5, 6, 5, 6}),
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {},
          {6, 7, 8},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({5, 6}),
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
      }),
      makeArrayVector<int64_t>({
          {},
          {6, 7, 8},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2)", "max_by(c1, c2)"}, {expected});
}

TEST_F(MinMaxByComplexTypes, map) {
  auto data = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{1, 10}, {2, 20}, {3, 30}},
          {{4, 40}, {5, 50}},
          {},
          {{6, 60}, {7, 70}, {8, 80}},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });

  auto expected = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{1, 10}, {2, 20}, {3, 30}},
      }),
      makeMapVector<int64_t, int64_t>({
          {{6, 60}, {7, 70}, {8, 80}},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1)", "max_by(c0, c1)"}, {expected});
}

TEST_F(MinMaxByComplexTypes, mapGroupBy) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({5, 6, 5, 6}),
      makeMapVector<int64_t, int64_t>({
          {{1, 10}, {2, 20}, {3, 30}},
          {{4, 40}, {5, 50}},
          {},
          {{6, 60}, {7, 70}, {8, 80}},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({5, 6}),
      makeMapVector<int64_t, int64_t>({
          {{1, 10}, {2, 20}, {3, 30}},
          {{4, 40}, {5, 50}},
      }),
      makeMapVector<int64_t, int64_t>({
          {},
          {{6, 60}, {7, 70}, {8, 80}},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2)", "max_by(c1, c2)"}, {expected});
}

TEST_F(MinMaxByComplexTypes, arrayCompare) {
  auto expected = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8},
      }),
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {3, 4, 5},
          {6, 7, 8},
      }),
  });
  testAggregations(
      {data}, {}, {"min_by(c0, c1)", "max_by(c0, c1)"}, {expected});
}

TEST_F(MinMaxByComplexTypes, rowCompare) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8},
      }),
      makeRowVector({makeNullableFlatVector<int32_t>({1, 2, 3})}),
  });

  auto expected = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1)", "max_by(c0, c1)"}, {expected});
}

TEST_F(MinMaxByComplexTypes, arrayCheckNulls) {
  auto batch = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2]",
          "[6, 7]",
          "[2, 3]",
      }),
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  auto batchWithNull = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2]",
          "[6, 7]",
          "[3, null]",
      }),
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  for (const auto& expr : {"min_by(c0, c1)", "max_by(c0, c1)"}) {
    testFailingAggregations(
        {batch, batchWithNull},
        {},
        {expr},
        "ARRAY comparison not supported for values that contain nulls");
    testFailingAggregations(
        {batch, batchWithNull},
        {"c2"},
        {expr},
        "ARRAY comparison not supported for values that contain nulls");
  }
}

TEST_F(MinMaxByComplexTypes, rowCheckNull) {
  auto batch = makeRowVector({
      makeFlatVector<int8_t>({1, 2, 3}),
      makeRowVector(
          {makeFlatVector<std::string>({"a", "b", "c"}),
           makeFlatVector<std::string>({"aa", "bb", "cc"})}),
      makeFlatVector<int8_t>({1, 2, 3}),
  });

  auto batchWithNull = makeRowVector({
      makeFlatVector<int8_t>({1, 2, 3}),
      makeRowVector({
          makeFlatVector<std::string>({"a", "b", "c"}),
          makeNullableFlatVector<std::string>({"aa", std::nullopt, "cc"}),
      }),
      makeFlatVector<int8_t>({1, 2, 3}),
  });

  for (const auto& expr : {"min_by(c0, c1)", "max_by(c0, c1)"}) {
    testFailingAggregations(
        {batch, batchWithNull},
        {},
        {expr},
        "ROW comparison not supported for values that contain nulls");
    testFailingAggregations(
        {batch, batchWithNull},
        {"c2"},
        {expr},
        "ROW comparison not supported for values that contain nulls");
  }
}

TEST_F(MinMaxByComplexTypes, failOnUnorderableType) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
      makeAllNullMapVector(5, VARCHAR(), BIGINT()),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
  });

  static const std::string kErrorMessage =
      "Aggregate function signature is not supported";
  for (const auto& expr : {"min_by(c0, c1)", "min_by(c0, c1)"}) {
    {
      auto builder = PlanBuilder().values({data});
      VELOX_ASSERT_THROW(builder.singleAggregation({}, {expr}), kErrorMessage);
    }

    {
      auto builder = PlanBuilder().values({data});
      VELOX_ASSERT_THROW(
          builder.singleAggregation({"c2"}, {expr}), kErrorMessage);
    }
  }
}

class MinMaxByNTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    AggregationTestBase::allowInputShuffle();
    AggregationTestBase::enableTestStreaming();
  }
};

TEST_F(MinMaxByNTest, global) {
  // DuckDB doesn't support 3-argument versions of min_by and max_by.

  // No nulls in values.
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
  });

  auto expected = makeRowVector({
      makeArrayVector<int32_t>({
          {7, 6, 5},
      }),
      makeArrayVector<int32_t>({
          {1, 2, 3, 4},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, 3)", "max_by(c0, c1, 4)"}, {expected});

  // Some nulls in values.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {1, 2, 3, std::nullopt, 5, std::nullopt, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
  });

  expected = makeRowVector({
      makeNullableArrayVector<int32_t>({
          {7, std::nullopt, 5},
      }),
      makeNullableArrayVector<int32_t>({
          {1, 2, 3, std::nullopt},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, 3)", "max_by(c0, c1, 4)"}, {expected});

  // bool type of comparison
  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeFlatVector<bool>({true, false}),
  });

  expected = makeRowVector({
      makeArrayVector<int32_t>({
          {2},
      }),
      makeArrayVector<int32_t>({
          {1},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, 1)", "max_by(c0, c1, 1)"}, {expected});

  // bool type of value
  data = makeRowVector({
      makeFlatVector<bool>({true, false, true, false, true, false, true}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
  });

  expected = makeRowVector({
      makeArrayVector<bool>({
          {true, false, true},
      }),
      makeArrayVector<bool>({
          {true, false, true, false},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, 3)", "max_by(c0, c1, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, globalWithNullCompare) {
  // Rows with null 'compare' should be ignored.
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeNullableFlatVector<int64_t>(
          {77, std::nullopt, 55, 44, 33, 22, std::nullopt}),
  });

  auto expected = makeRowVector({
      makeNullableArrayVector<int32_t>({
          {6, 5, 4},
      }),
      makeNullableArrayVector<int32_t>({
          {1, 3, 4, 5},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, 3)", "max_by(c0, c1, 4)"}, {expected});

  // All 'compare' values are null.
  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeNullConstant(TypeKind::BIGINT, 7),
  });

  expected = makeRowVector({
      makeAllNullArrayVector(1, INTEGER()),
      makeAllNullArrayVector(1, INTEGER()),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, 3)", "max_by(c0, c1, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, globalWithNullN) {
  // Rows with null 'compare' should be ignored.
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
       makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
       makeNullableFlatVector<int64_t>(
           {3, std::nullopt, 3, 3, 3, std::nullopt, 3})});

  auto expected = makeRowVector({
      makeArrayVector<int32_t>({
          {7, 5, 4},
      }),
      makeArrayVector<int32_t>({
          {1, 3, 4},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, c2)", "max_by(c0, c1, c2)"}, {expected});

  // All 'N' are null.
  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
      makeNullConstant(TypeKind::BIGINT, 7),
  });

  expected = makeRowVector({
      makeAllNullArrayVector(1, INTEGER()),
      makeAllNullArrayVector(1, INTEGER()),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, c2)", "max_by(c0, c1, c2)"}, {expected});
}

TEST_F(MinMaxByNTest, sortedGlobal) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int32_t>({10, 20, 30, 40, 10, 20, 30}),
      makeFlatVector<int32_t>({11, 22, 33, 44, 55, 66, 77}),
  });

  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {},
                      {
                          "min_by(c0, c1 ORDER BY c2)",
                          "min_by(c0, c1 ORDER BY c2 DESC)",
                      })
                  .planNode();

  assertQuery(
      plan,
      "SELECT min_by(c0, c1 ORDER BY c2), min_by(c0, c1 ORDER BY c2 DESC) FROM tmp");
}

TEST_F(MinMaxByNTest, groupBy) {
  // No nulls in values.
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {7, 5, 3},
          {6, 4, 2},
      }),
      makeArrayVector<int32_t>({
          {1, 3, 5, 7},
          {2, 4, 6},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, 3)", "max_by(c1, c2, 4)"}, {expected});

  // Some nulls in values.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeNullableFlatVector<int32_t>(
          {1, 2, std::nullopt, std::nullopt, 5, std::nullopt, std::nullopt}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeNullableArrayVector<int32_t>({
          {std::nullopt, 5, std::nullopt},
          {std::nullopt, std::nullopt, 2},
      }),
      makeNullableArrayVector<int32_t>({
          {1, std::nullopt, 5, std::nullopt},
          {2, std::nullopt, std::nullopt},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, 3)", "max_by(c1, c2, 4)"}, {expected});

  // bool type of comparison
  // Make input size at least 8 to ensure drivers get 2 input batches for
  // spilling when tested with data read from files.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1, 2}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 1, 2, 3, 4}),
      makeFlatVector<bool>(
          {true, false, false, true, true, false, false, true}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {3},
          {2},
      }),
      makeArrayVector<int32_t>({
          {1},
          {4},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, 1)", "max_by(c1, c2, 1)"}, {expected});

  // bool type of value
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<bool>({true, false, false, true, true, false, true}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<bool>({
          {true, true, false},
          {false, true, false},
      }),
      makeArrayVector<bool>({
          {true, false, true, true},
          {false, true, false},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, 3)", "max_by(c1, c2, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, groupByWithNullCompare) {
  // Rows with null 'compare' should be ignored.
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeNullableFlatVector<int64_t>(
          {77, std::nullopt, 55, 44, std::nullopt, 22, 11}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {7, 3, 1},
          {6, 4},
      }),
      makeArrayVector<int32_t>({
          {1, 3, 7},
          {4, 6},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, 3)", "max_by(c1, c2, 4)"}, {expected});

  // All 'compare' values are null for one group.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeNullableFlatVector<int64_t>(
          {77, std::nullopt, 55, std::nullopt, std::nullopt, std::nullopt, 11}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeNullableArrayVector<int32_t>({
          {{7, 3, 1}},
          std::nullopt,
      }),
      makeNullableArrayVector<int32_t>({
          {{1, 3, 7}},
          std::nullopt,
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, 3)", "max_by(c1, c2, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, groupByWithNullN) {
  // Rows with null 'N' should be ignored.
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
      makeNullableFlatVector<int64_t>(
          {3, std::nullopt, 3, 3, std::nullopt, 3, 3}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {7, 3, 1},
          {6, 4},
      }),
      makeArrayVector<int32_t>({
          {1, 3, 7},
          {4, 6},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, c3)", "max_by(c1, c2, c3)"}, {expected});

  // All 'N' values are null for one group.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
      makeNullableFlatVector<int64_t>(
          {3, std::nullopt, 3, std::nullopt, std::nullopt, std::nullopt, 3}),
  });

  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeNullableArrayVector<int32_t>({
          {{7, 3, 1}},
          std::nullopt,
      }),
      makeNullableArrayVector<int32_t>({
          {{1, 3, 7}},
          std::nullopt,
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, c3)", "max_by(c1, c2, c3)"}, {expected});
}

TEST_F(MinMaxByNTest, sortedGroupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int32_t>({10, 20, 30, 40, 10, 20, 30}),
      makeFlatVector<int32_t>({11, 22, 33, 44, 55, 66, 77}),
  });

  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {"c0"},
                      {
                          "min_by(c1, c2 ORDER BY c3)",
                          "min_by(c1, c2 ORDER BY c3 DESC)",
                      })
                  .planNode();

  assertQuery(
      plan,
      "SELECT c0, min_by(c1, c2 ORDER BY c3), min_by(c1, c2 ORDER BY c3 DESC) FROM tmp GROUP BY 1");
}

TEST_F(MinMaxByNTest, variableN) {
  // Tests below check the error behavior on invalid inputs, so testIncremental
  // is not needed for these cases.
  AggregationTestBase::disableTestIncremental();

  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
  });

  // Global aggregation with variable value of 'n' is not allowed.
  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"min_by(c0, c1, c2)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "third argument of max_by/min_by must be a constant for all rows in a group");

  // Different groups in a group-by may have different values of 'n'.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
      makeFlatVector<int64_t>({1, 3, 1, 3, 1, 3, 1}),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector<int32_t>({
          {7},
          {6, 4, 2},
      }),
      makeArrayVector<int32_t>({
          {1},
          {2, 4, 6},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, c3)", "max_by(c1, c2, c3)"}, {expected});

  // Variable value of 'n' within a group is not allowed.
  data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
  });

  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"min_by(c1, c2, c3)"})
             .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "third argument of max_by/min_by must be a constant for all rows in a group");

  AggregationTestBase::enableTestIncremental();
}

TEST_F(MinMaxByNTest, globalRow) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6}),
      makeRowVector({
          makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6}),
          makeFlatVector<int32_t>({10, 20, 30, 40, 50, 60}),
      }),
  });

  auto expected = makeRowVector({
      makeArrayVector(
          {0},
          makeRowVector({
              makeFlatVector<int16_t>({1, 2, 3}),
              makeFlatVector<int32_t>({10, 20, 30}),
          })),
      makeArrayVector(
          {0},
          makeRowVector({
              makeFlatVector<int16_t>({6, 5, 4, 3}),
              makeFlatVector<int32_t>({60, 50, 40, 30}),
          })),
  });

  testAggregations(
      {data}, {}, {"min_by(c1, c0, 3)", "max_by(c1, c0, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, globalRowWithNulls) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6}),
      makeRowVector(
          {
              makeFlatVector<int16_t>({-1, 2, -1, 4, -1, 6}),
              makeFlatVector<int32_t>({0, 20, 0, 40, 0, 60}),
          },
          nullEvery(2)),
  });

  auto expected = makeRowVector({
      makeArrayVector(
          {0},
          makeRowVector(
              {
                  makeFlatVector<int16_t>({-1, 2, -1}),
                  makeFlatVector<int32_t>({0, 20, 0}),
              },
              nullEvery(2))),
      makeArrayVector(
          {0},
          makeRowVector(
              {
                  makeFlatVector<int16_t>({6, -1, 4, -1}),
                  makeFlatVector<int32_t>({60, 0, 40, 0}),
              },
              nullEvery(2, 1))),
  });

  testAggregations(
      {data}, {}, {"min_by(c1, c0, 3)", "max_by(c1, c0, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, groupByRow) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 1, 2}),
      makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6}),
      makeRowVector({
          makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6}),
          makeFlatVector<int32_t>({10, 20, 30, 40, 50, 60}),
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeArrayVector(
          {0, 2},
          makeRowVector({
              makeFlatVector<int16_t>({1, 2, 3, 4}),
              makeFlatVector<int32_t>({10, 20, 30, 40}),
          })),
      makeArrayVector(
          {0, 2},
          makeRowVector({
              makeFlatVector<int16_t>({5, 2, 6, 4}),
              makeFlatVector<int32_t>({50, 20, 60, 40}),
          })),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c2, c1, 2)", "max_by(c2, c1, 2)"}, {expected});
}

namespace {
std::vector<std::string> makeStrings(vector_size_t size) {
  std::vector<std::string> s;
  s.reserve(size);
  for (auto i = 0; i < size; ++i) {
    if (i % 7 == 0) {
      // Short (inline) string.
      s.push_back(fmt::format("s{}", i));
    } else {
      // Long (non-inline string).
      s.push_back(fmt::format("Non-inline string #{}...", i));
    }
  }
  return s;
}
} // namespace

TEST_F(MinMaxByNTest, globalVarchar) {
  // DuckDB doesn't support 3-argument versions of min_by and max_by.

  vector_size_t size = 1'000;
  auto s = makeStrings(size);

  auto data = makeRowVector({
      makeFlatVector<std::string>(s),
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
  });

  auto expected = makeRowVector({
      makeArrayVector<std::string>({
          {s[0], s[1], s[2]},
      }),
      makeArrayVector<std::string>({
          {s[size - 1], s[size - 2], s[size - 3], s[size - 4]},
      }),
  });

  testAggregations(
      {data}, {}, {"min_by(c0, c1, 3)", "max_by(c0, c1, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, groupByVarchar) {
  // DuckDB doesn't support 3-argument versions of min_by and max_by.

  vector_size_t size = 1'000;
  auto s = makeStrings(size);

  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 10; }),
      makeFlatVector<std::string>(s),
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      makeArrayVector<std::string>({
          {s[0], s[10], s[20]},
          {s[1], s[11], s[21]},
          {s[2], s[12], s[22]},
          {s[3], s[13], s[23]},
          {s[4], s[14], s[24]},
          {s[5], s[15], s[25]},
          {s[6], s[16], s[26]},
          {s[7], s[17], s[27]},
          {s[8], s[18], s[28]},
          {s[9], s[19], s[29]},
      }),
      makeArrayVector<std::string>({
          {s[size - 10], s[size - 20], s[size - 30], s[size - 40]},
          {s[size - 9], s[size - 19], s[size - 29], s[size - 39]},
          {s[size - 8], s[size - 18], s[size - 28], s[size - 38]},
          {s[size - 7], s[size - 17], s[size - 27], s[size - 37]},
          {s[size - 6], s[size - 16], s[size - 26], s[size - 36]},
          {s[size - 5], s[size - 15], s[size - 25], s[size - 35]},
          {s[size - 4], s[size - 14], s[size - 24], s[size - 34]},
          {s[size - 3], s[size - 13], s[size - 23], s[size - 33]},
          {s[size - 2], s[size - 12], s[size - 22], s[size - 32]},
          {s[size - 1], s[size - 11], s[size - 21], s[size - 31]},
      }),
  });

  testAggregations(
      {data}, {"c0"}, {"min_by(c1, c2, 3)", "max_by(c1, c2, 4)"}, {expected});
}

TEST_F(MinMaxByNTest, stringComparison) {
  std::string s[6];
  for (int i = 0; i < 6; ++i) {
    s[i] = std::string(StringView::kInlineSize, 'x') + std::to_string(i);
  }

  auto data = makeRowVector({
      makeFlatVector<std::string>({s[0], s[1], s[2], s[3], s[4], s[5]}),
      makeRowVector({
          makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6}),
          makeFlatVector<int32_t>({10, 20, 30, 40, 50, 60}),
      }),
      makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>({s[5], s[4], s[3], s[2], s[1], s[0]}),
      makeFlatVector<bool>({true, true, true, false, false, false}),
  });

  // Test min_by(row, varchar, n) and max_by(row, varchar, n).
  {
    // Global.
    auto expected = makeRowVector({
        makeArrayVector(
            {0},
            makeRowVector({
                makeFlatVector<int16_t>({1, 2, 3}),
                makeFlatVector<int32_t>({10, 20, 30}),
            })),
        makeArrayVector(
            {0},
            makeRowVector({
                makeFlatVector<int16_t>({6, 5, 4, 3}),
                makeFlatVector<int32_t>({60, 50, 40, 30}),
            })),
    });
    testAggregations(
        {data}, {}, {"min_by(c1, c0, 3)", "max_by(c1, c0, 4)"}, {expected});
    testReadFromFiles(
        {data}, {}, {"min_by(c1, c0, 3)", "max_by(c1, c0, 4)"}, {expected});

    // Group-by.
    expected = makeRowVector({
        makeFlatVector<bool>({false, true}),
        makeArrayVector(
            {0, 2},
            makeRowVector({
                makeFlatVector<int16_t>({4, 5, 1, 2}),
                makeFlatVector<int32_t>({40, 50, 10, 20}),
            })),
        makeArrayVector(
            {0, 2},
            makeRowVector({
                makeFlatVector<int16_t>({6, 5, 3, 2}),
                makeFlatVector<int32_t>({60, 50, 30, 20}),
            })),
    });
    testAggregations(
        {data}, {"c4"}, {"min_by(c1, c0, 2)", "max_by(c1, c0, 2)"}, {expected});
    testReadFromFiles(
        {data}, {"c4"}, {"min_by(c1, c0, 2)", "max_by(c1, c0, 2)"}, {expected});
  }

  // Test min_by(smallint, varchar, n) and max_by(smallint, varchar, n).
  {
    // Global.
    auto expected = makeRowVector({
        makeArrayVector({0}, makeFlatVector<int16_t>({1, 2, 3})),
        makeArrayVector({0}, makeFlatVector<int16_t>({6, 5, 4, 3})),
    });
    testAggregations(
        {data}, {}, {"min_by(c2, c0, 3)", "max_by(c2, c0, 4)"}, {expected});
    testReadFromFiles(
        {data}, {}, {"min_by(c2, c0, 3)", "max_by(c2, c0, 4)"}, {expected});

    // Group-by.
    expected = makeRowVector({
        makeFlatVector<bool>({false, true}),
        makeArrayVector({0, 2}, makeFlatVector<int16_t>({4, 5, 1, 2})),
        makeArrayVector({0, 2}, makeFlatVector<int16_t>({6, 5, 3, 2})),
    });
    testAggregations(
        {data}, {"c4"}, {"min_by(c2, c0, 2)", "max_by(c2, c0, 2)"}, {expected});
    testReadFromFiles(
        {data}, {"c4"}, {"min_by(c2, c0, 2)", "max_by(c2, c0, 2)"}, {expected});
  }

  // Test min_by(varchar, varchar, n) and max_by(varchar, varchar, n).
  {
    // Global.
    auto expected = makeRowVector({
        makeArrayVector({0}, makeFlatVector<std::string>({s[5], s[4], s[3]})),
        makeArrayVector(
            {0}, makeFlatVector<std::string>({s[0], s[1], s[2], s[3]})),
    });
    testAggregations(
        {data}, {}, {"min_by(c3, c0, 3)", "max_by(c3, c0, 4)"}, {expected});
    testReadFromFiles(
        {data}, {}, {"min_by(c3, c0, 3)", "max_by(c3, c0, 4)"}, {expected});

    // Group-by.
    expected = makeRowVector({
        makeFlatVector<bool>({false, true}),
        makeArrayVector(
            {0, 2}, makeFlatVector<std::string>({s[2], s[1], s[5], s[4]})),
        makeArrayVector(
            {0, 2}, makeFlatVector<std::string>({s[0], s[1], s[3], s[4]})),
    });
    testAggregations(
        {data}, {"c4"}, {"min_by(c3, c0, 2)", "max_by(c3, c0, 2)"}, {expected});
    testReadFromFiles(
        {data}, {"c4"}, {"min_by(c3, c0, 2)", "max_by(c3, c0, 2)"}, {expected});
  }
}

TEST_F(MinMaxByNTest, incrementalWindow) {
  // Test that min_by(x, x, 10) and max_by(x, x, 10) produce correct results
  // when used in window operation with incremental frames.
  std::vector<VectorPtr> inputs = {
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<StringView>({"1"_sv, "2"_sv}),
      makeArrayVector<StringView>({{"1"_sv}, {"2"_sv}}),
      makeFlatVector<Timestamp>({Timestamp(0, 0), Timestamp(0, 1)}),
      makeFlatVector<int64_t>({10, 10}),
      makeFlatVector<bool>({false, false}),
      makeFlatVector<int64_t>({0, 1})};
  auto data = makeRowVector(inputs);
  auto result = inputs;

  // Test primitive type.
  {
    auto plan =
        PlanBuilder()
            .values({data})
            .window(
                {"max_by(c0, c0, c4) over (partition by c5 order by c6 asc)"})
            .planNode();

    result.push_back(makeArrayVector<int64_t>({{1}, {2, 1}}));
    AssertQueryBuilder(plan).assertResults(makeRowVector(result));

    plan =
        PlanBuilder()
            .values({data})
            .window(
                {"min_by(c0, c0, c4) over (partition by c5 order by c6 asc)"})
            .planNode();
    result.back() = makeArrayVector<int64_t>({{1}, {1, 2}});
    AssertQueryBuilder(plan).assertResults(makeRowVector(result));
  }

  // Test varchar type.
  {
    auto plan =
        PlanBuilder()
            .values({data})
            .window(
                {"max_by(c1, c1, c4) over (partition by c5 order by c6 asc)"})
            .planNode();

    result.back() = makeArrayVector<StringView>({{"1"_sv}, {"2"_sv, "1"_sv}});
    AssertQueryBuilder(plan).assertResults(makeRowVector(result));

    plan =
        PlanBuilder()
            .values({data})
            .window(
                {"min_by(c1, c1, c4) over (partition by c5 order by c6 asc)"})
            .planNode();

    result.back() = makeArrayVector<StringView>({{"1"_sv}, {"1"_sv, "2"_sv}});
    AssertQueryBuilder(plan).assertResults(makeRowVector(result));
  }

  // Test complex type.
  {
    auto plan =
        PlanBuilder()
            .values({data})
            .window(
                {"max_by(c2, c3, c4) over (partition by c5 order by c6 asc)"})
            .planNode();

    result.back() = makeNullableNestedArrayVector<StringView>(
        {{{{{"1"_sv}}}}, {{{{"2"_sv}}, {{"1"_sv}}}}});
    AssertQueryBuilder(plan).assertResults(makeRowVector(result));

    plan =
        PlanBuilder()
            .values({data})
            .window(
                {"min_by(c2, c3, c4) over (partition by c5 order by c6 asc)"})
            .planNode();

    result.back() = makeNullableNestedArrayVector<StringView>(
        {{{{{"1"_sv}}}}, {{{{"1"_sv}}, {{"2"_sv}}}}});
    AssertQueryBuilder(plan).assertResults(makeRowVector(result));
  }
}

} // namespace
} // namespace facebook::velox::aggregate::test
