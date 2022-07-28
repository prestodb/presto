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

#include <fmt/format.h>

using namespace facebook::velox::exec::test;

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
    TypeKind::VARCHAR};

std::vector<TestParam> getTestParams() {
  std::vector<TestParam> params;
  for (TypeKind valueType : kSupportedTypes) {
    for (TypeKind comparisonType : kSupportedTypes) {
      params.push_back({valueType, comparisonType});
    }
  }
  return params;
}

#define EXECUTE_TEST_BY_VALUE_TYPE(testFunc, valueType, isPartial)   \
  do {                                                               \
    switch (GetParam().comparisonType) {                             \
      case TypeKind::TINYINT:                                        \
        testFunc<valueType, int8_t>(isPartial);                      \
        break;                                                       \
      case TypeKind::SMALLINT:                                       \
        testFunc<valueType, int16_t>(isPartial);                     \
        break;                                                       \
      case TypeKind::INTEGER:                                        \
        testFunc<valueType, int32_t>(isPartial);                     \
        break;                                                       \
      case TypeKind::BIGINT:                                         \
        testFunc<valueType, int64_t>(isPartial);                     \
        break;                                                       \
      case TypeKind::REAL:                                           \
        testFunc<valueType, float>(isPartial);                       \
        break;                                                       \
      case TypeKind::DOUBLE:                                         \
        testFunc<valueType, double>(isPartial);                      \
        break;                                                       \
      case TypeKind::VARCHAR:                                        \
        testFunc<valueType, StringView>(isPartial);                  \
        break;                                                       \
      default:                                                       \
        LOG(FATAL) << "Unsupported comparison type of minmax_by(): " \
                   << mapTypeKindToName(GetParam().comparisonType);  \
    }                                                                \
  } while (0);

#define EXECUTE_TEST(testFunc, isPartial)                            \
  do {                                                               \
    switch (GetParam().valueType) {                                  \
      case TypeKind::TINYINT:                                        \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int8_t, isPartial);     \
        break;                                                       \
      case TypeKind::SMALLINT:                                       \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int16_t, isPartial);    \
        break;                                                       \
      case TypeKind::INTEGER:                                        \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int32_t, isPartial);    \
        break;                                                       \
      case TypeKind::BIGINT:                                         \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, int64_t, isPartial);    \
        break;                                                       \
      case TypeKind::REAL:                                           \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, float, isPartial);      \
        break;                                                       \
      case TypeKind::DOUBLE:                                         \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, double, isPartial);     \
        break;                                                       \
      case TypeKind::VARCHAR:                                        \
        EXECUTE_TEST_BY_VALUE_TYPE(testFunc, StringView, isPartial); \
        break;                                                       \
      default:                                                       \
        LOG(FATAL) << "Unsupported value type of minmax_by(): "      \
                   << mapTypeKindToName(GetParam().valueType);       \
    }                                                                \
  } while (0);

class MinMaxByAggregationTestBase : public AggregationTestBase {
 protected:
  MinMaxByAggregationTestBase() : numValues_(6) {}

  void SetUp() override;

  // Build a flat vector with numeric native type of T. The value in the
  // returned flat vector is in ascending order.
  template <typename T>
  FlatVectorPtr<T> buildDataVector() {
    return makeFlatVector<T>(numValues_, [](auto row) { return row + 1; });
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

  void executeQuery(
      const std::vector<RowVectorPtr>& values,
      const std::string& aggregate,
      bool isPartial,
      const std::string& verifyDuckDbSql) {
    executeQuery(values, aggregate, isPartial, {}, verifyDuckDbSql);
  }

  void executeQuery(
      const std::vector<RowVectorPtr>& values,
      const std::string& aggregate,
      bool isPartial,
      const std::vector<std::string>& groupByKeys,
      const std::string& verifyDuckDbSql) {
    if (isPartial) {
      auto op = PlanBuilder()
                    .values(values)
                    .partialAggregation(groupByKeys, {aggregate})
                    .planNode();
      assertQuery(op, verifyDuckDbSql);
    } else {
      testAggregations(values, groupByKeys, {aggregate}, verifyDuckDbSql);
    }
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

  const RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR()})};
  // Specify the number of values in each typed data vector in
  // 'dataVectorsByType_'.
  const int numValues_;
  std::unordered_map<TypeKind, VectorPtr> dataVectorsByType_;
  std::vector<RowVectorPtr> rowVectors_;
};

// Build a flat vector with StringView. The value in the returned flat vector
// is in ascending order.
template <>
FlatVectorPtr<StringView> MinMaxByAggregationTestBase::buildDataVector() {
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

void MinMaxByAggregationTestBase::SetUp() {
  AggregationTestBase::SetUp();

  for (const TypeKind type : kSupportedTypes) {
    switch (type) {
      case TypeKind::TINYINT:
        dataVectorsByType_.emplace(type, buildDataVector<int8_t>());
        break;
      case TypeKind::SMALLINT:
        dataVectorsByType_.emplace(type, buildDataVector<int16_t>());
        break;
      case TypeKind::INTEGER:
        dataVectorsByType_.emplace(type, buildDataVector<int32_t>());
        break;
      case TypeKind::BIGINT:
        dataVectorsByType_.emplace(type, buildDataVector<int64_t>());
        break;
      case TypeKind::REAL:
        dataVectorsByType_.emplace(type, buildDataVector<float>());
        break;
      case TypeKind::DOUBLE:
        dataVectorsByType_.emplace(type, buildDataVector<double>());
        break;
      case TypeKind::VARCHAR:
        dataVectorsByType_.emplace(type, buildDataVector<StringView>());
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
    executeQuery(vectors, aggregate, false, verifyDuckDbSql);
  }

  template <typename T, typename U>
  void minByGlobalByTest(bool isPartial) {
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
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => '{}', y => '{}')",
                   dataAt<T>(0),
                   dataAt<U>(0))
             : fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(0))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        // All null cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 10),
              makeNullConstant(GetParam().comparisonType, 10)}),
         "SELECT null"},

        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 10),
              makeConstant(std::optional<U>(dataAt<U>(0)), 10)}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(0))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(0), std::nullopt, dataAt<U>(1), dataAt<U>(2)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(0))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(0), std::nullopt, dataAt<U>(1), dataAt<U>(2)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => '{}', y => '{}')",
                   dataAt<T>(0),
                   dataAt<U>(0))
             : fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => '{}', y => '{}')",
                   dataAt<T>(4),
                   dataAt<U>(0))
             : fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(4))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(0), dataAt<U>(3)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(0))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), std::nullopt, dataAt<T>(3), dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(0), dataAt<U>(3)})}),
         isPartial ? fmt::format(
                         "SELECT struct_pack(x => '{}', y => '{}')",
                         dataAt<T>(3),
                         dataAt<U>(0))
                   : fmt::format(
                         "SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(3))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(
          fmt::format("{}\nisPartial:{}", testData.debugString(), isPartial));
      executeQuery(
          {testData.inputRowVector},
          "min_by(c0, c1)",
          isPartial,
          testData.verifyDuckDbSql);
    }
  }

  template <typename T, typename U>
  void maxByGlobalByTest(bool isPartial) {
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
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => '{}', y => '{}')",
                   dataAt<T>(0),
                   dataAt<U>(0))
             : fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(0), dataAt<T>(1), dataAt<T>(2)}),
              makeConstant(std::optional<U>(dataAt<U>(0)), 5)}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(0))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        // All null cases.
        {makeRowVector(
             {makeConstant(std::optional<T>(dataAt<T>(0)), 10),
              makeNullConstant(GetParam().comparisonType, 10)}),
         "SELECT null"},

        {makeRowVector(
             {makeNullConstant(GetParam().valueType, 10),
              makeConstant(std::optional<U>(dataAt<U>(0)), 10)}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(0))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        // Regular cases.
        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {std::nullopt, dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(2))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(1), dataAt<U>(0)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => '{}', y => '{}')",
                   dataAt<T>(0),
                   dataAt<U>(2))
             : fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(0))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(0), std::nullopt, dataAt<U>(1), dataAt<U>(2)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => '{}', y => '{}')",
                   dataAt<T>(4),
                   dataAt<U>(2))
             : fmt::format("SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(4))},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), dataAt<T>(3), std::nullopt, dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(3), dataAt<U>(0)})}),
         isPartial
             ? fmt::format(
                   "SELECT struct_pack(x => NULL, y => '{}')", dataAt<U>(3))
             : "SELECT * FROM (VALUES (NULL)) AS t"},

        {makeRowVector(
             {makeNullableFlatVector<T>(
                  {dataAt<T>(0), std::nullopt, dataAt<T>(3), dataAt<T>(4)}),
              makeNullableFlatVector<U>(
                  {dataAt<U>(2), std::nullopt, dataAt<U>(3), dataAt<U>(0)})}),
         isPartial ? fmt::format(
                         "SELECT struct_pack(x => '{}', y => '{}')",
                         dataAt<T>(3),
                         dataAt<U>(3))
                   : fmt::format(
                         "SELECT * FROM (VALUES ('{}')) AS t", dataAt<T>(3))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(
          fmt::format("{}\nisPartial:{}", testData.debugString(), isPartial));
      executeQuery(
          {testData.inputRowVector},
          "max_by(c0, c1)",
          isPartial,
          testData.verifyDuckDbSql);
    }
  }
};

TEST_P(MinMaxByGlobalByAggregationTest, minByPartialGlobalBy) {
  EXECUTE_TEST(minByGlobalByTest, true);
}

TEST_P(MinMaxByGlobalByAggregationTest, maxByPartialGlobalBy) {
  EXECUTE_TEST(maxByGlobalByTest, true);
}

TEST_P(MinMaxByGlobalByAggregationTest, minByFinalGlobalBy) {
  EXECUTE_TEST(minByGlobalByTest, false);
}

TEST_P(MinMaxByGlobalByAggregationTest, maxByFinalGlobalBy) {
  EXECUTE_TEST(maxByGlobalByTest, false);
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
    executeQuery(
        vectors, aggregate, false, {groupByColumnName}, verifyDuckDbSql);
  }

  template <typename T, typename U>
  void testMinByGroupBy(bool isPartial) {
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => '{}', y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<T>(0),
                   dataAt<U>(0))
             : fmt::format(
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => NULL, y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<U>(0))
             : fmt::format(
                   "SELECT * FROM( VALUES ('{}', NULL)) AS t",
                   dataAt<int32_t>(0))},

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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => NULL, y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<U>(4),
                   dataAt<int32_t>(1),
                   dataAt<U>(1),
                   dataAt<int32_t>(2),
                   dataAt<U>(0))
             : fmt::format(
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => '{}', y => '{}')), ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => '{}', y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<T>(0),
                   dataAt<U>(0),
                   dataAt<int32_t>(1),
                   dataAt<U>(2),
                   dataAt<int32_t>(2),
                   dataAt<T>(1),
                   dataAt<U>(1))
             : fmt::format(
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => '{}', y => '{}')), ('{}', struct_pack(x => '{}', y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<U>(4),
                   dataAt<int32_t>(1),
                   dataAt<T>(1),
                   dataAt<U>(1),
                   dataAt<int32_t>(2),
                   dataAt<T>(0),
                   dataAt<U>(0))
             : fmt::format(
                   "SELECT * FROM( VALUES ('{}', NULL), ('{}', '{}'), ('{}', '{}')) AS t",
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<T>(1),
                   dataAt<int32_t>(2),
                   dataAt<T>(0))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(
          fmt::format("{}\nisPartial: {}", testData.debugString(), isPartial));
      executeQuery(
          {testData.inputRowVector},
          "min_by(c0, c1)",
          isPartial,
          {"c2"},
          testData.verifyDuckDbSql);
    }
  }

  template <typename T, typename U>
  void testMaxByGroupBy(bool isPartial) {
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => '{}', y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<T>(0),
                   dataAt<U>(0))
             : fmt::format(
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => NULL, y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<U>(0))
             : fmt::format(
                   "SELECT * FROM( VALUES ('{}', NULL)) AS t",
                   dataAt<int32_t>(0))},

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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => NULL, y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<U>(5),
                   dataAt<int32_t>(1),
                   dataAt<U>(1),
                   dataAt<int32_t>(2),
                   dataAt<U>(2))
             : fmt::format(
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => '{}', y => '{}')), ('{}', struct_pack(x => '{}', y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<U>(4),
                   dataAt<int32_t>(1),
                   dataAt<T>(2),
                   dataAt<U>(5),
                   dataAt<int32_t>(2),
                   dataAt<T>(1),
                   dataAt<U>(1))
             : fmt::format(
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
         isPartial
             ? fmt::format(
                   "SELECT * FROM( VALUES ('{}', struct_pack(x => NULL, y => '{}')), ('{}', struct_pack(x => '{}', y => '{}')), ('{}', struct_pack(x => '{}', y => '{}'))) AS t",
                   dataAt<int32_t>(0),
                   dataAt<U>(5),
                   dataAt<int32_t>(1),
                   dataAt<T>(1),
                   dataAt<U>(1),
                   dataAt<int32_t>(2),
                   dataAt<T>(0),
                   dataAt<U>(2))
             : fmt::format(
                   "SELECT * FROM( VALUES ('{}', NULL), ('{}', '{}'), ('{}', '{}')) AS t",
                   dataAt<int32_t>(0),
                   dataAt<int32_t>(1),
                   dataAt<T>(1),
                   dataAt<int32_t>(2),
                   dataAt<T>(0))}};
    for (const auto& testData : testSettings) {
      SCOPED_TRACE(
          fmt::format("{}\nisPartial: {}", testData.debugString(), isPartial));
      executeQuery(
          {testData.inputRowVector},
          "max_by(c0, c1)",
          isPartial,
          {"c2"},
          testData.verifyDuckDbSql);
    }
  }
};

TEST_P(MinMaxByGroupByAggregationTest, minByPartialGroupBy) {
  EXECUTE_TEST(testMinByGroupBy, true);
}

TEST_P(MinMaxByGroupByAggregationTest, maxByPartialGroupBy) {
  EXECUTE_TEST(testMaxByGroupBy, true);
}

TEST_P(MinMaxByGroupByAggregationTest, minByFinalGroupBy) {
  EXECUTE_TEST(testMinByGroupBy, false);
}

TEST_P(MinMaxByGroupByAggregationTest, maxByFinalGroupBy) {
  EXECUTE_TEST(testMaxByGroupBy, false);
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

VELOX_INSTANTIATE_TEST_SUITE_P(
    MinMaxByAggregationTest,
    MinMaxByGroupByAggregationTest,
    testing::ValuesIn(getTestParams()));

} // namespace
} // namespace facebook::velox::aggregate::test
