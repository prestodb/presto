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

#include <gtest/gtest.h>

#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/vector/VectorTypeUtils.h"

using facebook::velox::duckdb::duckdbTimestampToVelox;
using facebook::velox::duckdb::veloxTimestampToDuckDB;

namespace facebook::velox::exec::test {
namespace {

std::string makeCreateTableSql(
    const std::string& tableName,
    const RowType& rowType) {
  std::ostringstream sql;
  sql << "CREATE TABLE " << tableName << "(";
  for (int32_t i = 0; i < rowType.size(); i++) {
    if (i > 0) {
      sql << ", ";
    }
    sql << rowType.nameOf(i) << " ";
    auto child = rowType.childAt(i);
    if (child->isArray()) {
      sql << child->asArray().elementType()->kindName() << "[]";
    } else {
      sql << child->kindName();
    }
  }
  sql << ")";
  return sql.str();
}

template <TypeKind kind>
::duckdb::Value duckValueAt(const VectorPtr& vector, vector_size_t index) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  return ::duckdb::Value(vector->as<SimpleVector<T>>()->valueAt(index));
}

template <>
::duckdb::Value duckValueAt<TypeKind::VARCHAR>(
    const VectorPtr& vector,
    vector_size_t index) {
  // DuckDB requires zero-ending string
  auto copy = vector->as<SimpleVector<StringView>>()->valueAt(index).str();
  return ::duckdb::Value(copy);
}

template <>
::duckdb::Value duckValueAt<TypeKind::TIMESTAMP>(
    const VectorPtr& vector,
    vector_size_t index) {
  using T = typename KindToFlatVector<TypeKind::TIMESTAMP>::WrapperType;
  return ::duckdb::Value::TIMESTAMP(
      veloxTimestampToDuckDB(vector->as<SimpleVector<T>>()->valueAt(index)));
}

template <>
::duckdb::Value duckValueAt<TypeKind::ARRAY>(
    const VectorPtr& vector,
    int32_t row) {
  auto arrayVector = vector->as<ArrayVector>();
  auto& elements = arrayVector->elements();
  auto offset = arrayVector->offsetAt(row);
  auto size = arrayVector->sizeAt(row);

  std::vector<::duckdb::Value> array;
  array.reserve(size);
  for (auto i = 0; i < size; i++) {
    auto innerRow = offset + i;
    if (elements->isNullAt(innerRow)) {
      array.emplace_back(::duckdb::Value(nullptr));
    } else {
      array.emplace_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          duckValueAt, elements->typeKind(), elements, innerRow));
    }
  }

  return ::duckdb::Value::LIST(array);
}

template <TypeKind kind>
velox::variant
variantAt(::duckdb::DataChunk* dataChunk, int32_t row, int32_t column) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  return velox::variant(dataChunk->GetValue(column, row).GetValue<T>());
}

template <>
velox::variant variantAt<TypeKind::VARCHAR>(
    ::duckdb::DataChunk* dataChunk,
    int32_t row,
    int32_t column) {
  return velox::variant(StringView(dataChunk->GetValue(column, row).str_value));
}

template <>
velox::variant variantAt<TypeKind::VARBINARY>(
    ::duckdb::DataChunk* dataChunk,
    int32_t row,
    int32_t column) {
  return velox::variant(StringView(dataChunk->GetValue(column, row).str_value));
}

template <>
velox::variant variantAt<TypeKind::TIMESTAMP>(
    ::duckdb::DataChunk* dataChunk,
    int32_t row,
    int32_t column) {
  return velox::variant::timestamp(duckdbTimestampToVelox(
      dataChunk->GetValue(column, row).GetValue<::duckdb::timestamp_t>()));
}

template <TypeKind kind>
velox::variant variantAt(const ::duckdb::Value& value) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  return velox::variant(value.GetValue<T>());
}

velox::variant rowVariantAt(
    const ::duckdb::Value& vector,
    const std::shared_ptr<const Type>& rowType) {
  std::vector<velox::variant> values;
  for (size_t i = 0; i < vector.struct_value.size(); ++i) {
    auto currChild = vector.struct_value[i];
    auto currType = rowType->childAt(i)->kind();
    if (currChild.is_null) {
      values.push_back(variant(currType));
    } else if (currType == TypeKind::ROW) {
      values.push_back(rowVariantAt(currChild, rowType->childAt(i)));
    } else {
      auto value =
          VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(variantAt, currType, currChild);
      values.push_back(value);
    }
  }
  return velox::variant::row(std::move(values));
}

std::vector<MaterializedRow> materialize(
    ::duckdb::DataChunk* dataChunk,
    const std::shared_ptr<const RowType>& rowType) {
  EXPECT_EQ(rowType->size(), dataChunk->GetTypes().size())
      << "Wrong number of columns";

  auto size = dataChunk->size();
  std::vector<MaterializedRow> rows;
  rows.reserve(size);

  for (size_t i = 0; i < size; ++i) {
    MaterializedRow row;
    row.reserve(rowType->size());
    for (size_t j = 0; j < rowType->size(); ++j) {
      auto typeKind = rowType->childAt(j)->kind();
      if (dataChunk->GetValue(j, i).is_null) {
        row.push_back(variant(typeKind));
      } else if (typeKind == TypeKind::ROW) {
        row.push_back(
            rowVariantAt(dataChunk->GetValue(j, i), rowType->childAt(j)));
      } else {
        auto value = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            variantAt, typeKind, dataChunk, i, j);
        row.push_back(value);
      }
    }
    rows.push_back(row);
  }
  return rows;
}

template <TypeKind kind>
velox::variant variantAt(VectorPtr vector, int32_t row) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  return velox::variant(vector->as<SimpleVector<T>>()->valueAt(row));
}

template <>
velox::variant variantAt<TypeKind::TIMESTAMP>(VectorPtr vector, int32_t row) {
  // DuckDB's timestamps have microseconds precision, while Velox has nanos.
  // Converting to duckDB and back to Velox to truncate nanoseconds, and thus
  // allow the comparisons to match.
  using T = typename KindToFlatVector<TypeKind::TIMESTAMP>::WrapperType;
  return velox::variant(duckdbTimestampToVelox(
      veloxTimestampToDuckDB(vector->as<SimpleVector<T>>()->valueAt(row))));
}

velox::variant arrayVariantAt(const VectorPtr& vector, vector_size_t row) {
  auto arrayVector = vector->as<ArrayVector>();
  auto& elements = arrayVector->elements();
  auto offset = arrayVector->offsetAt(row);
  auto size = arrayVector->sizeAt(row);

  std::vector<velox::variant> array;
  array.reserve(size);
  for (auto i = 0; i < size; i++) {
    auto innerRow = offset + i;
    if (elements->isNullAt(innerRow)) {
      array.emplace_back(elements->typeKind());
    } else {
      array.emplace_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          variantAt, elements->typeKind(), elements, innerRow));
    }
  }
  return velox::variant::array(array);
}

velox::variant mapVariantAt(const VectorPtr& vector, vector_size_t row) {
  auto mapVector = vector->as<MapVector>();
  auto& mapKeys = mapVector->mapKeys();
  auto& mapValues = mapVector->mapValues();
  auto offset = mapVector->offsetAt(row);
  auto size = mapVector->sizeAt(row);

  std::map<variant, variant> map;
  for (auto i = 0; i < size; i++) {
    auto innerRow = offset + i;
    auto key = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        variantAt, mapKeys->typeKind(), mapKeys, innerRow);
    velox::variant value;
    if (mapValues->isNullAt(innerRow)) {
      value = velox::variant(mapValues->typeKind());
    } else {
      value = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          variantAt, mapValues->typeKind(), mapValues, innerRow);
    }
    map.insert({key, value});
  }
  return velox::variant::map(map);
}

velox::variant rowVariantAt(const VectorPtr& vector, vector_size_t row) {
  auto rowValues = vector->as<RowVector>();
  std::vector<velox::variant> values;
  for (auto& child : rowValues->children()) {
    if (child->isNullAt(row)) {
      values.push_back(variant(child->typeKind()));
    } else if (child->typeKind() == TypeKind::ROW) {
      values.push_back(rowVariantAt(child, row));
    } else if (child->typeKind() == TypeKind::ARRAY) {
      values.push_back(arrayVariantAt(child, row));
    } else if (child->typeKind() == TypeKind::MAP) {
      values.push_back(mapVariantAt(child, row));
    } else {
      auto value = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          variantAt, child->typeKind(), child, row);
      values.push_back(value);
    }
  }
  return velox::variant::row(std::move(values));
}

std::vector<MaterializedRow> materialize(const RowVectorPtr& vector) {
  auto size = vector->size();
  std::vector<MaterializedRow> rows;
  rows.reserve(size);

  auto rowType = vector->type()->as<TypeKind::ROW>();

  for (size_t i = 0; i < size; ++i) {
    auto numColumns = rowType.size();
    MaterializedRow row;
    row.reserve(numColumns);
    for (size_t j = 0; j < numColumns; ++j) {
      auto typeKind = rowType.childAt(j)->kind();
      if (vector->childAt(j)->isNullAt(i)) {
        row.push_back(variant(typeKind));
      } else if (typeKind == TypeKind::ROW) {
        row.push_back(rowVariantAt(vector->childAt(j), i));
      } else if (typeKind == TypeKind::ARRAY) {
        row.push_back(arrayVariantAt(vector->childAt(j), i));
      } else if (typeKind == TypeKind::MAP) {
        row.push_back(mapVariantAt(vector->childAt(j), i));
      } else {
        auto value = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            variantAt, typeKind, vector->childAt(j), i);
        row.push_back(value);
      }
    }
    rows.push_back(row);
  }

  return rows;
}

MaterializedRow getColumns(
    const MaterializedRow& row,
    const std::vector<uint32_t>& columnIndices) {
  MaterializedRow columns;
  columns.reserve(columnIndices.size());
  for (auto& index : columnIndices) {
    columns.push_back(row[index]);
  }
  return columns;
}

void printRow(const MaterializedRow& row, std::ostringstream& out) {
  out << row[0];
  for (int32_t i = 1; i < row.size(); ++i) {
    out << " | " << row[i];
  }
}

std::string toString(const MaterializedRow& row) {
  std::ostringstream oss;
  printRow(row, oss);
  return oss.str();
}

std::string generateUserFriendlyDiff(
    const std::multiset<MaterializedRow>& expectedRows,
    const std::multiset<MaterializedRow>& actualRows) {
  std::vector<MaterializedRow> extraActualRows;
  std::set_difference(
      actualRows.begin(),
      actualRows.end(),
      expectedRows.begin(),
      expectedRows.end(),
      std::inserter(extraActualRows, extraActualRows.end()));

  std::vector<MaterializedRow> missingActualRows;
  std::set_difference(
      expectedRows.begin(),
      expectedRows.end(),
      actualRows.begin(),
      actualRows.end(),
      std::inserter(missingActualRows, missingActualRows.end()));

  std::ostringstream message;
  message << "Expected " << expectedRows.size() << ", got " << actualRows.size()
          << std::endl;
  message << extraActualRows.size() << " extra rows, "
          << missingActualRows.size() << " missing rows" << std::endl;

  auto extraRowsToPrint = std::min((size_t)10, extraActualRows.size());
  message << extraRowsToPrint << " of extra rows:" << std::endl;

  for (int32_t i = 0; i < extraRowsToPrint; i++) {
    message << "\t";
    printRow(extraActualRows[i], message);
    message << std::endl;
  }
  message << std::endl;

  auto missingRowsToPrint = std::min((size_t)10, missingActualRows.size());
  message << missingRowsToPrint << " of missing rows:" << std::endl;
  for (int32_t i = 0; i < missingRowsToPrint; i++) {
    message << "\t";
    printRow(missingActualRows[i], message);
    message << std::endl;
  }
  message << std::endl;
  return message.str();
}

} // namespace

void DuckDbQueryRunner::createTable(
    const std::string& name,
    const std::vector<RowVectorPtr>& data) {
  ::duckdb::Connection con(db_);
  con.Query("DROP TABLE IF EXISTS " + name);

  auto rowType = data[0]->type()->as<TypeKind::ROW>();
  auto res = con.Query(makeCreateTableSql(name, rowType));
  if (!res->success) {
    VELOX_FAIL(res->error);
  }

  for (auto& vector : data) {
    for (int32_t row = 0; row < vector->size(); row++) {
      ::duckdb::Appender appender(con, name);
      appender.BeginRow();
      for (int32_t column = 0; column < rowType.size(); column++) {
        auto columnVector = vector->childAt(column);
        if (columnVector->isNullAt(row)) {
          appender.Append(nullptr);
        } else if (rowType.childAt(column)->isArray()) {
          appender.Append(duckValueAt<TypeKind::ARRAY>(columnVector, row));
        } else {
          auto value = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
              duckValueAt, rowType.childAt(column)->kind(), columnVector, row);
          appender.Append(value);
        }
      }
      appender.EndRow();
    }
  }
}

void DuckDbQueryRunner::execute(
    const std::string& sql,
    const std::shared_ptr<const RowType>& resultRowType,
    std::function<void(std::vector<MaterializedRow>&)> resultCallback) {
  ::duckdb::Connection con(db_);
  auto duckDbResult = con.Query(sql);
  ASSERT_TRUE(duckDbResult->success)
      << "DuckDB query failed: " << duckDbResult->error << std::endl
      << sql;

  for (;;) {
    auto dataChunk = duckDbResult->Fetch();
    ASSERT_TRUE(duckDbResult->success)
        << "DuckDB query failed: " << duckDbResult->error << std::endl
        << sql;
    if (!dataChunk || (dataChunk->size() == 0)) {
      break;
    }
    auto rows = materialize(dataChunk.get(), resultRowType);
    resultCallback(rows);
  }
}

std::shared_ptr<Task> assertQuery(
    const std::shared_ptr<const core::PlanNode>& plan,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    std::optional<std::vector<uint32_t>> sortingKeys) {
  return assertQuery(
      plan, [](Task*) {}, duckDbSql, duckDbQueryRunner, sortingKeys);
}

std::shared_ptr<Task> assertQueryReturnsEmptyResult(
    const std::shared_ptr<const core::PlanNode>& plan) {
  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});

  auto totalCount = 0;
  for (const auto& vector : result.second) {
    totalCount += vector->size();
  }

  EXPECT_EQ(0, totalCount) << "Expected empty result but received "
                           << totalCount << " rows";
  return result.first->task();
}

void assertResults(
    const std::vector<RowVectorPtr>& results,
    const std::shared_ptr<const RowType>& resultType,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner) {
  std::multiset<MaterializedRow> actualRows;
  for (auto vector : results) {
    auto rows = materialize(vector);
    std::copy(
        rows.begin(), rows.end(), std::inserter(actualRows, actualRows.end()));
  }

  auto expectedRows = duckDbQueryRunner.execute(duckDbSql, resultType);
  if (actualRows != expectedRows) {
    auto message = generateUserFriendlyDiff(expectedRows, actualRows);
    EXPECT_TRUE(false) << message << "DuckDB query: " << duckDbSql;
  }
}

void assertResultsOrdered(
    const std::vector<RowVectorPtr>& results,
    const std::shared_ptr<const RowType>& resultType,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    const std::vector<uint32_t>& sortingKeys) {
  // To handle the case when the sorting keys are not unique and the order
  // within the same key is not deterministic, we partition the results with
  // sorting keys and verify the partitions are equal using set-diff.
  using OrderedPartition =
      std::pair<MaterializedRow, std::multiset<MaterializedRow>>;

  std::vector<OrderedPartition> actualPartitions;
  std::vector<OrderedPartition> expectedPartitions;

  for (auto vector : results) {
    auto rows = materialize(vector);
    for (auto row : rows) {
      auto keys = getColumns(row, sortingKeys);
      if (actualPartitions.empty() || actualPartitions.back().first != keys) {
        actualPartitions.emplace_back(keys, std::multiset<MaterializedRow>());
      }
      actualPartitions.back().second.insert(row);
    }
  }

  auto expectedRows = duckDbQueryRunner.executeOrdered(duckDbSql, resultType);
  for (auto row : expectedRows) {
    auto keys = getColumns(row, sortingKeys);
    if (expectedPartitions.empty() || expectedPartitions.back().first != keys) {
      expectedPartitions.emplace_back(keys, std::multiset<MaterializedRow>());
    }
    expectedPartitions.back().second.insert(row);
  }

  if (expectedPartitions != actualPartitions) {
    auto actualPartIter = actualPartitions.begin();
    auto expectedPartIter = expectedPartitions.begin();
    while (expectedPartIter != expectedPartitions.end() &&
           actualPartIter != actualPartitions.end()) {
      if (*expectedPartIter != *actualPartIter) {
        break;
      }
      ++expectedPartIter;
      ++actualPartIter;
    }
    std::ostringstream oss;
    if (expectedPartIter == expectedPartitions.end()) {
      oss << "Got extra rows: keys: " << toString(actualPartIter->first)
          << std::endl;
    } else if (actualPartIter == actualPartitions.end()) {
      oss << "Missing rows: keys: " << toString(expectedPartIter->first)
          << std::endl;
    } else {
      if (actualPartIter->first != expectedPartIter->first) {
        oss << "Expected keys: " << toString(expectedPartIter->first)
            << ", actual: " << toString(actualPartIter->first) << std::endl;
      } else {
        oss << "Keys: " << toString(expectedPartIter->first) << " ";
        oss << generateUserFriendlyDiff(
            expectedPartIter->second, actualPartIter->second);
      }
      EXPECT_TRUE(false) << oss.str() << "DuckDB query: " << duckDbSql;
    }
  }
}

std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> readCursor(
    const CursorParameters& params,
    std::function<void(exec::Task*)> addSplits) {
  std::vector<RowVectorPtr> result;
  auto cursor = std::make_unique<TaskCursor>(params);
  addSplits(cursor->task().get());

  while (cursor->moveNext()) {
    result.push_back(cursor->current());
    addSplits(cursor->task().get());
  }
  return {std::move(cursor), std::move(result)};
}

std::shared_ptr<Task> assertQuery(
    const std::shared_ptr<const core::PlanNode>& plan,
    std::function<void(exec::Task*)> addSplits,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    std::optional<std::vector<uint32_t>> sortingKeys) {
  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, addSplits);
  if (sortingKeys) {
    assertResultsOrdered(
        result.second,
        params.planNode->outputType(),
        duckDbSql,
        duckDbQueryRunner,
        sortingKeys.value());
  } else {
    assertResults(
        result.second,
        params.planNode->outputType(),
        duckDbSql,
        duckDbQueryRunner);
  }
  return result.first->task();
}

std::shared_ptr<Task> assertQuery(
    const CursorParameters& params,
    std::function<void(exec::Task*)> addSplits,
    const std::string& duckDbSql,
    DuckDbQueryRunner& duckDbQueryRunner,
    std::optional<std::vector<uint32_t>> sortingKeys) {
  auto cursor = std::make_unique<TaskCursor>(params);
  addSplits(cursor->task().get());

  std::vector<RowVectorPtr> actualResults;
  while (cursor->moveNext()) {
    actualResults.push_back(cursor->current());
    addSplits(cursor->task().get());
  }

  if (sortingKeys) {
    assertResultsOrdered(
        actualResults,
        params.planNode->outputType(),
        duckDbSql,
        duckDbQueryRunner,
        sortingKeys.value());
  } else {
    assertResults(
        actualResults,
        params.planNode->outputType(),
        duckDbSql,
        duckDbQueryRunner);
  }
  return cursor->task();
}

std::shared_ptr<Task> assertQuery(
    const std::shared_ptr<const core::PlanNode>& plan,
    const std::vector<RowVectorPtr>& expectedResults) {
  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});

  assertEqualResults(expectedResults, result.second);
  return result.first->task();
}

velox::variant readSingleValue(
    const std::shared_ptr<const core::PlanNode>& plan) {
  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});

  EXPECT_EQ(1, result.second.size());
  EXPECT_EQ(1, result.second[0]->size());
  return materialize(result.second[0])[0][0];
}

void assertEqualResults(
    const std::vector<RowVectorPtr>& expected,
    const std::vector<RowVectorPtr>& actual) {
  std::multiset<MaterializedRow> actualRows;
  for (auto vector : actual) {
    auto rows = materialize(vector);
    std::copy(
        rows.begin(), rows.end(), std::inserter(actualRows, actualRows.end()));
  }

  std::multiset<MaterializedRow> expectedRows;
  for (auto vector : expected) {
    auto rows = materialize(vector);
    std::copy(
        rows.begin(),
        rows.end(),
        std::inserter(expectedRows, expectedRows.end()));
  }

  if (actualRows != expectedRows) {
    auto message = generateUserFriendlyDiff(expectedRows, actualRows);
    EXPECT_TRUE(false) << message << "Unexpected results";
  }
}

} // namespace facebook::velox::exec::test
