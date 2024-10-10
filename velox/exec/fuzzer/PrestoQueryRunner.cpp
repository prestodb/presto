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

#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include <cpr/cpr.h> // @manual
#include <folly/json.h>
#include <iostream>
#include "velox/common/base/Fs.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/ToSQLUtil.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/parser/TypeParser.h"

#include <utility>

using namespace facebook::velox;

namespace facebook::velox::exec::test {
namespace {

void writeToFile(
    const std::string& path,
    const std::vector<RowVectorPtr>& data,
    memory::MemoryPool* pool) {
  VELOX_CHECK_GT(data.size(), 0);

  std::shared_ptr<dwio::common::WriterOptions> options =
      std::make_shared<dwrf::WriterOptions>();
  options->schema = data[0]->type();
  options->memoryPool = pool;

  auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  auto writer = dwio::common::getWriterFactory(dwio::common::FileFormat::DWRF)
                    ->createWriter(std::move(sink), options);
  for (const auto& vector : data) {
    writer->write(vector);
  }
  writer->close();
}

std::unique_ptr<ByteInputStream> toByteStream(const std::string& input) {
  std::vector<ByteRange> ranges;
  ranges.push_back(
      {reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
       (int32_t)input.length(),
       0});
  return std::make_unique<BufferInputStream>(std::move(ranges));
}

RowVectorPtr deserialize(
    const RowTypePtr& rowType,
    const std::string& input,
    memory::MemoryPool* pool) {
  auto byteStream = toByteStream(input);
  auto serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
  RowVectorPtr result;
  serde->deserialize(byteStream.get(), pool, rowType, &result, nullptr);
  return result;
}

class ServerResponse {
 public:
  explicit ServerResponse(const std::string& responseJson)
      : response_(folly::parseJson(responseJson)) {}

  void throwIfFailed() const {
    if (response_.count("error") == 0) {
      return;
    }

    const auto& error = response_["error"];

    VELOX_FAIL(
        "Presto query failed: {} {}",
        error["errorCode"].asInt(),
        error["message"].asString());
  }

  std::string queryId() const {
    return response_["id"].asString();
  }

  bool queryCompleted() const {
    return response_.count("nextUri") == 0;
  }

  std::string nextUri() const {
    return response_["nextUri"].asString();
  }

  std::vector<RowVectorPtr> queryResults(memory::MemoryPool* pool) const {
    if (!response_.count("binaryData")) {
      return {};
    }

    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (const auto& column : response_["columns"]) {
      names.push_back(column["name"].asString());
      types.push_back(parseType(column["type"].asString()));
    }

    auto rowType = ROW(std::move(names), std::move(types));

    // There should be a single column with possibly multiple rows. Each row
    // contains base64-encoded PrestoPage of results.

    std::vector<RowVectorPtr> vectors;
    for (auto& encodedData : response_["binaryData"]) {
      const std::string data =
          encoding::Base64::decode(encodedData.stringPiece());
      vectors.push_back(deserialize(rowType, data, pool));
    }
    return vectors;
  }

 private:
  folly::dynamic response_;
};

} // namespace

PrestoQueryRunner::PrestoQueryRunner(
    memory::MemoryPool* pool,
    std::string coordinatorUri,
    std::string user,
    std::chrono::milliseconds timeout)
    : ReferenceQueryRunner(pool),
      coordinatorUri_{std::move(coordinatorUri)},
      user_{std::move(user)},
      timeout_(timeout) {
  eventBaseThread_.start("PrestoQueryRunner");
  pool_ = aggregatePool()->addLeafChild("leaf");
  queryRunnerContext_ = std::make_shared<QueryRunnerContext>();
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const core::PlanNodePtr& plan) {
  if (const auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(plan)) {
    return toSql(projectNode);
  }

  if (const auto windowNode =
          std::dynamic_pointer_cast<const core::WindowNode>(plan)) {
    return toSql(windowNode);
  }

  if (const auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(plan)) {
    return toSql(aggregationNode);
  }

  if (const auto rowNumberNode =
          std::dynamic_pointer_cast<const core::RowNumberNode>(plan)) {
    return toSql(rowNumberNode);
  }

  if (auto tableWriteNode =
          std::dynamic_pointer_cast<const core::TableWriteNode>(plan)) {
    return toSql(tableWriteNode);
  }

  if (const auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(plan)) {
    return toSql(joinNode);
  }

  if (const auto joinNode =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(plan)) {
    return toSql(joinNode);
  }

  if (const auto valuesNode =
          std::dynamic_pointer_cast<const core::ValuesNode>(plan)) {
    return toSql(valuesNode);
  }

  VELOX_NYI();
}

namespace {

std::string toWindowCallSql(
    const core::CallTypedExprPtr& call,
    bool ignoreNulls = false) {
  std::stringstream sql;
  sql << call->name() << "(";
  toCallInputsSql(call->inputs(), sql);
  sql << ")";
  if (ignoreNulls) {
    sql << " IGNORE NULLS";
  }
  return sql.str();
}

bool isSupportedDwrfType(const TypePtr& type) {
  if (type->isDate() || type->isIntervalDayTime() || type->isUnKnown()) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isSupportedDwrfType(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

} // namespace

const std::vector<TypePtr>& PrestoQueryRunner::supportedScalarTypes() const {
  static const std::vector<TypePtr> kScalarTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
  };
  return kScalarTypes;
}

const std::unordered_map<std::string, DataSpec>&
PrestoQueryRunner::aggregationFunctionDataSpecs() const {
  // For some functions, velox supports NaN, Infinity better than presto query
  // runner, which makes the comparison impossible.
  // Add data constraint in vector fuzzer to enforce to not generate such data
  // for those functions before they are fixed in presto query runner
  static const std::unordered_map<std::string, DataSpec>
      kAggregationFunctionDataSpecs{
          {"regr_avgx", DataSpec{false, false}},
          {"regr_avgy", DataSpec{false, false}},
          {"regr_r2", DataSpec{false, false}},
          {"regr_sxx", DataSpec{false, false}},
          {"regr_syy", DataSpec{false, false}},
          {"regr_sxy", DataSpec{false, false}},
          {"regr_slope", DataSpec{false, false}},
          {"regr_replacement", DataSpec{false, false}},
          {"covar_pop", DataSpec{true, false}},
          {"covar_samp", DataSpec{true, false}},
      };

  return kAggregationFunctionDataSpecs;
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::AggregationNode>& aggregationNode) {
  // Assume plan is Aggregation over Values.
  VELOX_CHECK(aggregationNode->step() == core::AggregationNode::Step::kSingle);

  if (!isSupportedDwrfType(aggregationNode->sources()[0]->outputType())) {
    return std::nullopt;
  }

  std::vector<std::string> groupingKeys;
  for (const auto& key : aggregationNode->groupingKeys()) {
    groupingKeys.push_back(key->name());
  }

  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", groupingKeys);

  const auto& aggregates = aggregationNode->aggregates();
  if (!aggregates.empty()) {
    if (!groupingKeys.empty()) {
      sql << ", ";
    }

    for (auto i = 0; i < aggregates.size(); ++i) {
      appendComma(i, sql);
      const auto& aggregate = aggregates[i];
      sql << toAggregateCallSql(
          aggregate.call,
          aggregate.sortingKeys,
          aggregate.sortingOrders,
          aggregate.distinct);

      if (aggregate.mask != nullptr) {
        sql << " filter (where " << aggregate.mask->name() << ")";
      }
      sql << " as " << aggregationNode->aggregateNames()[i];
    }
  }

  sql << " FROM tmp";

  if (!groupingKeys.empty()) {
    sql << " GROUP BY " << folly::join(", ", groupingKeys);
  }

  return sql.str();
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::ProjectNode>& projectNode) {
  auto sourceSql = toSql(projectNode->sources()[0]);
  if (!sourceSql.has_value()) {
    return std::nullopt;
  }

  std::stringstream sql;
  sql << "SELECT ";

  for (auto i = 0; i < projectNode->names().size(); ++i) {
    appendComma(i, sql);
    auto projection = projectNode->projections()[i];
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                projection)) {
      sql << field->name();
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(projection)) {
      sql << toCallSql(call);
    } else if (
        auto cast =
            std::dynamic_pointer_cast<const core::CastTypedExpr>(projection)) {
      sql << toCastSql(cast);
    } else if (
        auto concat = std::dynamic_pointer_cast<const core::ConcatTypedExpr>(
            projection)) {
      sql << toConcatSql(concat);
    } else if (
        auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                projection)) {
      if (constant->type()->isPrimitiveType()) {
        sql << toConstantSql(constant);
      } else {
        // TODO: support complex-typed constant literals.
        VELOX_NYI();
      }
    } else {
      VELOX_NYI();
    }

    sql << " as " << projectNode->names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  return sql.str();
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::WindowNode>& windowNode) {
  if (!isSupportedDwrfType(windowNode->sources()[0]->outputType())) {
    return std::nullopt;
  }

  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = windowNode->sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", ";

  const auto& functions = windowNode->windowFunctions();
  for (auto i = 0; i < functions.size(); ++i) {
    appendComma(i, sql);
    sql << toWindowCallSql(functions[i].functionCall, functions[i].ignoreNulls);

    sql << " OVER (";

    const auto& partitionKeys = windowNode->partitionKeys();
    if (!partitionKeys.empty()) {
      sql << "PARTITION BY ";
      for (auto j = 0; j < partitionKeys.size(); ++j) {
        appendComma(j, sql);
        sql << partitionKeys[j]->name();
      }
    }

    const auto& sortingKeys = windowNode->sortingKeys();
    const auto& sortingOrders = windowNode->sortingOrders();

    if (!sortingKeys.empty()) {
      sql << " ORDER BY ";
      for (auto j = 0; j < sortingKeys.size(); ++j) {
        appendComma(j, sql);
        sql << sortingKeys[j]->name() << " " << sortingOrders[j].toString();
      }
    }

    sql << " " << queryRunnerContext_->windowFrames_.at(windowNode->id()).at(i);
    sql << ")";
  }

  sql << " FROM tmp";

  return sql.str();
}

bool PrestoQueryRunner::isConstantExprSupported(
    const core::TypedExprPtr& expr) {
  if (std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    // TODO: support constant literals of these types. Complex-typed constant
    // literals require support of converting them to SQL. Json can be enabled
    // after we're able to generate valid Json strings, because when Json is
    // used as the type of constant literals in SQL, Presto implicitly invoke
    // json_parse() on it, which makes the behavior of Presto different from
    // Velox. Timestamp constant literals require further investigation to
    // ensure Presto uses the same timezone as Velox.
    auto& type = expr->type();
    return type->isPrimitiveType() && !type->isTimestamp() && !isJsonType(type);
  }
  return true;
}

bool PrestoQueryRunner::isSupported(const exec::FunctionSignature& signature) {
  // TODO: support queries with these types. Among the types below, hugeint is
  // not a native type in Presto, so fuzzer should not use it as the type of
  // cast-to or constant literals. Hyperloglog can only be casted from varbinary
  // and cannot be used as the type of constant literals. Interval year to month
  // can only be casted from NULL and cannot be used as the type of constant
  // literals.
  return !(
      usesTypeName(signature, "interval year to month") ||
      usesTypeName(signature, "hugeint") ||
      usesTypeName(signature, "hyperloglog"));
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::RowNumberNode>& rowNumberNode) {
  if (!isSupportedDwrfType(rowNumberNode->sources()[0]->outputType())) {
    return std::nullopt;
  }

  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = rowNumberNode->sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", row_number() OVER (";

  const auto& partitionKeys = rowNumberNode->partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  sql << ") as row_number FROM tmp";

  return sql.str();
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::TableWriteNode>& tableWriteNode) {
  auto insertTableHandle =
      std::dynamic_pointer_cast<connector::hive::HiveInsertTableHandle>(
          tableWriteNode->insertTableHandle()->connectorInsertTableHandle());

  // Returns a CTAS sql with specified table properties from TableWriteNode,
  // example sql:
  // CREATE TABLE tmp_write WITH (
  // PARTITIONED_BY = ARRAY['p0'],
  // BUCKETED_COUNT = 2, BUCKETED_BY = ARRAY['b0', 'b1'],
  // SORTED_BY = ARRAY['s0 ASC', 's1 DESC'],
  // FORMAT = 'ORC'
  // )
  // AS SELECT * FROM tmp
  std::stringstream sql;
  sql << "CREATE TABLE tmp_write";
  std::vector<std::string> partitionKeys;
  for (auto i = 0; i < tableWriteNode->columnNames().size(); ++i) {
    if (insertTableHandle->inputColumns()[i]->isPartitionKey()) {
      partitionKeys.push_back(insertTableHandle->inputColumns()[i]->name());
    }
  }
  sql << " WITH (";

  if (insertTableHandle->isPartitioned()) {
    sql << " PARTITIONED_BY = ARRAY[";
    for (int i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << "'" << partitionKeys[i] << "'";
    }
    sql << "], ";

    if (insertTableHandle->bucketProperty() != nullptr) {
      const auto bucketCount =
          insertTableHandle->bucketProperty()->bucketCount();
      const auto bucketColumns =
          insertTableHandle->bucketProperty()->bucketedBy();
      sql << " BUCKET_COUNT = " << bucketCount << ", BUCKETED_BY = ARRAY[";
      for (int i = 0; i < bucketColumns.size(); ++i) {
        appendComma(i, sql);
        sql << "'" << bucketColumns[i] << "'";
      }
      sql << "], ";

      const auto sortColumns = insertTableHandle->bucketProperty()->sortedBy();
      if (!sortColumns.empty()) {
        sql << " SORTED_BY = ARRAY[";
        for (int i = 0; i < sortColumns.size(); ++i) {
          appendComma(i, sql);
          sql << "'" << sortColumns[i]->sortColumn() << " "
              << (sortColumns[i]->sortOrder().isAscending() ? "ASC" : "DESC")
              << "'";
        }
        sql << "], ";
      }
    }
  }

  sql << "FORMAT = 'ORC')  AS SELECT * FROM tmp";
  return sql.str();
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  if (!isSupportedDwrfType(joinNode->sources()[0]->outputType())) {
    return std::nullopt;
  }

  if (!isSupportedDwrfType(joinNode->sources()[1]->outputType())) {
    return std::nullopt;
  }

  const auto joinKeysToSql = [](auto keys) {
    std::stringstream out;
    for (auto i = 0; i < keys.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << keys[i]->name();
    }
    return out.str();
  };

  const auto equiClausesToSql = [](auto joinNode) {
    std::stringstream out;
    for (auto i = 0; i < joinNode->leftKeys().size(); ++i) {
      if (i > 0) {
        out << " AND ";
      }
      out << joinNode->leftKeys()[i]->name() << " = "
          << joinNode->rightKeys()[i]->name();
    }
    return out.str();
  };

  const auto& outputNames = joinNode->outputType()->names();

  std::stringstream sql;
  if (joinNode->isLeftSemiProjectJoin()) {
    sql << "SELECT "
        << folly::join(", ", outputNames.begin(), --outputNames.end());
  } else {
    sql << "SELECT " << folly::join(", ", outputNames);
  }

  switch (joinNode->joinType()) {
    case core::JoinType::kInner:
      sql << " FROM t INNER JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kLeft:
      sql << " FROM t LEFT JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kFull:
      sql << " FROM t FULL OUTER JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kLeftSemiFilter:
      if (joinNode->leftKeys().size() > 1) {
        return std::nullopt;
      }
      sql << " FROM t WHERE " << joinKeysToSql(joinNode->leftKeys())
          << " IN (SELECT " << joinKeysToSql(joinNode->rightKeys())
          << " FROM u)";
      break;
    case core::JoinType::kLeftSemiProject:
      if (joinNode->isNullAware()) {
        sql << ", " << joinKeysToSql(joinNode->leftKeys()) << " IN (SELECT "
            << joinKeysToSql(joinNode->rightKeys()) << " FROM u) FROM t";
      } else {
        sql << ", EXISTS (SELECT * FROM u WHERE " << equiClausesToSql(joinNode)
            << ") FROM t";
      }
      break;
    case core::JoinType::kAnti:
      if (joinNode->isNullAware()) {
        sql << " FROM t WHERE " << joinKeysToSql(joinNode->leftKeys())
            << " NOT IN (SELECT " << joinKeysToSql(joinNode->rightKeys())
            << " FROM u)";
      } else {
        sql << " FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE "
            << equiClausesToSql(joinNode) << ")";
      }
      break;
    default:
      VELOX_UNREACHABLE(
          "Unknown join type: {}", static_cast<int>(joinNode->joinType()));
  }

  return sql.str();
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::NestedLoopJoinNode>& joinNode) {
  const auto& joinKeysToSql = [](auto keys) {
    std::stringstream out;
    for (auto i = 0; i < keys.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << keys[i]->name();
    }
    return out.str();
  };

  const auto& outputNames = joinNode->outputType()->names();
  std::stringstream sql;

  // Nested loop join without filter.
  VELOX_CHECK(
      joinNode->joinCondition() == nullptr,
      "This code path should be called only for nested loop join without filter");
  const std::string joinCondition{"(1 = 1)"};
  switch (joinNode->joinType()) {
    case core::JoinType::kInner:
      sql << " FROM t INNER JOIN u ON " << joinCondition;
      break;
    case core::JoinType::kLeft:
      sql << " FROM t LEFT JOIN u ON " << joinCondition;
      break;
    case core::JoinType::kFull:
      sql << " FROM t FULL OUTER JOIN u ON " << joinCondition;
      break;
    default:
      VELOX_UNREACHABLE(
          "Unknown join type: {}", static_cast<int>(joinNode->joinType()));
  }

  return sql.str();
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const std::shared_ptr<const core::ValuesNode>& valuesNode) {
  if (!isSupportedDwrfType(valuesNode->outputType())) {
    return std::nullopt;
  }
  return "tmp";
}

std::multiset<std::vector<variant>> PrestoQueryRunner::execute(
    const std::string& sql,
    const std::vector<RowVectorPtr>& input,
    const RowTypePtr& resultType) {
  return exec::test::materialize(executeVector(sql, input, resultType));
}

std::multiset<std::vector<velox::variant>> PrestoQueryRunner::execute(
    const std::string& sql,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const RowTypePtr& resultType) {
  return exec::test::materialize(
      executeVector(sql, probeInput, buildInput, resultType));
}

std::string PrestoQueryRunner::createTable(
    const std::string& name,
    const TypePtr& type) {
  auto inputType = asRowType(type);
  std::stringstream nullValues;
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, nullValues);
    nullValues << fmt::format(
        "cast(null as {})", toTypeSql(inputType->childAt(i)));
  }

  execute(fmt::format("DROP TABLE IF EXISTS {}", name));

  execute(fmt::format(
      "CREATE TABLE {}({}) WITH (format = 'DWRF') AS SELECT {}",
      name,
      folly::join(", ", inputType->names()),
      nullValues.str()));

  // Query Presto to find out table's location on disk.
  auto results = execute(fmt::format("SELECT \"$path\" FROM {}", name));

  auto filePath = extractSingleValue<StringView>(results);
  auto tableDirectoryPath = fs::path(filePath).parent_path();

  // Delete the all-null row.
  execute(fmt::format("DELETE FROM {}", name));

  return tableDirectoryPath;
}

std::vector<velox::RowVectorPtr> PrestoQueryRunner::executeVector(
    const std::string& sql,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const velox::RowTypePtr& resultType) {
  auto probeType = asRowType(probeInput[0]->type());
  if (probeType->size() == 0) {
    auto rowVector = makeNullRows(probeInput, "x", pool());
    return executeVector(sql, {rowVector}, buildInput, resultType);
  }

  auto buildType = asRowType(buildInput[0]->type());
  if (probeType->size() == 0) {
    auto rowVector = makeNullRows(buildInput, "y", pool());
    return executeVector(sql, probeInput, {rowVector}, resultType);
  }

  auto probeTableDirectoryPath = createTable("t", probeInput[0]->type());
  auto buildTableDirectoryPath = createTable("u", buildInput[0]->type());

  // Create a new file in table's directory with fuzzer-generated data.
  auto probeFilePath = fs::path(probeTableDirectoryPath)
                           .append("probe.dwrf")
                           .string()
                           .substr(strlen("file:"));

  auto buildFilePath = fs::path(buildTableDirectoryPath)
                           .append("build.dwrf")
                           .string()
                           .substr(strlen("file:"));

  auto writerPool = aggregatePool()->addAggregateChild("writer");
  writeToFile(probeFilePath, probeInput, writerPool.get());
  writeToFile(buildFilePath, buildInput, writerPool.get());

  // Run the query.
  return execute(sql);
}

std::vector<velox::RowVectorPtr> PrestoQueryRunner::executeVector(
    const std::string& sql,
    const std::vector<velox::RowVectorPtr>& input,
    const velox::RowTypePtr& resultType) {
  auto inputType = asRowType(input[0]->type());
  if (inputType->size() == 0) {
    auto rowVector = makeNullRows(input, "x", pool());
    return executeVector(sql, {rowVector}, resultType);
  }

  auto tableDirectoryPath = createTable("tmp", input[0]->type());

  // Create a new file in table's directory with fuzzer-generated data.
  auto newFilePath = fs::path(tableDirectoryPath)
                         .append("fuzzer.dwrf")
                         .string()
                         .substr(strlen("file:"));

  auto writerPool = aggregatePool()->addAggregateChild("writer");
  writeToFile(newFilePath, input, writerPool.get());

  // Run the query.
  return execute(sql);
}

std::vector<RowVectorPtr> PrestoQueryRunner::execute(const std::string& sql) {
  return execute(sql, "");
}

std::vector<RowVectorPtr> PrestoQueryRunner::execute(
    const std::string& sql,
    const std::string& sessionProperty) {
  LOG(INFO) << "Execute presto sql: " << sql;
  auto response = ServerResponse(startQuery(sql, sessionProperty));
  response.throwIfFailed();

  std::vector<RowVectorPtr> queryResults;
  for (;;) {
    for (auto& result : response.queryResults(pool_.get())) {
      queryResults.push_back(result);
    }

    if (response.queryCompleted()) {
      break;
    }

    response = ServerResponse(fetchNext(response.nextUri()));
    response.throwIfFailed();
  }

  return queryResults;
}

std::string PrestoQueryRunner::startQuery(
    const std::string& sql,
    const std::string& sessionProperty) {
  auto uri = fmt::format("{}/v1/statement?binaryResults=true", coordinatorUri_);
  cpr::Url url{uri};
  cpr::Body body{sql};
  cpr::Header header(
      {{"X-Presto-User", user_},
       {"X-Presto-Catalog", "hive"},
       {"X-Presto-Schema", "tpch"},
       {"Content-Type", "text/plain"},
       {"X-Presto-Session", sessionProperty}});
  cpr::Timeout timeout{timeout_};
  cpr::Response response = cpr::Post(url, body, header, timeout);
  VELOX_CHECK_EQ(
      response.status_code,
      200,
      "POST to {} failed: {}",
      uri,
      response.error.message);
  return response.text;
}

std::string PrestoQueryRunner::fetchNext(const std::string& nextUri) {
  cpr::Url url(nextUri);
  cpr::Header header({{"X-Presto-Client-Binary-Results", "true"}});
  cpr::Timeout timeout{timeout_};
  cpr::Response response = cpr::Get(url, header, timeout);
  VELOX_CHECK_EQ(
      response.status_code,
      200,
      "GET from {} failed: {}",
      nextUri,
      response.error.message);
  return response.text;
}

bool PrestoQueryRunner::supportsVeloxVectorResults() const {
  return true;
}

} // namespace facebook::velox::exec::test
