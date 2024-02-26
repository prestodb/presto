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
#include "velox/core/Expressions.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/parser/TypeParser.h"

#include <utility>

using namespace facebook::velox;

namespace facebook::velox::exec::test {

namespace {

template <typename T>
T extractSingleValue(const std::vector<RowVectorPtr>& data) {
  VELOX_CHECK_EQ(1, data.size());
  VELOX_CHECK_EQ(1, data[0]->childrenSize());

  auto simpleVector = data[0]->childAt(0)->as<SimpleVector<T>>();
  VELOX_CHECK(!simpleVector->isNullAt(0));
  return simpleVector->valueAt(0);
}

void writeToFile(
    const std::string& path,
    const std::vector<RowVectorPtr>& data,
    memory::MemoryPool* pool) {
  VELOX_CHECK_GT(data.size(), 0);

  dwio::common::WriterOptions options;
  options.schema = data[0]->type();
  options.memoryPool = pool;

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

ByteInputStream toByteStream(const std::string& input) {
  std::vector<ByteRange> ranges;
  ranges.push_back(
      {reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
       (int32_t)input.length(),
       0});
  return ByteInputStream(std::move(ranges));
}

RowVectorPtr deserialize(
    const RowTypePtr& rowType,
    const std::string& input,
    memory::MemoryPool* pool) {
  auto byteStream = toByteStream(input);
  auto serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
  RowVectorPtr result;
  serde->deserialize(&byteStream, pool, rowType, &result, nullptr);
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
    std::string coordinatorUri,
    std::string user,
    std::chrono::milliseconds timeout)
    : coordinatorUri_{std::move(coordinatorUri)},
      user_{std::move(user)},
      timeout_(timeout) {
  eventBaseThread_.start("PrestoQueryRunner");
}

std::optional<std::string> PrestoQueryRunner::toSql(
    const core::PlanNodePtr& plan) {
  if (auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(plan)) {
    return toSql(projectNode);
  }

  if (auto windowNode =
          std::dynamic_pointer_cast<const core::WindowNode>(plan)) {
    return toSql(windowNode);
  }

  if (auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(plan)) {
    return toSql(aggregationNode);
  }

  VELOX_NYI();
}

namespace {

void appendComma(int32_t i, std::stringstream& sql) {
  if (i > 0) {
    sql << ", ";
  }
}

std::string toTypeSql(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::ARRAY:
      return fmt::format("array({})", toTypeSql(type->childAt(0)));
    case TypeKind::MAP:
      return fmt::format(
          "map({}, {})",
          toTypeSql(type->childAt(0)),
          toTypeSql(type->childAt(1)));
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      std::stringstream sql;
      sql << "row(";
      for (auto i = 0; i < type->size(); ++i) {
        appendComma(i, sql);
        sql << rowType.nameOf(i) << " ";
        sql << toTypeSql(type->childAt(i));
      }
      sql << ")";
      return sql.str();
    }
    default:
      if (type->isPrimitiveType()) {
        return type->toString();
      }
      VELOX_UNSUPPORTED("Type is not supported: {}", type->toString());
  }
}

std::string toCallSql(const core::CallTypedExprPtr& call);

void toCallInputsSql(
    const std::vector<core::TypedExprPtr>& inputs,
    std::stringstream& sql) {
  for (auto i = 0; i < inputs.size(); ++i) {
    appendComma(i, sql);

    const auto& input = inputs.at(i);
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                input)) {
      sql << field->name();
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(input)) {
      sql << toCallSql(call);
    } else if (
        auto lambda =
            std::dynamic_pointer_cast<const core::LambdaTypedExpr>(input)) {
      const auto& signature = lambda->signature();
      const auto& body =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(lambda->body());
      VELOX_CHECK_NOT_NULL(body);

      sql << "(";
      for (auto j = 0; j < signature->size(); ++j) {
        appendComma(j, sql);
        sql << signature->nameOf(j);
      }

      sql << ") -> " << toCallSql(body);
    } else {
      VELOX_NYI();
    }
  }
}

std::string toCallSql(const core::CallTypedExprPtr& call) {
  std::stringstream sql;
  sql << call->name() << "(";
  toCallInputsSql(call->inputs(), sql);
  sql << ")";
  return sql.str();
}

std::string toAggregateCallSql(
    const core::CallTypedExprPtr& call,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    bool distinct) {
  VELOX_CHECK_EQ(sortingKeys.size(), sortingOrders.size());
  std::stringstream sql;
  sql << call->name() << "(";

  if (distinct) {
    sql << "distinct ";
  }

  toCallInputsSql(call->inputs(), sql);

  if (!sortingKeys.empty()) {
    sql << " ORDER BY ";

    for (int i = 0; i < sortingKeys.size(); i++) {
      appendComma(i, sql);
      sql << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
    }
  }

  sql << ")";
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
    } else {
      VELOX_NYI();
    }

    sql << " as " << projectNode->names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  return sql.str();
}

namespace {

void appendWindowFrame(
    const core::WindowNode::Frame& frame,
    std::stringstream& sql) {
  // TODO: Add support for k Range Frames by retrieving the original range bound
  // from WindowNode.
  switch (frame.type) {
    case core::WindowNode::WindowType::kRange:
      sql << " RANGE";
      break;
    case core::WindowNode::WindowType::kRows:
      sql << " ROWS";
      break;
    default:
      VELOX_UNREACHABLE();
  }
  sql << " BETWEEN";

  auto appendBound = [&sql](
                         const core::WindowNode::BoundType& bound,
                         const core::TypedExprPtr& value) {
    switch (bound) {
      case core::WindowNode::BoundType::kUnboundedPreceding:
        sql << " UNBOUNDED PRECEDING";
        break;
      case core::WindowNode::BoundType::kUnboundedFollowing:
        sql << " UNBOUNDED FOLLOWING";
        break;
      case core::WindowNode::BoundType::kCurrentRow:
        sql << " CURRENT ROW";
        break;
      case core::WindowNode::BoundType::kPreceding:
        sql << " " << value->toString() << " PRECEDING";
        break;
      case core::WindowNode::BoundType::kFollowing:
        sql << " " << value->toString() << " FOLLOWING";
        break;
      default:
        VELOX_UNREACHABLE();
    }
  };

  appendBound(frame.startType, frame.startValue);
  sql << " AND";
  appendBound(frame.endType, frame.endValue);
}

} // namespace

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
    sql << toCallSql(functions[i].functionCall);

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

    appendWindowFrame(functions[i].frame, sql);
    sql << ")";
  }

  sql << " FROM tmp";

  return sql.str();
}

std::multiset<std::vector<variant>> PrestoQueryRunner::execute(
    const std::string& sql,
    const std::vector<RowVectorPtr>& input,
    const RowTypePtr& resultType) {
  auto inputType = asRowType(input[0]->type());
  if (inputType->size() == 0) {
    // The query doesn't need to read any columns, but it needs to see a
    // specific number of rows. Make new 'input' as single all-null BIGINT
    // column with as many rows as original input. This way we'll be able to
    // create a 'tmp' table will the necessary number of rows.
    vector_size_t numInput = 0;
    for (const auto& v : input) {
      numInput += v->size();
    }

    auto column = BaseVector::createNullConstant(BIGINT(), numInput, pool());
    auto rowVector = std::make_shared<RowVector>(
        pool(),
        ROW({"x"}, {BIGINT()}),
        nullptr,
        numInput,
        std::vector<VectorPtr>{column});
    return execute(sql, {rowVector}, resultType);
  }

  // Create tmp table in Presto using DWRF file format and add a single
  // all-null row to it.

  std::stringstream nullValues;
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, nullValues);
    nullValues << fmt::format(
        "cast(null as {})", toTypeSql(inputType->childAt(i)));
  }

  execute("DROP TABLE IF EXISTS tmp");

  execute(fmt::format(
      "CREATE TABLE tmp({}) WITH (format = 'DWRF') AS SELECT {}",
      folly::join(", ", inputType->names()),
      nullValues.str()));

  // Query Presto to find out table's location on disk.
  auto results = execute("SELECT \"$path\" FROM tmp");

  auto filePath = extractSingleValue<StringView>(results);
  auto tableDirectoryPath = fs::path(filePath).parent_path();

  // Delete the all-null row.
  execute("DELETE FROM tmp");

  // Create a new file in table's directory with fuzzer-generated data.
  auto newFilePath = fs::path(tableDirectoryPath)
                         .append("fuzzer.dwrf")
                         .string()
                         .substr(strlen("file:"));

  auto writerPool = rootPool()->addAggregateChild("writer");
  writeToFile(newFilePath, input, writerPool.get());

  // Run the query.
  results = execute(sql);

  return exec::test::materialize(results);
}

std::vector<RowVectorPtr> PrestoQueryRunner::execute(const std::string& sql) {
  auto response = ServerResponse(startQuery(sql));
  response.throwIfFailed();

  vector_size_t numResults = 0;
  std::vector<RowVectorPtr> queryResults;
  for (;;) {
    for (auto& result : response.queryResults(pool_.get())) {
      queryResults.push_back(result);
      numResults += result->size();
    }

    if (response.queryCompleted()) {
      break;
    }

    response = ServerResponse(fetchNext(response.nextUri()));
    response.throwIfFailed();
  }

  return queryResults;
}

std::string PrestoQueryRunner::startQuery(const std::string& sql) {
  auto uri = fmt::format("{}/v1/statement?binaryResults=true", coordinatorUri_);
  cpr::Url url{uri};
  cpr::Body body{sql};
  cpr::Header header({
      {"X-Presto-User", user_},
      {"X-Presto-Catalog", "hive"},
      {"X-Presto-Schema", "tpch"},
      {"Content-Type", "text/plain"},
  });
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

} // namespace facebook::velox::exec::test
