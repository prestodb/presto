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

#include <cpr/cpr.h> // @manual
#include <folly/json.h>
#include <iostream>
#include <utility>

#include "velox/common/base/Fs.h"
#include "velox/common/encode/Base64.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/fuzzer/PrestoQueryRunnerIntermediateTypeTransforms.h"
#include "velox/exec/fuzzer/PrestoQueryRunnerToSqlPlanNodeVisitor.h"
#include "velox/exec/fuzzer/PrestoSql.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/types/GeometryType.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/QDigestType.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/UuidType.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/parser/TypeParser.h"

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
       static_cast<int32_t>(input.length()),
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
  PrestoSqlPlanNodeVisitorContext context;
  PrestoQueryRunnerToSqlPlanNodeVisitor visitor(queryRunnerContext_);
  plan->accept(visitor, context);

  return context.sql;
}

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
      TIMESTAMP_WITH_TIME_ZONE(),
  };
  return kScalarTypes;
}

// static
bool PrestoQueryRunner::isSupportedDwrfType(const TypePtr& type) {
  if (type->isDate() || type->isIntervalDayTime() || type->isUnKnown() ||
      isGeometryType(type)) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isSupportedDwrfType(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

std::pair<std::vector<RowVectorPtr>, std::vector<core::ExprPtr>>
PrestoQueryRunner::inputProjections(
    const std::vector<RowVectorPtr>& input) const {
  if (input.empty()) {
    return {input, {}};
  }

  std::vector<core::ExprPtr> projections;
  std::vector<std::string> names = input[0]->type()->asRow().names();
  std::vector<std::vector<VectorPtr>> children(input.size());
  for (int childIndex = 0; childIndex < input[0]->childrenSize();
       childIndex++) {
    const auto& childType = input[0]->childAt(childIndex)->type();
    // If it's an intermediate only type, transform the input and add
    // expressions to reverse the transformation.  Otherwise the input is
    // unchanged and the projection is just an identity mapping.
    if (isIntermediateOnlyType(childType)) {
      for (int batchIndex = 0; batchIndex < input.size(); batchIndex++) {
        children[batchIndex].push_back(
            transformIntermediateTypes(input[batchIndex]->childAt(childIndex)));
      }
      projections.push_back(getProjectionsToIntermediateTypes(
          childType,
          std::make_shared<core::FieldAccessExpr>(
              names[childIndex], names[childIndex]),
          names[childIndex]));
    } else {
      for (int batchIndex = 0; batchIndex < input.size(); batchIndex++) {
        children[batchIndex].push_back(input[batchIndex]->childAt(childIndex));
      }

      projections.push_back(std::make_shared<core::FieldAccessExpr>(
          names[childIndex], names[childIndex]));
    }
  }

  std::vector<TypePtr> types;
  for (const auto& child : children[0]) {
    types.push_back(child->type());
  }

  auto rowType = ROW(std::move(names), std::move(types));

  std::vector<RowVectorPtr> output;
  output.reserve(input.size());
  for (int batchIndex = 0; batchIndex < input.size(); batchIndex++) {
    output.push_back(std::make_shared<RowVector>(
        input[batchIndex]->pool(),
        rowType,
        input[batchIndex]->nulls(),
        input[batchIndex]->size(),
        std::move(children[batchIndex])));
  }

  return std::make_pair(output, projections);
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

bool PrestoQueryRunner::isConstantExprSupported(
    const core::TypedExprPtr& expr) {
  if (std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    // TODO: support constant literals of these types. Complex-typed constant
    // literals require support of converting them to SQL. Json, Ipaddress,
    // Ipprefix, and Uuid can be enabled after we're able to generate valid
    // input values, because when these types are used as the type of a constant
    // literal in SQL, Presto implicitly invokes json_parse(),
    // cast(x as Ipaddress), cast(x as Ipprefix) and cast(x as uuid) on it,
    // which makes the behavior of Presto different from Velox. Timestamp
    // constant literals require further investigation to ensure Presto uses the
    // same timezone as Velox. Interval type cannot be used as the type of
    // constant literals in Presto SQL.
    auto& type = expr->type();
    return type->isPrimitiveType() && !type->isTimestamp() &&
        !isJsonType(type) && !type->isIntervalDayTime() &&
        !isIPAddressType(type) && !isIPPrefixType(type) && !isUuidType(type) &&
        !isTimestampWithTimeZoneType(type) && !isHyperLogLogType(type) &&
        !isTDigestType(type) && !isQDigestType(type);
    ;
  }
  return true;
}

bool PrestoQueryRunner::isSupported(const exec::FunctionSignature& signature) {
  // TODO: support queries with these types. Among the types below, hugeint is
  // not a native type in Presto, so fuzzer should not use it as the type of
  // cast-to or constant literals. Hyperloglog and TDigest can only be casted
  // from varbinary and cannot be used as the type of constant literals.
  // Interval year to month can only be casted from NULL and cannot be used as
  // the type of constant literals. Json, Ipaddress, Ipprefix, and UUID require
  // special handling, because Presto requires literals of these types to be
  // valid, and doesn't allow creating HIVE columns of these types.
  return !(
      usesTypeName(signature, "bingtile") ||
      usesTypeName(signature, "interval year to month") ||
      usesTypeName(signature, "hugeint") ||
      usesInputTypeName(signature, "json") ||
      usesInputTypeName(signature, "ipaddress") ||
      usesInputTypeName(signature, "ipprefix") ||
      usesInputTypeName(signature, "uuid"));
}

std::pair<
    std::optional<std::multiset<std::vector<velox::variant>>>,
    ReferenceQueryErrorCode>
PrestoQueryRunner::execute(const core::PlanNodePtr& plan) {
  std::pair<
      std::optional<std::vector<velox::RowVectorPtr>>,
      ReferenceQueryErrorCode>
      result = executeAndReturnVector(plan);
  if (result.first) {
    return std::make_pair(
        exec::test::materialize(*result.first), result.second);
  }
  return std::make_pair(std::nullopt, result.second);
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

std::pair<
    std::optional<std::vector<velox::RowVectorPtr>>,
    ReferenceQueryErrorCode>
PrestoQueryRunner::executeAndReturnVector(const core::PlanNodePtr& plan) {
  if (std::optional<std::string> sql = toSql(plan)) {
    try {
      std::unordered_map<std::string, std::vector<velox::RowVectorPtr>>
          inputMap = getAllTables(plan);
      for (const auto& [tableName, input] : inputMap) {
        auto inputType = asRowType(input[0]->type());
        if (inputType->size() == 0) {
          inputMap[tableName] = {
              makeNullRows(input, fmt::format("{}x", tableName), pool())};
        }
      }

      auto writerPool = aggregatePool()->addAggregateChild("writer");
      for (const auto& [tableName, input] : inputMap) {
        auto tableDirectoryPath = createTable(tableName, input[0]->type());

        // Create a new file in table's directory with fuzzer-generated data.
        auto filePath = fs::path(tableDirectoryPath)
                            .append(fmt::format("{}.dwrf", tableName))
                            .string()
                            .substr(strlen("file:"));

        writeToFile(filePath, input, writerPool.get());
      }

      // Run the query.
      return std::make_pair(execute(*sql), ReferenceQueryErrorCode::kSuccess);
    } catch (const VeloxRuntimeError& e) {
      // Throw if connection to Presto server is unsuccessful.
      if (e.message().find("Couldn't connect to server") != std::string::npos) {
        throw;
      }
      LOG(WARNING) << "Query failed in Presto";
      return std::make_pair(
          std::nullopt, ReferenceQueryErrorCode::kReferenceQueryFail);
    } catch (...) {
      LOG(WARNING) << "Query failed in Presto";
      return std::make_pair(
          std::nullopt, ReferenceQueryErrorCode::kReferenceQueryFail);
    }
  }

  LOG(INFO) << "Query not supported in Presto";
  return std::make_pair(
      std::nullopt, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
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
