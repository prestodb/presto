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
#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/c/bridge.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "grpc/grpc.h"
#include "spark/connect/base.pb.h"
#include "spark/connect/relations.pb.h"
#include "velox/common/base/Fs.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/ToSQLUtil.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/functions/sparksql/fuzzer/SparkQueryRunner.h"
#include "velox/vector/arrow/Bridge.h"

using namespace spark::connect;

namespace facebook::velox::functions::sparksql::fuzzer {
namespace {

void writeToFile(
    const std::string& path,
    const std::vector<RowVectorPtr>& data,
    memory::MemoryPool* pool) {
  VELOX_CHECK_GT(data.size(), 0);

  auto options = std::make_shared<parquet::WriterOptions>();
  options->schema = data[0]->type();
  options->memoryPool = pool;
  // Spark does not recognize int64-timestamp written as nano precision in
  // Parquet.
  options->parquetWriteTimestampUnit = TimestampUnit::kMicro;

  auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  auto writer =
      dwio::common::getWriterFactory(dwio::common::FileFormat::PARQUET)
          ->createWriter(std::move(sink), options);

  for (const auto& vector : data) {
    // When vector is dictionary-encoded, complex types are not supported in
    // exportFlattenedVector. Flatten the vector before writing to Parquet.
    // https://github.com/facebookincubator/velox/issues/10397
    VectorPtr flattened = vector;
    BaseVector::flattenVector(flattened);
    writer->write(flattened);
  }
  writer->close();
}

} // namespace

const std::vector<TypePtr>& SparkQueryRunner::supportedScalarTypes() const {
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
      DATE(),
  };
  return kScalarTypes;
}

std::optional<std::string> SparkQueryRunner::toSql(
    const velox::core::PlanNodePtr& plan) {
  if (const auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(plan)) {
    return toSql(aggregationNode);
  }
  if (const auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(plan)) {
    return toSql(projectNode);
  }
  VELOX_NYI("Unsupported plan node: {}.", plan->toString());
}

std::multiset<std::vector<variant>> SparkQueryRunner::execute(
    const std::string& sql,
    const std::vector<RowVectorPtr>& input,
    const RowTypePtr& resultType) {
  return exec::test::materialize(executeVector(sql, input, resultType));
}

std::vector<RowVectorPtr> SparkQueryRunner::executeVector(
    const std::string& sql,
    const std::vector<RowVectorPtr>& input,
    const RowTypePtr& resultType) {
  auto inputType = asRowType(input[0]->type());
  if (inputType->size() == 0) {
    auto rowVector = exec::test::makeNullRows(input, "x", pool());
    return executeVector(sql, {rowVector}, resultType);
  }

  // Write the input to a Parquet file.
  auto tempFile = exec::test::TempFilePath::create();
  const auto& filePath = tempFile->getPath();
  auto writerPool = aggregatePool()->addAggregateChild("writer");
  writeToFile(filePath, input, writerPool.get());

  // Create temporary view 'tmp' in Spark by reading the generated Parquet file.
  execute(fmt::format(
      "CREATE OR REPLACE TEMPORARY VIEW tmp AS (SELECT * from parquet.`file://{}`);",
      filePath));
  return execute(sql);
}

std::vector<RowVectorPtr> SparkQueryRunner::execute(
    const std::string& content) {
  auto sql = google::protobuf::Arena::CreateMessage<SQL>(&arena_);
  sql->set_query(content);

  auto relation = google::protobuf::Arena::CreateMessage<Relation>(&arena_);
  relation->set_allocated_sql(sql);

  auto plan = google::protobuf::Arena::CreateMessage<Plan>(&arena_);
  plan->set_allocated_root(relation);

  auto context = google::protobuf::Arena::CreateMessage<UserContext>(&arena_);
  context->set_user_id(userId_);
  context->set_user_name(userName_);

  auto request =
      google::protobuf::Arena::CreateMessage<ExecutePlanRequest>(&arena_);
  request->set_session_id(sessionId_);
  request->set_allocated_user_context(context);
  request->set_allocated_plan(plan);

  grpc::ClientContext clientContext;
  auto reader = stub_->ExecutePlan(&clientContext, *request);
  ExecutePlanResponse response;
  std::vector<RowVectorPtr> results;
  while (reader->Read(&response)) {
    VELOX_CHECK_EQ(
        response.session_id(),
        sessionId_,
        "The session id of the response does not match that of the request.");
    if (response.has_arrow_batch()) {
      const std::string& data = response.arrow_batch().data();
      const auto batchResults = readArrowData(data);
      results.insert(results.end(), batchResults.begin(), batchResults.end());
    }
  }
  return results;
}

std::string SparkQueryRunner::generateUUID() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 15);
  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 32; i++) {
    ss << "0123456789abcdef"[dis(gen)];
    if (i == 7 || i == 11 || i == 15 || i == 19)
      ss << "-";
  }
  return ss.str();
}

std::vector<RowVectorPtr> SparkQueryRunner::readArrowData(
    const std::string& data) {
  auto buffer = std::make_shared<arrow::Buffer>(data);
  auto bufferReader = std::make_shared<arrow::io::BufferReader>(buffer);
  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> result =
      arrow::ipc::RecordBatchStreamReader::Open(bufferReader);
  VELOX_CHECK(
      result.ok(),
      "Failed to open RecordBatchReader: {}.",
      result.status().ToString());

  std::shared_ptr<arrow::ipc::RecordBatchReader> reader = result.ValueUnsafe();
  std::shared_ptr<arrow::Schema> schema = reader->schema();

  ArrowSchema arrowSchema;
  auto status = arrow::ExportSchema(*schema, &arrowSchema);
  VELOX_CHECK(status.ok(), "Failed to export schema: {}.", status.ToString());

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> batchResult;
  std::vector<RowVectorPtr> results;
  while ((batchResult = reader->Next()).ok() && batchResult.ValueUnsafe()) {
    std::shared_ptr<arrow::RecordBatch> batch = batchResult.ValueUnsafe();
    ArrowArray arrowArray;
    status = ExportRecordBatch(*batch, &arrowArray);
    VELOX_CHECK(status.ok(), "Failed to export array: {}.", status.ToString());
    auto rv = std::dynamic_pointer_cast<RowVector>(
        importFromArrowAsOwner(arrowSchema, arrowArray, pool()));
    auto copy =
        BaseVector::create<RowVector>(rv->type(), rv->size(), copyPool_.get());
    copy->copy(rv.get(), 0, 0, rv->size());
    results.push_back(copy);
  }
  VELOX_CHECK(
      batchResult.ok(),
      "Failed to read batch: {}.",
      batchResult.status().ToString());
  return results;
}

std::optional<std::string> SparkQueryRunner::toSql(
    const std::shared_ptr<const core::AggregationNode>& aggregationNode) {
  // Assume plan is Aggregation over Values.
  VELOX_CHECK(aggregationNode->step() == core::AggregationNode::Step::kSingle);

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
      exec::test::appendComma(i, sql);
      const auto& aggregate = aggregates[i];
      VELOX_CHECK(
          aggregate.sortingKeys.empty(),
          "Sort key is not supported in Spark's aggregation. You may need to disable 'enable_sorted_aggregations' when running the fuzzer test.");
      sql << exec::test::toAggregateCallSql(
          aggregate.call, {}, {}, aggregate.distinct);

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

std::optional<std::string> SparkQueryRunner::toSql(
    const std::shared_ptr<const core::ProjectNode>& projectNode) {
  auto sourceSql = toSql(projectNode->sources()[0]);
  if (!sourceSql.has_value()) {
    return std::nullopt;
  }

  std::stringstream sql;
  sql << "SELECT ";

  for (auto i = 0; i < projectNode->names().size(); ++i) {
    exec::test::appendComma(i, sql);
    auto projection = projectNode->projections()[i];
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                projection)) {
      sql << field->name();
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(projection)) {
      sql << exec::test::toCallSql(call);
    } else {
      VELOX_NYI(
          "Unsupported projection {} in project node: {}.",
          projection->toString(),
          projectNode->toString());
    }

    sql << " as " << projectNode->names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  return sql.str();
}
} // namespace facebook::velox::functions::sparksql::fuzzer
