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

#include "velox/functions/sparksql/fuzzer/SparkQueryRunner.h"

#include "arrow/buffer.h"
#include "arrow/c/bridge.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "grpc/grpc.h" // @manual
#include "velox/common/base/Fs.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoSql.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/functions/sparksql/fuzzer/SparkQueryRunnerToSqlPlanNodeVisitor.h"
#include "velox/functions/sparksql/fuzzer/spark/connect/base.pb.h"
#include "velox/functions/sparksql/fuzzer/spark/connect/relations.pb.h"
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
  options->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;

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

const std::unordered_map<std::string, DataSpec>&
SparkQueryRunner::aggregationFunctionDataSpecs() const {
  static const std::unordered_map<std::string, DataSpec>
      kAggregationFunctionDataSpecs{};

  return kAggregationFunctionDataSpecs;
}

std::optional<std::string> SparkQueryRunner::toSql(
    const velox::core::PlanNodePtr& plan) {
  exec::test::PrestoSqlPlanNodeVisitorContext context;
  SparkQueryRunnerToSqlPlanNodeVisitor visitor;
  plan->accept(visitor, context);

  return context.sql;
}

std::pair<
    std::optional<std::multiset<std::vector<variant>>>,
    exec::test::ReferenceQueryErrorCode>
SparkQueryRunner::execute(const core::PlanNodePtr& plan) {
  std::pair<
      std::optional<std::vector<RowVectorPtr>>,
      exec::test::ReferenceQueryErrorCode>
      result = executeAndReturnVector(plan);
  if (result.first) {
    return std::make_pair(
        exec::test::materialize(*result.first), result.second);
  }
  return std::make_pair(std::nullopt, result.second);
}

std::pair<
    std::optional<std::vector<RowVectorPtr>>,
    exec::test::ReferenceQueryErrorCode>
SparkQueryRunner::executeAndReturnVector(const core::PlanNodePtr& plan) {
  if (std::optional<std::string> sql = toSql(plan)) {
    try {
      std::unordered_map<std::string, std::vector<RowVectorPtr>> inputMap =
          getAllTables(plan);
      for (const auto& [tableName, input] : inputMap) {
        auto inputType = asRowType(input[0]->type());
        if (inputType->size() == 0) {
          inputMap[tableName] = {exec::test::makeNullRows(
              input, fmt::format("{}x", tableName), pool())};
        }
      }

      auto writerPool = aggregatePool()->addAggregateChild("writer");
      std::vector<std::shared_ptr<exec::test::TempFilePath>> tempFiles;
      tempFiles.reserve(inputMap.size());
      for (const auto& [tableName, input] : inputMap) {
        auto tempFile = exec::test::TempFilePath::create();
        tempFiles.emplace_back(tempFile);
        const auto& filePath = tempFile->getPath();
        writeToFile(filePath, input, writerPool.get());
        // Create temporary view for this table in Spark by reading the
        // generated Parquet file.
        execute(fmt::format(
            "CREATE OR REPLACE TEMPORARY VIEW {} AS (SELECT * from parquet.`file://{}`);",
            tableName,
            filePath));
      }

      // Run the query.
      return std::make_pair(
          execute(*sql), exec::test::ReferenceQueryErrorCode::kSuccess);
    } catch (const VeloxRuntimeError& e) {
      throw;
    } catch (...) {
      LOG(WARNING) << "Query failed in Spark";
      return std::make_pair(
          std::nullopt,
          exec::test::ReferenceQueryErrorCode::kReferenceQueryFail);
    }
  }

  LOG(INFO) << "Query not supported in Spark";
  return std::make_pair(
      std::nullopt,
      exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported);
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
} // namespace facebook::velox::functions::sparksql::fuzzer
