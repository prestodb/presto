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

#include "presto_cpp/presto_protocol//WriteProtocol.h"

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto::protocol {

namespace {

std::string buildPartitionUpdate(
    const std::shared_ptr<const WriteParameters> writeParameters,
    TableWriter& tableWriter) {
  const auto hiveWriteParameters =
      std::dynamic_pointer_cast<const HiveWriteParameters>(writeParameters);
  // clang-format off
  auto partitionUpdateJson = folly::toJson(
    folly::dynamic::object
      ("name", "")
      ("updateMode", hiveWriteParameters->encodeUpdateMode())
      ("writePath", hiveWriteParameters->writeDirectory())
      ("targetPath", hiveWriteParameters->targetDirectory())
      ("fileWriteInfos", folly::dynamic::array(
        folly::dynamic::object
          ("writeFileName", hiveWriteParameters->writeFileName())
          ("targetFileName", hiveWriteParameters->targetFileName())
          ("fileSize", 0)))
      ("rowCount", tableWriter.numWrittenRows())
      ("inMemoryDataSizeInBytes", 0)
      ("onDiskDataSizeInBytes", 0)
      ("containsNumberedFileNames", true));
  // clang-format on
  return partitionUpdateJson;
}

RowVectorPtr buildTableWriterCommitOutput(
    TableWriter& tableWriter,
    const std::string& commitStrategy) {
  std::vector<VectorPtr> columns = {};

  vector_size_t numOutputRows = 1;
  FlatVectorPtr<int64_t> rowWrittenVector;
  FlatVectorPtr<StringView> fragmentsVector;
  ConstantVectorPtr<StringView> commitContextVector;
  if (tableWriter.outputType()->size() <= 1) {
    rowWrittenVector = std::dynamic_pointer_cast<FlatVector<int64_t>>(
        BaseVector::create(BIGINT(), 1, tableWriter.pool()));
    rowWrittenVector->set(0, tableWriter.numWrittenRows());
    columns.emplace_back(rowWrittenVector);
  } else {
    std::shared_ptr<connector::hive::HiveDataSink> hiveDataSink = nullptr;
    const auto dataSink = tableWriter.dataSink();
    if (!dataSink) {
      numOutputRows = 1;
    } else {
      hiveDataSink =
          std::dynamic_pointer_cast<connector::hive::HiveDataSink>(dataSink);
      VELOX_CHECK(
          hiveDataSink != nullptr,
          "PrestoNoCommitWriteProtocol currently only supports TableWriter commit");
      numOutputRows = hiveDataSink->getWriteParameters().size() + 1;
    }

    rowWrittenVector = std::dynamic_pointer_cast<FlatVector<int64_t>>(
        BaseVector::create(BIGINT(), numOutputRows, tableWriter.pool()));
    rowWrittenVector->set(0, tableWriter.numWrittenRows());
    for (int idx = 1; idx < numOutputRows; ++idx) {
      rowWrittenVector->setNull(idx, true);
    }
    columns.emplace_back(rowWrittenVector);

    fragmentsVector = std::dynamic_pointer_cast<velox::FlatVector<StringView>>(
        BaseVector::create(VARBINARY(), numOutputRows, tableWriter.pool()));
    fragmentsVector->setNull(0, true);
    for (int i = 1; i < numOutputRows; i++) {
      fragmentsVector->set(
          i,
          StringView(buildPartitionUpdate(
              hiveDataSink->getWriteParameters().at(i - 1), tableWriter)));
    }

    columns.emplace_back(fragmentsVector);

    // clang-format off
  auto commitContextJson = folly::toJson(
      folly::dynamic::object
          ("lifespan", "TaskWide")
          ("taskId", tableWriter.driverCtx()->task->taskId())
          ("pageSinkCommitStrategy", commitStrategy)
          ("lastPage", true));
    // clang-format on
    commitContextVector = std::make_shared<ConstantVector<StringView>>(
        tableWriter.pool(),
        numOutputRows,
        false,
        VARBINARY(),
        StringView(commitContextJson));
    columns.emplace_back(commitContextVector);
  }

  return std::make_shared<RowVector>(
      tableWriter.pool(),
      tableWriter.outputType(),
      BufferPtr(nullptr),
      numOutputRows,
      columns);
}

} // namespace

RowVectorPtr PrestoNoCommitWriteProtocol::commit(Operator& commitOperator) {
  if (auto* tableWriterOperator = dynamic_cast<TableWriter*>(&commitOperator)) {
    return commit(*tableWriterOperator);
  }
  VELOX_UNSUPPORTED(
      "PrestoNoCommitWriteProtocol currently only supports TableWriter commit");
}

RowVectorPtr PrestoNoCommitWriteProtocol::commit(TableWriter& tableWriter) {
  return buildTableWriterCommitOutput(tableWriter, encodeCommitStrategy());
}

RowVectorPtr PrestoTaskCommitWriteProtocol::commit(Operator& commitOperator) {
  if (auto* tableWriterOperator = dynamic_cast<TableWriter*>(&commitOperator)) {
    return commit(*tableWriterOperator);
  }
  VELOX_UNSUPPORTED(
      "PrestoNoCommitWriteProtocol currently only supports TableWriter commit");
}

RowVectorPtr PrestoTaskCommitWriteProtocol::commit(TableWriter& tableWriter) {
  return buildTableWriterCommitOutput(tableWriter, encodeCommitStrategy());
}

} // namespace facebook::presto::protocol