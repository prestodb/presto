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

#include "presto_cpp/presto_protocol/WriteProtocol.h"

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/TableWriter.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

namespace facebook::presto::protocol {

namespace {

std::string buildPartitionUpdate(
    const std::shared_ptr<const WriterParameters> writerParameters,
    vector_size_t numWrittenRows) {
  const auto hiveWriterParameters =
      std::dynamic_pointer_cast<const HiveWriterParameters>(writerParameters);
  VELOX_CHECK(
      hiveWriterParameters != nullptr,
      "Presto commit protocol only supports Hive commits at the moment.")
  // clang-format off
 auto partitionUpdateJson = folly::toJson(
   folly::dynamic::object
     ("name", "")
     ("updateMode", hiveWriterParameters->encodeUpdateMode())
     ("writePath", hiveWriterParameters->writeDirectory())
     ("targetPath", hiveWriterParameters->targetDirectory())
     ("fileWriteInfos", folly::dynamic::array(
       folly::dynamic::object
         ("writeFileName", hiveWriterParameters->writeFileName())
         ("targetFileName", hiveWriterParameters->targetFileName())
         ("fileSize", 0)))
     ("rowCount", numWrittenRows)
     ("inMemoryDataSizeInBytes", 0)
     ("onDiskDataSizeInBytes", 0)
     ("containsNumberedFileNames", true));
  // clang-format on
  return partitionUpdateJson;
}

RowVectorPtr buildTableWriterCommitOutput(
    const TableWriterWriteInfo& writeInfo,
    const std::string& commitStrategy,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  std::vector<VectorPtr> columns = {};

  vector_size_t numOutputRows = 1;
  FlatVectorPtr<int64_t> rowWrittenVector;
  FlatVectorPtr<StringView> fragmentsVector;
  ConstantVectorPtr<StringView> commitContextVector;
  if (writeInfo.outputType()->size() <= 1) {
    rowWrittenVector = std::dynamic_pointer_cast<FlatVector<int64_t>>(
        BaseVector::create(BIGINT(), 1, pool));
    rowWrittenVector->set(0, writeInfo.numWrittenRows());
    columns.emplace_back(rowWrittenVector);
  } else {
    numOutputRows = writeInfo.writeParameters()->size() + 1;

    // Set rows column
    rowWrittenVector = std::dynamic_pointer_cast<FlatVector<int64_t>>(
        BaseVector::create(BIGINT(), numOutputRows, pool));
    rowWrittenVector->set(0, writeInfo.numWrittenRows());
    for (int idx = 1; idx < numOutputRows; ++idx) {
      rowWrittenVector->setNull(idx, true);
    }
    columns.emplace_back(rowWrittenVector);

    // Set fragments column
    fragmentsVector = std::dynamic_pointer_cast<velox::FlatVector<StringView>>(
        BaseVector::create(VARBINARY(), numOutputRows, pool));
    fragmentsVector->setNull(0, true);
    for (int i = 1; i < numOutputRows; i++) {
      fragmentsVector->set(
          i,
          StringView(buildPartitionUpdate(
              writeInfo.writeParameters()->at(i - 1),
              writeInfo.numWrittenRows())));
    }
    columns.emplace_back(fragmentsVector);

    // Set commitcontext column
    // clang-format off
   auto commitContextJson = folly::toJson(
     folly::dynamic::object
         ("lifespan", "TaskWide")
         ("taskId", writeInfo.taskId())
         ("pageSinkCommitStrategy", commitStrategy)
         ("lastPage", true));
    // clang-format on
    commitContextVector = std::make_shared<ConstantVector<StringView>>(
        pool, numOutputRows, false, VARBINARY(), StringView(commitContextJson));
    columns.emplace_back(commitContextVector);
  }

  return std::make_shared<RowVector>(
      pool, writeInfo.outputType(), BufferPtr(nullptr), numOutputRows, columns);
}

} // namespace

RowVectorPtr PrestoNoCommitWriteProtocol::commit(
    const WriteInfo& writeInfo,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  if (const auto* tableWriterWriteInfo =
          dynamic_cast<const TableWriterWriteInfo*>(&writeInfo)) {
    return commit(*tableWriterWriteInfo, pool);
  }
  VELOX_UNSUPPORTED(
      "PrestoNoCommitWriteProtocol currently only supports TableWriter commit");
}

RowVectorPtr PrestoNoCommitWriteProtocol::commit(
    const TableWriterWriteInfo& writeInfo,
    memory::MemoryPool* FOLLY_NONNULL pool) {
  return buildTableWriterCommitOutput(writeInfo, encodeCommitStrategy(), pool);
}

RowVectorPtr PrestoTaskCommitWriteProtocol::commit(
    const WriteInfo& writeInfo,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  if (const auto* tableWriterWriteInfo =
          dynamic_cast<const TableWriterWriteInfo*>(&writeInfo)) {
    return commit(*tableWriterWriteInfo, pool);
  }
  VELOX_UNSUPPORTED(
      "PrestoNoCommitWriteProtocol currently only supports TableWriter commit");
}

RowVectorPtr PrestoTaskCommitWriteProtocol::commit(
    const TableWriterWriteInfo& writeInfo,
    memory::MemoryPool* FOLLY_NONNULL pool) {
  return buildTableWriterCommitOutput(writeInfo, encodeCommitStrategy(), pool);
}

} // namespace facebook::presto::protocol