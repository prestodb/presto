/*
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
#include "velox/connectors/hive/HiveWriteProtocol.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

namespace facebook::presto::protocol {

namespace {

std::string makePartitionUpdate(
    const std::shared_ptr<const WriterParameters> writerParameters,
    vector_size_t numWrittenRows) {
  const auto hiveWriterParameters =
      std::dynamic_pointer_cast<const HiveWriterParameters>(writerParameters);
  VELOX_CHECK_NOT_NULL(
      hiveWriterParameters,
      "Presto commit protocol only supports Hive commits at the moment.")
  // clang-format off
  auto partitionUpdateJson = folly::toJson(
   folly::dynamic::object
     ("name", "")(
      "updateMode",
      HiveWriterParameters::updateModeToString(
          hiveWriterParameters->updateMode()))("writePath", hiveWriterParameters->writeDirectory())
     ("targetPath", hiveWriterParameters->targetDirectory())
     ("fileWriteInfos", folly::dynamic::array(
       folly::dynamic::object
         ("writeFileName", hiveWriterParameters->writeFileName())
         ("targetFileName", hiveWriterParameters->targetFileName())
         ("fileSize", 0)))
     ("rowCount", numWrittenRows)
     // TODO(gaoge): track and send the fields when inMemoryDataSizeInBytes, onDiskDataSizeInBytes
     // and containsNumberedFileNames are needed at coordinator when file_renaming_enabled are turned on,
     // https://fburl.com/18vim7k4.
     ("inMemoryDataSizeInBytes", 0)
     ("onDiskDataSizeInBytes", 0)
     ("containsNumberedFileNames", true));
  // clang-format on
  return partitionUpdateJson;
}

RowVectorPtr makeCommitOutput(
    const CommitInfo& commitInfo,
    const std::string& commitStrategy,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  std::vector<VectorPtr> columns = {};

  vector_size_t numOutputRows = 1;
  if (commitInfo.outputType()->size() <= 1) {
    columns.emplace_back(BaseVector::createConstant(
        (int64_t)commitInfo.numWrittenRows(), 1, pool));
  } else {
    auto hiveCommitInfo =
        std::dynamic_pointer_cast<const HiveConnectorCommitInfo>(
            commitInfo.connectorCommitInfo());
    VELOX_CHECK_NOT_NULL(
        hiveCommitInfo,
        "Presto write protocol expects commit info from Hive connector.");
    numOutputRows = hiveCommitInfo->writerParameters().size() + 1;

    // Set rows column.
    FlatVectorPtr<int64_t> rowWrittenVector =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), numOutputRows, pool);
    rowWrittenVector->set(0, commitInfo.numWrittenRows());
    for (int idx = 1; idx < numOutputRows; ++idx) {
      rowWrittenVector->setNull(idx, true);
    }
    columns.emplace_back(rowWrittenVector);

    // Set fragments column.
    FlatVectorPtr<StringView> fragmentsVector =
        BaseVector::create<FlatVector<StringView>>(
            VARBINARY(), numOutputRows, pool);
    fragmentsVector->setNull(0, true);
    for (int i = 1; i < numOutputRows; i++) {
      fragmentsVector->set(
          i,
          StringView(makePartitionUpdate(
              hiveCommitInfo->writerParameters().at(i - 1),
              commitInfo.numWrittenRows())));
    }
    columns.emplace_back(fragmentsVector);

    // Set commitcontext column.
    // clang-format off
   auto commitContextJson = folly::toJson(
     folly::dynamic::object
         ("lifespan", "TaskWide")
         ("taskId", commitInfo.taskId())
         ("pageSinkCommitStrategy", commitStrategy)
         ("lastPage", true));
    // clang-format on
    VectorPtr commitContextVector = BaseVector::createConstant(
        variant::binary(commitContextJson), numOutputRows, pool);
    columns.emplace_back(commitContextVector);
  }

  return std::make_shared<RowVector>(
      pool, commitInfo.outputType(), nullptr, numOutputRows, columns);
}

} // namespace

RowVectorPtr HiveNoCommitWriteProtocol::commit(
    const CommitInfo& commitInfo,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  return makeCommitOutput(
      commitInfo, commitStrategyToString(commitStrategy()), pool);
}

RowVectorPtr HiveTaskCommitWriteProtocol::commit(
    const CommitInfo& commitInfo,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  return makeCommitOutput(
      commitInfo, commitStrategyToString(commitStrategy()), pool);
}

} // namespace facebook::presto::protocol