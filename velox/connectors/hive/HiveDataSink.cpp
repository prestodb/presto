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

#include "velox/connectors/hive/HiveDataSink.h"

#include "velox/common/base/Fs.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HivePartitionUtil.h"
#include "velox/dwio/dwrf/writer/Writer.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace facebook::velox::dwrf;
using WriterConfig = facebook::velox::dwrf::Config;

namespace facebook::velox::connector::hive {

namespace {

// Returns a subset of column indices corresponding to partition keys.
std::vector<column_index_t> getPartitionChannels(
    const std::shared_ptr<const HiveInsertTableHandle>& insertTableHandle) {
  std::vector<column_index_t> channels;

  for (column_index_t i = 0; i < insertTableHandle->inputColumns().size();
       i++) {
    if (insertTableHandle->inputColumns()[i]->isPartitionKey()) {
      channels.push_back(i);
    }
  }

  return channels;
}

std::string makePartitionDirectory(
    const std::string& tableDirectory,
    const std::optional<std::string>& partitionSubdirectory) {
  if (partitionSubdirectory.has_value()) {
    return (fs::path(tableDirectory) / partitionSubdirectory.value()).string();
  }
  return tableDirectory;
}

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

} // namespace

HiveDataSink::HiveDataSink(
    RowTypePtr inputType,
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      connectorQueryCtx_(connectorQueryCtx),
      commitStrategy_(commitStrategy),
      partitionChannels_(getPartitionChannels(insertTableHandle_)),
      partitionIdGenerator_(
          !partitionChannels_.empty() ? std::make_unique<PartitionIdGenerator>(
                                            inputType_,
                                            partitionChannels_,
                                            HiveConfig::maxPartitionsPerWriters(
                                                connectorQueryCtx_->config()),
                                            connectorQueryCtx_->memoryPool())
                                      : nullptr) {}

void HiveDataSink::appendData(RowVectorPtr input) {
  // Write to unpartitioned table.
  if (partitionChannels_.empty()) {
    ensureSingleWriter();

    writers_[0]->write(input);
    writerInfo_[0]->numWrittenRows += input->size();
    return;
  }

  // Write to partitioned table.
  partitionIdGenerator_->run(input, partitionIds_);

  ensurePartitionWriters();

  for (column_index_t i = 0; i < input->childrenSize(); i++) {
    input->childAt(i)->loadedVector();
  }

  const auto numPartitions = partitionIdGenerator_->numPartitions();

  // All inputs belong to a single partition.
  if (numPartitions == 1) {
    writers_[0]->write(input);
    writerInfo_[0]->numWrittenRows += input->size();
    return;
  }

  computePartitionRowCountsAndIndices();

  for (auto id = 0; id < numPartitions; id++) {
    vector_size_t partitionSize = partitionSizes_[id];
    if (partitionSize == 0) {
      continue;
    }

    RowVectorPtr writerInput = partitionSize == input->size()
        ? input
        : exec::wrap(partitionSize, partitionRows_[id], input);
    writers_[id]->write(writerInput);
    writerInfo_[id]->numWrittenRows += partitionSize;
  }
}

std::vector<std::string> HiveDataSink::finish() const {
  std::vector<std::string> partitionUpdates;
  partitionUpdates.reserve(writerInfo_.size());

  for (const auto& info : writerInfo_) {
    if (info) {
      // clang-format off
      auto partitionUpdateJson = folly::toJson(
       folly::dynamic::object
          ("name", info->writerParameters.partitionName().value_or(""))
          ("updateMode",
            HiveWriterParameters::updateModeToString(
              info->writerParameters.updateMode()))
          ("writePath", info->writerParameters.writeDirectory())
          ("targetPath", info->writerParameters.targetDirectory())
          ("fileWriteInfos", folly::dynamic::array(
            folly::dynamic::object
              ("writeFileName", info->writerParameters.writeFileName())
              ("targetFileName", info->writerParameters.targetFileName())
              ("fileSize", 0)))
          ("rowCount", info->numWrittenRows)
         // TODO(gaoge): track and send the fields when inMemoryDataSizeInBytes, onDiskDataSizeInBytes
         // and containsNumberedFileNames are needed at coordinator when file_renaming_enabled are turned on.
          ("inMemoryDataSizeInBytes", 0)
          ("onDiskDataSizeInBytes", 0)
          ("containsNumberedFileNames", true));
      // clang-format on
      partitionUpdates.push_back(partitionUpdateJson);
    }
  }
  return partitionUpdates;
}

void HiveDataSink::close() {
  for (const auto& writer : writers_) {
    writer->close();
  }
}

void HiveDataSink::ensureSingleWriter() {
  if (writers_.empty()) {
    appendWriter(std::nullopt);
  }
}

void HiveDataSink::ensurePartitionWriters() {
  const auto numPartitions = partitionIdGenerator_->numPartitions();
  const auto numWriters = writers_.size();

  VELOX_CHECK_LE(numWriters, numPartitions);

  if (numWriters < numPartitions) {
    writers_.reserve(numPartitions);
    writerInfo_.reserve(numPartitions);
    for (auto id = numWriters; id < numPartitions; id++) {
      appendWriter(partitionIdGenerator_->partitionName(id));
    }
  }
}

void HiveDataSink::appendWriter(
    const std::optional<std::string>& partitionName) {
  auto config = std::make_shared<WriterConfig>();
  // TODO: Wire up serde properties to writer configs.

  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = inputType_;
  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.
  auto writerParameters = getWriterParameters(partitionName);
  auto writePath = fs::path(writerParameters->writeDirectory()) /
      writerParameters->writeFileName();

  auto sink = dwio::common::DataSink::create(writePath);
  writers_.push_back(std::make_unique<Writer>(
      options, std::move(sink), *connectorQueryCtx_->aggregatePool()));
  writerInfo_.push_back(std::make_shared<HiveWriterInfo>(*writerParameters));
}

void HiveDataSink::computePartitionRowCountsAndIndices() {
  const auto numPartitions = partitionIdGenerator_->numPartitions();
  const auto numRows = partitionIds_.size();

  partitionSizes_.resize(numPartitions);
  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);

  partitionRows_.resize(numPartitions, nullptr);
  rawPartitionRows_.resize(numPartitions);
  for (auto id = 0; id < numPartitions; id++) {
    if (partitionRows_[id] == nullptr ||
        partitionRows_[id]->capacity() < numRows * sizeof(vector_size_t)) {
      partitionRows_[id] =
          allocateIndices(numRows, connectorQueryCtx_->memoryPool());
      rawPartitionRows_[id] = partitionRows_[id]->asMutable<vector_size_t>();
    }
  }

  for (auto row = 0; row < numRows; row++) {
    uint64_t id = partitionIds_[row];
    rawPartitionRows_[id][partitionSizes_[id]] = row;
    partitionSizes_[id]++;
  }

  for (auto id = 0; id < numPartitions; id++) {
    partitionRows_[id]->setSize(partitionSizes_[id] * sizeof(vector_size_t));
  }
}

std::shared_ptr<const HiveWriterParameters> HiveDataSink::getWriterParameters(
    const std::optional<std::string>& partition) const {
  auto updateMode = getUpdateMode();

  std::string targetFileName;
  std::string writeFileName;
  switch (commitStrategy_) {
    case CommitStrategy::kNoCommit: {
      targetFileName = fmt::format(
          "{}_{}_{}",
          connectorQueryCtx_->taskId(),
          connectorQueryCtx_->driverId(),
          makeUuid());
      writeFileName = targetFileName;
      break;
    }
    case CommitStrategy::kTaskCommit: {
      targetFileName = fmt::format(
          "{}_{}_{}",
          connectorQueryCtx_->taskId(),
          connectorQueryCtx_->driverId(),
          0);
      writeFileName =
          fmt::format(".tmp.velox.{}_{}", targetFileName, makeUuid());
      break;
    }
    default:
      VELOX_UNREACHABLE();
  }

  return std::make_shared<HiveWriterParameters>(
      updateMode,
      partition,
      targetFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->targetPath(), partition),
      writeFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->writePath(), partition));
}

HiveWriterParameters::UpdateMode HiveDataSink::getUpdateMode() const {
  if (insertTableHandle_->isInsertTable()) {
    if (insertTableHandle_->isPartitioned()) {
      auto insertBehavior = HiveConfig::insertExistingPartitionsBehavior(
          connectorQueryCtx_->config());
      switch (insertBehavior) {
        case HiveConfig::InsertExistingPartitionsBehavior::kOverwrite:
          return HiveWriterParameters::UpdateMode::kOverwrite;

        case HiveConfig::InsertExistingPartitionsBehavior::kError:
          return HiveWriterParameters::UpdateMode::kNew;
        default:
          VELOX_UNSUPPORTED("Unsupported insert existing partitions behavior.");
      }
    } else {
      VELOX_USER_FAIL("Unpartitioned Hive tables are immutable.");
    }
  } else {
    return HiveWriterParameters::UpdateMode::kNew;
  }
}

bool HiveInsertTableHandle::isPartitioned() const {
  return std::any_of(
      inputColumns_.begin(), inputColumns_.end(), [](auto column) {
        return column->isPartitionKey();
      });
}

bool HiveInsertTableHandle::isInsertTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kExisting;
}

} // namespace facebook::velox::connector::hive
