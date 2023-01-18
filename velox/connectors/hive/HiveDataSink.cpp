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
#include "velox/dwio/dwrf/writer/Writer.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace facebook::velox::dwrf;
using WriterConfig = facebook::velox::dwrf::Config;

namespace facebook::velox::connector::hive {

namespace {

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

std::string makePartitionDirectory(
    const std::string& tableDirectory,
    const std::optional<std::string>& partitionSubdirectory) {
  if (partitionSubdirectory.has_value()) {
    return (fs::path(tableDirectory) / partitionSubdirectory.value()).string();
  }
  return tableDirectory;
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
      commitStrategy_(commitStrategy) {}

void HiveDataSink::appendData(VectorPtr input) {
  // For the time being the hive data sink supports one file
  // To extend it we can create a new writer for every
  // partition
  if (writers_.empty()) {
    createWriter(std::nullopt);
  }
  writers_[0]->write(input);
  writerInfo_[0]->numWrittenRows += input->size();
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

void HiveDataSink::createWriter(
    const std::optional<std::string>& partitionName) {
  auto config = std::make_shared<WriterConfig>();
  // TODO: Wire up serde properties to writer configs.

  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = inputType_;
  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.
  auto writerParameters = getWriterParameters(partitionName);

  writerInfo_.emplace_back(std::make_shared<HiveWriterInfo>(*writerParameters));

  auto writePath = fs::path(writerParameters->writeDirectory()) /
      writerParameters->writeFileName();
  auto sink = dwio::common::DataSink::create(writePath);
  writers_.push_back(std::make_unique<Writer>(
      options, std::move(sink), *connectorQueryCtx_->memoryPool()));
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
