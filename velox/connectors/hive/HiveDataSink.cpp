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
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveWriteProtocol.h"
#include "velox/dwio/dwrf/writer/Writer.h"

using namespace facebook::velox::dwrf;
using WriterConfig = facebook::velox::dwrf::Config;

namespace facebook::velox::connector::hive {

HiveDataSink::HiveDataSink(
    RowTypePtr inputType,
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx,
    std::shared_ptr<WriteProtocol> writeProtocol)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      connectorQueryCtx_(connectorQueryCtx),
      writeProtocol_(std::move(writeProtocol)) {
  VELOX_CHECK_NOT_NULL(
      writeProtocol_, "Write protocol could not be nullptr for HiveDataSink.");
}

std::shared_ptr<ConnectorCommitInfo> HiveDataSink::getConnectorCommitInfo()
    const {
  return std::make_shared<HiveConnectorCommitInfo>(writerParameters_);
}

void HiveDataSink::appendData(VectorPtr input) {
  // For the time being the hive data sink supports one file
  // To extend it we can create a new writer for every
  // partition
  if (writers_.empty()) {
    writers_.emplace_back(createWriter());
  }
  writers_[0]->write(input);
}

void HiveDataSink::close() {
  for (const auto& writer : writers_) {
    writer->close();
  }
}

std::unique_ptr<velox::dwrf::Writer> HiveDataSink::createWriter() {
  auto config = std::make_shared<WriterConfig>();
  // TODO: Wire up serde properties to writer configs.

  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = inputType_;
  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.

  auto hiveWriterParameters =
      std::dynamic_pointer_cast<const HiveWriterParameters>(
          writeProtocol_->getWriterParameters(
              insertTableHandle_, connectorQueryCtx_));
  VELOX_CHECK_NOT_NULL(
      hiveWriterParameters,
      "Hive data sink expects write parameters for Hive.");
  writerParameters_.emplace_back(hiveWriterParameters);

  auto writePath = fs::path(hiveWriterParameters->writeDirectory()) /
      hiveWriterParameters->writeFileName();
  auto sink = dwio::common::DataSink::create(writePath);
  return std::make_unique<Writer>(
      options, std::move(sink), *connectorQueryCtx_->memoryPool());
}

bool HiveInsertTableHandle::isPartitioned() const {
  return std::any_of(
      inputColumns_.begin(), inputColumns_.end(), [](auto column) {
        return column->isPartitionKey();
      });
}

bool HiveInsertTableHandle::isCreateTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kNew;
}

bool HiveInsertTableHandle::isInsertTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kExisting;
}

} // namespace facebook::velox::connector::hive
