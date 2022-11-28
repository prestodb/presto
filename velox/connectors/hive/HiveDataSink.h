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
#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::dwrf {
class Writer;
}

namespace facebook::velox::connector::hive {
class HiveColumnHandle;
class HiveWriterParameters;

/// Location related properties of the Hive table to be written
class LocationHandle {
 public:
  enum class TableType {
    kNew, // Write to a new table to be created.
    kExisting, // Write to an existing table.
    kTemporary, // Write to a temporary table.
  };

  enum class WriteMode {
    // Write to a staging directory and then move to the target directory
    // after write finishes.
    kStageAndMoveToTargetDirectory,
    // Directly write to the target directory to be created.
    kDirectToTargetNewDirectory,
    // Directly write to the existing target directory.
    kDirectToTargetExistingDirectory,
  };

  LocationHandle(
      std::string targetPath,
      std::string writePath,
      TableType tableType,
      WriteMode writeMode)
      : targetPath_(std::move(targetPath)),
        writePath_(std::move(writePath)),
        tableType_(tableType),
        writeMode_(writeMode) {}

  const std::string& targetPath() const {
    return targetPath_;
  }

  const std::string& writePath() const {
    return writePath_;
  }

  TableType tableType() const {
    return tableType_;
  }

  WriteMode writeMode() const {
    return writeMode_;
  }

 private:
  // Target directory path.
  const std::string targetPath_;
  // Staging directory path.
  const std::string writePath_;
  // Whether the table to be written is new, already existing or temporary.
  const TableType tableType_;
  // How the target path and directory path could be used.
  const WriteMode writeMode_;
};

/**
 * Represents a request for Hive write
 */
class HiveInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  HiveInsertTableHandle(
      std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns,
      std::shared_ptr<const LocationHandle> locationHandle)
      : inputColumns_(std::move(inputColumns)),
        locationHandle_(std::move(locationHandle)) {}

  virtual ~HiveInsertTableHandle() = default;

  const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns()
      const {
    return inputColumns_;
  }

  const std::shared_ptr<const LocationHandle>& locationHandle() const {
    return locationHandle_;
  }

  bool isPartitioned() const;

  bool isCreateTable() const;

  bool isInsertTable() const;

 private:
  const std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns_;
  const std::shared_ptr<const LocationHandle> locationHandle_;
};

class HiveDataSink : public DataSink {
 public:
  explicit HiveDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx,
      std::shared_ptr<WriteProtocol> writeProtocol);

  std::shared_ptr<ConnectorCommitInfo> getConnectorCommitInfo() const override;

  void appendData(VectorPtr input) override;

  void close() override;

 private:
  std::unique_ptr<dwrf::Writer> createWriter();

  const RowTypePtr inputType_;
  const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle_;
  const ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx_;
  const std::shared_ptr<WriteProtocol> writeProtocol_;
  // Parameters used by writers, and thus are tracked in the same order
  // as the writers_ vector
  std::vector<std::shared_ptr<const HiveWriterParameters>> writerParameters_;
  std::vector<std::unique_ptr<dwrf::Writer>> writers_;
};

} // namespace facebook::velox::connector::hive
