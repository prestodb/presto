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

/// Location related properties of the Hive table to be written
class LocationHandle {
 public:
  enum class TableType {
    kNew, // Write to a new table to be created.
    kExisting, // Write to an existing table.
  };

  LocationHandle(
      std::string targetPath,
      std::string writePath,
      TableType tableType)
      : targetPath_(std::move(targetPath)),
        writePath_(std::move(writePath)),
        tableType_(tableType) {}

  const std::string& targetPath() const {
    return targetPath_;
  }

  const std::string& writePath() const {
    return writePath_;
  }

  TableType tableType() const {
    return tableType_;
  }

 private:
  // Target directory path.
  const std::string targetPath_;
  // Staging directory path.
  const std::string writePath_;
  // Whether the table to be written is new, already existing or temporary.
  const TableType tableType_;
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

  bool isInsertTable() const;

 private:
  const std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns_;
  const std::shared_ptr<const LocationHandle> locationHandle_;
};

/// Parameters for Hive writers.
class HiveWriterParameters {
 public:
  enum class UpdateMode {
    kNew, // Write files to a new directory.
    kOverwrite, // Overwrite an existing directory.
  };

  /// @param updateMode Write the files to a new directory, or append to an
  /// existing directory or overwrite an existing directory.
  /// @param partitionName Partition name in the typical Hive style, which is
  /// also the partition subdirectory part of the partition path.
  /// @param targetFileName The final name of a file after committing.
  /// @param targetDirectory The final directory that a file should be in after
  /// committing.
  /// @param writeFileName The temporary name of the file that a running writer
  /// writes to. If a running writer writes directory to the target file, set
  /// writeFileName to targetFileName by default.
  /// @param writeDirectory The temporary directory that a running writer writes
  /// to. If a running writer writes directory to the target directory, set
  /// writeDirectory to targetDirectory by default.
  HiveWriterParameters(
      UpdateMode updateMode,
      std::optional<std::string> partitionName,
      std::string targetFileName,
      std::string targetDirectory,
      std::optional<std::string> writeFileName = std::nullopt,
      std::optional<std::string> writeDirectory = std::nullopt)
      : updateMode_(updateMode),
        partitionName_(std::move(partitionName)),
        targetFileName_(std::move(targetFileName)),
        targetDirectory_(std::move(targetDirectory)),
        writeFileName_(writeFileName.value_or(targetFileName_)),
        writeDirectory_(writeDirectory.value_or(targetDirectory_)) {}

  UpdateMode updateMode() const {
    return updateMode_;
  }

  static std::string updateModeToString(UpdateMode updateMode) {
    switch (updateMode) {
      case UpdateMode::kNew:
        return "NEW";
      case UpdateMode::kOverwrite:
        return "OVERWRITE";
      default:
        VELOX_UNSUPPORTED("Unsupported update mode.");
    }
  }

  const std::optional<std::string>& partitionName() const {
    return partitionName_;
  }

  const std::string& targetFileName() const {
    return targetFileName_;
  }

  const std::string& writeFileName() const {
    return writeFileName_;
  }

  const std::string& targetDirectory() const {
    return targetDirectory_;
  }

  const std::string& writeDirectory() const {
    return writeDirectory_;
  }

 private:
  const UpdateMode updateMode_;
  const std::optional<std::string> partitionName_;
  const std::string targetFileName_;
  const std::string targetDirectory_;
  const std::string writeFileName_;
  const std::string writeDirectory_;
};

struct HiveWriterInfo {
  explicit HiveWriterInfo(HiveWriterParameters parameters)
      : writerParameters(std::move(parameters)) {}

  const HiveWriterParameters writerParameters;
  vector_size_t numWrittenRows = 0;
};

class HiveDataSink : public DataSink {
 public:
  explicit HiveDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy);

  void appendData(VectorPtr input) override;

  std::vector<std::string> finish() const override;

  void close() override;

 private:
  void createWriter(const std::optional<std::string>& partitionName);

  std::shared_ptr<const HiveWriterParameters> getWriterParameters(
      const std::optional<std::string>& partition) const;

  HiveWriterParameters::UpdateMode getUpdateMode() const;

  const RowTypePtr inputType_;
  const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle_;
  const ConnectorQueryCtx* connectorQueryCtx_;
  const CommitStrategy commitStrategy_;
  // Parameters used by writers, and thus are tracked in the same order
  // as the writers_ vector
  std::vector<std::shared_ptr<HiveWriterInfo>> writerInfo_;
  std::vector<std::unique_ptr<dwrf::Writer>> writers_;
};

} // namespace facebook::velox::connector::hive
