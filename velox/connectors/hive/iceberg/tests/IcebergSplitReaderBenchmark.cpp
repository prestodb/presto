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

#include "velox/connectors/hive/iceberg/tests/IcebergSplitReaderBenchmark.h"

#include <filesystem>

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::connector::hive::iceberg;
using namespace facebook::velox::memory;

namespace facebook::velox::iceberg::reader::test {
void IcebergSplitReaderBenchmark::writeToFile(
    const std::vector<RowVectorPtr>& batches) {
  auto path = fileFolder_->getPath() + "/" + fileName_;
  auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink = std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
  dwrf::WriterOptions options;
  options.memoryPool = rootPool_.get();
  options.schema = batches[0]->type();
  dwrf::Writer dataFilewriter{std::move(sink), options};
  for (auto& batch : batches) {
    dataFilewriter.write(batch);
  }
  dataFilewriter.flush();
  dataFilewriter.close();
}

void IcebergSplitReaderBenchmark::writeToPositionDeleteFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors) {
  auto localPosWriteFile =
      std::make_unique<LocalWriteFile>(filePath, true, false);
  auto posDeletesink =
      std::make_unique<WriteFileSink>(std::move(localPosWriteFile), filePath);
  dwrf::WriterOptions options;
  options.memoryPool = rootPool_.get();
  options.schema = vectors[0]->type();
  dwrf::Writer posDeletewriter{std::move(posDeletesink), options};
  for (size_t i = 0; i < vectors.size(); ++i) {
    posDeletewriter.write(vectors[i]);
  }
  posDeletewriter.close();
}

std::vector<int64_t> IcebergSplitReaderBenchmark::makeSequenceRows(
    int32_t maxRowNumber) {
  std::vector<int64_t> deleteRows;
  deleteRows.resize(maxRowNumber);
  std::iota(deleteRows.begin(), deleteRows.end(), 0);
  return deleteRows;
}

std::vector<std::string> IcebergSplitReaderBenchmark::listFiles(
    const std::string& dirPath) {
  std::vector<std::string> files;
  for (auto& dirEntry :
       std::filesystem::recursive_directory_iterator(dirPath)) {
    if (dirEntry.is_regular_file()) {
      files.push_back(dirEntry.path().string());
    }
  }
  return files;
}

std::shared_ptr<HiveConnectorSplit>
IcebergSplitReaderBenchmark::makeIcebergSplit(
    const std::string& dataFilePath,
    const std::vector<IcebergDeleteFile>& deleteFiles) {
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  std::unordered_map<std::string, std::string> customSplitInfo;
  customSplitInfo["table_format"] = "hive-iceberg";

  auto readFile = std::make_shared<LocalReadFile>(dataFilePath);
  const int64_t fileSize = readFile->size();

  return std::make_shared<HiveIcebergSplit>(
      kHiveConnectorId,
      dataFilePath,
      fileFomat_,
      0,
      fileSize,
      partitionKeys,
      std::nullopt,
      customSplitInfo,
      nullptr,
      deleteFiles);
}

std::string IcebergSplitReaderBenchmark::writePositionDeleteFile(
    const std::string& dataFilePath,
    int64_t numDeleteRows) {
  facebook::velox::test::VectorMaker vectorMaker{leafPool_.get()};
  auto filePathVector =
      vectorMaker.flatVector<StringView>(numDeleteRows, [&](auto row) {
        if (row < numDeleteRows) {
          return StringView(dataFilePath);
        } else {
          return StringView();
        }
      });

  std::vector<int64_t> deleteRowsVec;
  deleteRowsVec.reserve(numDeleteRows);
  auto deleteRows = makeSequenceRows(numDeleteRows);
  deleteRowsVec.insert(
      deleteRowsVec.end(), deleteRows.begin(), deleteRows.end());

  auto deletePositionsVector = vectorMaker.flatVector<int64_t>(deleteRowsVec);

  std::shared_ptr<IcebergMetadataColumn> pathColumn =
      IcebergMetadataColumn::icebergDeleteFilePathColumn();
  std::shared_ptr<IcebergMetadataColumn> posColumn =
      IcebergMetadataColumn::icebergDeletePosColumn();
  RowVectorPtr deleteFileVectors = vectorMaker.rowVector(
      {pathColumn->name, posColumn->name},
      {filePathVector, deletePositionsVector});

  auto deleteFilePath = deleteFileFolder_->getPath() + "/" + "posDelete.data";
  writeToPositionDeleteFile(deleteFilePath, std::vector{deleteFileVectors});

  return deleteFilePath;
}

std::vector<std::shared_ptr<HiveConnectorSplit>>
IcebergSplitReaderBenchmark::createIcebergSplitsWithPositionalDelete(
    int32_t deleteRowsPercentage,
    int32_t deleteFilesCount) {
  std::vector<std::shared_ptr<HiveConnectorSplit>> splits;

  std::vector<std::string> deleteFilePaths;
  std::vector<std::string> dataFilePaths = listFiles(fileFolder_->getPath());

  for (const auto& dataFilePath : dataFilePaths) {
    std::vector<IcebergDeleteFile> deleteFiles;
    int64_t deleteRowsCount =
        kNumBatches * kNumRowsPerBatch * deleteRowsPercentage * 0.01;
    deleteFiles.reserve(deleteRowsCount);
    for (int i = 0; i < deleteFilesCount; i++) {
      std::string deleteFilePath =
          writePositionDeleteFile(dataFilePath, deleteRowsCount);

      IcebergDeleteFile deleteFile(
          FileContent::kPositionalDeletes,
          deleteFilePath,
          fileFomat_,
          deleteRowsCount,
          testing::internal::GetFileSize(
              std::fopen(deleteFilePath.c_str(), "r")));
      deleteFilePaths.emplace_back(deleteFilePath);
      deleteFiles.emplace_back(deleteFile);
    }
    splits.emplace_back(makeIcebergSplit(dataFilePath, deleteFiles));
  }
  return splits;
}

FilterSpec IcebergSplitReaderBenchmark::createFilterSpec(
    const std::string& columnName,
    float startPct,
    float selectPct,
    const TypePtr& type,
    bool isForRowGroupSkip,
    bool allowNulls) {
  switch (type->childAt(0)->kind()) {
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
      return FilterSpec(
          columnName,
          startPct,
          selectPct,
          FilterKind::kBigintRange,
          isForRowGroupSkip,
          allowNulls);
    default:
      VELOX_FAIL("Unsupported Data Type {}", type->childAt(0)->toString());
  }
  return FilterSpec(columnName, startPct, selectPct, FilterKind(), false);
}

std::shared_ptr<ScanSpec> IcebergSplitReaderBenchmark::createScanSpec(
    const std::vector<RowVectorPtr>& batches,
    RowTypePtr& rowType,
    const std::vector<FilterSpec>& filterSpecs,
    std::vector<uint64_t>& hitRows,
    std::unordered_map<Subfield, std::unique_ptr<Filter>>& filters) {
  std::unique_ptr<FilterGenerator> filterGenerator =
      std::make_unique<FilterGenerator>(rowType, 0);
  filters = filterGenerator->makeSubfieldFilters(
      filterSpecs, batches, nullptr, hitRows);
  auto scanSpec = filterGenerator->makeScanSpec(std::move(filters));
  return scanSpec;
}

// This method is the place where we do the read opeartions using
// icebergSplitReader. scanSpec contains the setting of filters. e.g.
// filterRateX100 = 30 means it would filter out 70% of rows and 30% remain.
// deleteRateX100 = 30 means it would delete 30% of overall data rows and 70%
// remain. Return the number of rows after the filter and delete.
int IcebergSplitReaderBenchmark::read(
    const RowTypePtr& rowType,
    uint32_t nextSize,
    std::unique_ptr<IcebergSplitReader> icebergSplitReader) {
  runtimeStats_ = RuntimeStatistics();
  icebergSplitReader->resetFilterCaches();
  int resultSize = 0;
  auto result = BaseVector::create(rowType, 0, leafPool_.get());
  while (true) {
    bool hasData = icebergSplitReader->next(nextSize, result);
    if (!hasData) {
      break;
    }
    auto rowsRemaining = result->size();
    resultSize += rowsRemaining;
  }
  icebergSplitReader->updateRuntimeStats(runtimeStats_);
  return resultSize;
}

void IcebergSplitReaderBenchmark::readSingleColumn(
    const std::string& columnName,
    const TypePtr& type,
    float startPct,
    float selectPct,
    float deletePct,
    uint32_t nextSize) {
  folly::BenchmarkSuspender suspender;
  auto rowType = ROW({columnName}, {type});

  auto batches =
      dataSetBuilder_->makeDataset(rowType, kNumBatches, kNumRowsPerBatch)
          .withRowGroupSpecificData(kNumRowsPerRowGroup)
          .withNullsForField(Subfield(columnName), 0)
          .build();
  writeToFile(*batches);
  std::vector<FilterSpec> filterSpecs;

  filterSpecs.emplace_back(
      createFilterSpec(columnName, startPct, selectPct, rowType, false, false));

  std::vector<uint64_t> hitRows;
  std::unordered_map<Subfield, std::unique_ptr<Filter>> filters;
  auto scanSpec =
      createScanSpec(*batches, rowType, filterSpecs, hitRows, filters);

  std::vector<std::shared_ptr<HiveConnectorSplit>> splits =
      createIcebergSplitsWithPositionalDelete(deletePct, 1);

  core::TypedExprPtr remainingFilterExpr;

  std::shared_ptr<HiveTableHandle> hiveTableHandle =
      std::make_shared<HiveTableHandle>(
          "kHiveConnectorId",
          "tableName",
          false,
          std::move(filters),
          remainingFilterExpr,
          rowType);

  std::shared_ptr<HiveConfig> hiveConfig =
      std::make_shared<HiveConfig>(std::make_shared<core::MemConfigMutable>());
  const RowTypePtr readerOutputType;
  const std::shared_ptr<io::IoStatistics> ioStats =
      std::make_shared<io::IoStatistics>();

  std::shared_ptr<memory::MemoryPool> root =
      memory::memoryManager()->addRootPool(
          "IcebergSplitReader", kMaxMemory, MemoryReclaimer::create());
  std::shared_ptr<memory::MemoryPool> opPool = root->addLeafChild("operator");
  std::shared_ptr<memory::MemoryPool> connectorPool =
      root->addAggregateChild(kHiveConnectorId, MemoryReclaimer::create());
  std::shared_ptr<core::MemConfig> connectorSessionProperties_ =
      std::make_shared<core::MemConfig>();

  std::unique_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_ =
      std::make_unique<connector::ConnectorQueryCtx>(
          opPool.get(),
          connectorPool.get(),
          connectorSessionProperties_.get(),
          nullptr,
          nullptr,
          nullptr,
          "query.IcebergSplitReader",
          "task.IcebergSplitReader",
          "planNodeId.IcebergSplitReader",
          0,
          "");

  FileHandleFactory fileHandleFactory(
      std::make_unique<SimpleLRUCache<std::string, FileHandle>>(
          hiveConfig->numCacheFileHandles()),
      std::make_unique<FileHandleGenerator>(connectorSessionProperties_));

  suspender.dismiss();

  uint64_t resultSize = 0;
  for (std::shared_ptr<HiveConnectorSplit> split : splits) {
    scanSpec->resetCachedValues(true);
    std::unique_ptr<IcebergSplitReader> icebergSplitReader =
        std::make_unique<IcebergSplitReader>(
            split,
            hiveTableHandle,
            nullptr,
            connectorQueryCtx_.get(),
            hiveConfig,
            rowType,
            ioStats,
            &fileHandleFactory,
            nullptr,
            scanSpec);

    std::shared_ptr<random::RandomSkipTracker> randomSkip;
    icebergSplitReader->configureReaderOptions(randomSkip);
    icebergSplitReader->prepareSplit(nullptr, runtimeStats_, nullptr);

    // Filter range is generated from a small sample data of 4096 rows. So the
    // upperBound and lowerBound are introduced to estimate the result size.
    resultSize += read(rowType, nextSize, std::move(icebergSplitReader));
  }
  // Calculate the expected number of rows after the filters.
  // Add one to expected to avoid 0 in calculating upperBound and lowerBound.
  int expected = kNumBatches * kNumRowsPerBatch * ((double)selectPct / 100) *
          (1 - (double)deletePct / 100) +
      1;

  // Make the upperBound and lowerBound large enough to avoid very small
  // resultSize and expected size, where the diff ratio is relatively very
  // large.
  int upperBound = expected * (1 + kFilterErrorMargin) + 1;
  int lowerBound = expected * (1 - kFilterErrorMargin) - 1;
  upperBound = std::max(16, upperBound);
  lowerBound = std::max(0, lowerBound);

  VELOX_CHECK(
      resultSize <= upperBound && resultSize >= lowerBound,
      "Result Size {} and Expected Size {} Mismatch",
      resultSize,
      expected);
}

void run(
    uint32_t,
    const std::string& columnName,
    const TypePtr& type,
    float filterRateX100,
    float deleteRateX100,
    uint32_t nextSize) {
  RowTypePtr rowType = ROW({columnName}, {type});
  IcebergSplitReaderBenchmark benchmark;
  BIGINT()->toString();
  benchmark.readSingleColumn(
      columnName, type, 0, filterRateX100, deleteRateX100, nextSize);
}

} // namespace facebook::velox::iceberg::reader::test
