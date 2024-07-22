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

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <folly/Singleton.h>

using namespace facebook::velox::exec::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::test;

namespace facebook::velox::connector::hive::iceberg {

class HiveIcebergTest : public HiveConnectorTestBase {
 public:
  HiveIcebergTest()
      : config_{std::make_shared<facebook::velox::dwrf::Config>()} {
    // Make the writers flush per batch so that we can create non-aligned
    // RowGroups between the base data files and delete files
    flushPolicyFactory_ = []() {
      return std::make_unique<dwrf::LambdaFlushPolicy>([]() { return true; });
    };
  }

  /// Create 1 base data file data_file_1 with 2 RowGroups of 10000 rows each.
  /// Also create 1 delete file delete_file_1 which contains delete positions
  /// for data_file_1.
  void assertSingleBaseFileSingleDeleteFile(
      const std::vector<int64_t>& deletePositionsVec) {
    std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles = {
        {"data_file_1", {10000, 10000}}};
    std::unordered_map<
        std::string,
        std::multimap<std::string, std::vector<int64_t>>>
        deleteFilesForBaseDatafiles = {
            {"delete_file_1", {{"data_file_1", deletePositionsVec}}}};

    assertPositionalDeletes(
        rowGroupSizesForFiles, deleteFilesForBaseDatafiles, 0);
  }

  /// Create 3 base data files, where the first file data_file_0 has 500 rows,
  /// the second file data_file_1 contains 2 RowGroups of 10000 rows each, and
  /// the third file data_file_2 contains 500 rows. It creates 1 positional
  /// delete file delete_file_1, which contains delete positions for
  /// data_file_1.
  void assertMultipleBaseFileSingleDeleteFile(
      const std::vector<int64_t>& deletePositionsVec) {
    int64_t previousFileRowCount = 500;
    int64_t afterFileRowCount = 500;

    assertPositionalDeletes(
        {
            {"data_file_0", {previousFileRowCount}},
            {"data_file_1", {10000, 10000}},
            {"data_file_2", {afterFileRowCount}},
        },
        {{"delete_file_1", {{"data_file_1", deletePositionsVec}}}},
        0);
  }

  /// Create 1 base data file data_file_1 with 2 RowGroups of 10000 rows each.
  /// Create multiple delete files with name data_file_1, data_file_2, and so on
  void assertSingleBaseFileMultipleDeleteFiles(
      const std::vector<std::vector<int64_t>>& deletePositionsVecs) {
    std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles = {
        {"data_file_1", {10000, 10000}}};

    std::unordered_map<
        std::string,
        std::multimap<std::string, std::vector<int64_t>>>
        deleteFilesForBaseDatafiles;
    for (int i = 0; i < deletePositionsVecs.size(); i++) {
      std::string deleteFileName = fmt::format("delete_file_{}", i);
      deleteFilesForBaseDatafiles[deleteFileName] = {
          {"data_file_1", deletePositionsVecs[i]}};
    }
    assertPositionalDeletes(
        rowGroupSizesForFiles, deleteFilesForBaseDatafiles, 0);
  }

  void assertMultipleSplits(
      const std::vector<int64_t>& deletePositions,
      int32_t splitCount,
      int32_t numPrefetchSplits) {
    std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles;
    for (int32_t i = 0; i < splitCount; i++) {
      std::string dataFileName = fmt::format("data_file_{}", i);
      rowGroupSizesForFiles[dataFileName] = {rowCount};
    }

    std::unordered_map<
        std::string,
        std::multimap<std::string, std::vector<int64_t>>>
        deleteFilesForBaseDatafiles;
    for (int i = 0; i < splitCount; i++) {
      std::string deleteFileName = fmt::format("delete_file_{}", i);
      deleteFilesForBaseDatafiles[deleteFileName] = {
          {fmt::format("data_file_{}", i), deletePositions}};
    }

    assertPositionalDeletes(
        rowGroupSizesForFiles, deleteFilesForBaseDatafiles, numPrefetchSplits);
  }

  std::vector<int64_t> makeRandomIncreasingValues(int64_t begin, int64_t end) {
    VELOX_CHECK(begin < end);

    std::mt19937 gen{0};
    std::vector<int64_t> values;
    values.reserve(end - begin);
    for (int i = begin; i < end; i++) {
      if (folly::Random::rand32(0, 10, gen) > 8) {
        values.push_back(i);
      }
    }
    return values;
  }

  std::vector<int64_t> makeContinuousIncreasingValues(
      int64_t begin,
      int64_t end) {
    std::vector<int64_t> values;
    values.resize(end - begin);
    std::iota(values.begin(), values.end(), begin);
    return values;
  }

  /// @rowGroupSizesForFiles The key is the file name, and the value is a vector
  /// of RowGroup sizes
  /// @deleteFilesForBaseDatafiles The key is the delete file name, and the
  /// value contains the information about the content of this delete file.
  /// e.g. {
  ///         "delete_file_1",
  ///         {
  ///             {"data_file_1", {1, 2, 3}},
  ///             {"data_file_1", {4, 5, 6}},
  ///             {"data_file_2", {0, 2, 4}}
  ///         }
  ///     }
  /// represents one delete file called delete_file_1, which contains delete
  /// positions for data_file_1 and data_file_2. THere are 3 RowGroups in this
  /// delete file, the first two contain positions for data_file_1, and the last
  /// contain positions for data_file_2
  void assertPositionalDeletes(
      const std::map<std::string, std::vector<int64_t>>& rowGroupSizesForFiles,
      const std::unordered_map<
          std::string,
          std::multimap<std::string, std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles,
      int32_t numPrefetchSplits = 0) {
    // Keep the reference to the deleteFilePath, otherwise the corresponding
    // file will be deleted.
    std::map<std::string, std::shared_ptr<TempFilePath>> dataFilePaths =
        writeDataFiles(rowGroupSizesForFiles);
    std::unordered_map<
        std::string,
        std::pair<int64_t, std::shared_ptr<TempFilePath>>>
        deleteFilePaths = writePositionDeleteFiles(
            deleteFilesForBaseDatafiles, dataFilePaths);

    std::vector<std::shared_ptr<ConnectorSplit>> splits;

    for (const auto& dataFile : dataFilePaths) {
      std::string baseFileName = dataFile.first;
      std::string baseFilePath = dataFile.second->getPath();

      std::vector<IcebergDeleteFile> deleteFiles;

      for (auto const& deleteFile : deleteFilesForBaseDatafiles) {
        std::string deleteFileName = deleteFile.first;
        std::multimap<std::string, std::vector<int64_t>> deleteFileContent =
            deleteFile.second;

        if (deleteFileContent.count(baseFileName) != 0) {
          // If this delete file contains rows for the target base file, then
          // add it to the split
          auto deleteFilePath =
              deleteFilePaths[deleteFileName].second->getPath();
          IcebergDeleteFile deleteFile(
              FileContent::kPositionalDeletes,
              deleteFilePath,
              fileFomat_,
              deleteFilePaths[deleteFileName].first,
              testing::internal::GetFileSize(
                  std::fopen(deleteFilePath.c_str(), "r")));
          deleteFiles.push_back(deleteFile);
        }
      }

      splits.emplace_back(makeIcebergSplit(baseFilePath, deleteFiles));
    }

    std::string duckdbSql =
        getDuckDBQuery(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
    auto plan = tableScanNode();
    auto task = HiveConnectorTestBase::assertQuery(
        plan, splits, duckdbSql, numPrefetchSplits);

    auto planStats = toPlanStats(task->taskStats());
    auto scanNodeId = plan->id();
    auto it = planStats.find(scanNodeId);
    ASSERT_TRUE(it != planStats.end());
    ASSERT_TRUE(it->second.peakMemoryBytes > 0);
  }

  const static int rowCount = 20000;

 private:
  std::map<std::string, std::shared_ptr<TempFilePath>> writeDataFiles(
      std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles) {
    std::map<std::string, std::shared_ptr<TempFilePath>> dataFilePaths;

    std::vector<RowVectorPtr> dataVectorsJoined;
    dataVectorsJoined.reserve(rowGroupSizesForFiles.size());

    int64_t startingValue = 0;
    for (auto& dataFile : rowGroupSizesForFiles) {
      dataFilePaths[dataFile.first] = TempFilePath::create();

      // We make the values are continuously increasing even across base data
      // files. This is to make constructing DuckDB queries easier
      std::vector<RowVectorPtr> dataVectors =
          makeVectors(dataFile.second, startingValue);
      writeToFile(
          dataFilePaths[dataFile.first]->getPath(),
          dataVectors,
          config_,
          flushPolicyFactory_);

      for (int i = 0; i < dataVectors.size(); i++) {
        dataVectorsJoined.push_back(dataVectors[i]);
      }
    }

    createDuckDbTable(dataVectorsJoined);
    return dataFilePaths;
  }

  /// Input is like <"deleteFile1", <"dataFile1", {pos_RG1, pos_RG2,..}>,
  /// <"dataFile2", {pos_RG1, pos_RG2,..}>
  std::unordered_map<
      std::string,
      std::pair<int64_t, std::shared_ptr<TempFilePath>>>
  writePositionDeleteFiles(
      const std::unordered_map<
          std::string, // delete file name
          std::multimap<
              std::string,
              std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles, // <base file name, delete position
                                       // vector for all RowGroups>
      std::map<std::string, std::shared_ptr<TempFilePath>> baseFilePaths) {
    std::unordered_map<
        std::string,
        std::pair<int64_t, std::shared_ptr<TempFilePath>>>
        deleteFilePaths;
    deleteFilePaths.reserve(deleteFilesForBaseDatafiles.size());

    for (auto& deleteFile : deleteFilesForBaseDatafiles) {
      auto deleteFileName = deleteFile.first;
      auto deleteFileContent = deleteFile.second;
      auto deleteFilePath = TempFilePath::create();

      std::vector<RowVectorPtr> deleteFileVectors;
      int64_t totalPositionsInDeleteFile = 0;

      for (auto& deleteFileRowGroup : deleteFileContent) {
        auto baseFileName = deleteFileRowGroup.first;
        auto baseFilePath = baseFilePaths[baseFileName]->getPath();
        auto positionsInRowGroup = deleteFileRowGroup.second;

        auto filePathVector = makeFlatVector<std::string>(
            static_cast<vector_size_t>(positionsInRowGroup.size()),
            [&](vector_size_t row) { return baseFilePath; });
        auto deletePosVector = makeFlatVector<int64_t>(positionsInRowGroup);

        RowVectorPtr deleteFileVector = makeRowVector(
            {pathColumn_->name, posColumn_->name},
            {filePathVector, deletePosVector});

        deleteFileVectors.push_back(deleteFileVector);
        totalPositionsInDeleteFile += positionsInRowGroup.size();
      }

      writeToFile(
          deleteFilePath->getPath(),
          deleteFileVectors,
          config_,
          flushPolicyFactory_);

      deleteFilePaths[deleteFileName] =
          std::make_pair(totalPositionsInDeleteFile, deleteFilePath);
    }

    return deleteFilePaths;
  }

  std::vector<RowVectorPtr> makeVectors(
      std::vector<int64_t> vectorSizes,
      int64_t& startingValue) {
    std::vector<RowVectorPtr> vectors;
    vectors.reserve(vectorSizes.size());

    vectors.reserve(vectorSizes.size());
    for (int j = 0; j < vectorSizes.size(); j++) {
      auto data = makeContinuousIncreasingValues(
          startingValue, startingValue + vectorSizes[j]);
      VectorPtr c0 = vectorMaker_.flatVector<int64_t>(data);
      vectors.push_back(makeRowVector({"c0"}, {c0}));
      startingValue += vectorSizes[j];
    }

    return vectors;
  }

  std::shared_ptr<ConnectorSplit> makeIcebergSplit(
      const std::string& dataFilePath,
      const std::vector<IcebergDeleteFile>& deleteFiles = {}) {
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
    std::unordered_map<std::string, std::string> customSplitInfo;
    customSplitInfo["table_format"] = "hive-iceberg";

    auto file = filesystems::getFileSystem(dataFilePath, nullptr)
                    ->openFileForRead(dataFilePath);
    const int64_t fileSize = file->size();

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

  std::string getDuckDBQuery(
      const std::map<std::string, std::vector<int64_t>>& rowGroupSizesForFiles,
      const std::unordered_map<
          std::string,
          std::multimap<std::string, std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles) {
    int64_t totalNumRowsInAllBaseFiles = 0;
    std::map<std::string, int64_t> baseFileSizes;
    for (auto rowGroupSizesInFile : rowGroupSizesForFiles) {
      // Sum up the row counts in all RowGroups in each base file
      baseFileSizes[rowGroupSizesInFile.first] += std::accumulate(
          rowGroupSizesInFile.second.begin(),
          rowGroupSizesInFile.second.end(),
          0LL);
      totalNumRowsInAllBaseFiles += baseFileSizes[rowGroupSizesInFile.first];
    }

    // Group the delete vectors by baseFileName
    std::map<std::string, std::vector<std::vector<int64_t>>>
        deletePosVectorsForAllBaseFiles;
    for (auto deleteFile : deleteFilesForBaseDatafiles) {
      auto deleteFileContent = deleteFile.second;
      for (auto rowGroup : deleteFileContent) {
        auto baseFileName = rowGroup.first;
        deletePosVectorsForAllBaseFiles[baseFileName].push_back(
            rowGroup.second);
      }
    }

    // Flatten and deduplicate the delete position vectors in
    // deletePosVectorsForAllBaseFiles from previous step, and count the total
    // number of distinct delete positions for all base files
    std::map<std::string, std::vector<int64_t>>
        flattenedDeletePosVectorsForAllBaseFiles;
    int64_t totalNumDeletePositions = 0;
    for (auto deleteVectorsForBaseFile : deletePosVectorsForAllBaseFiles) {
      auto baseFileName = deleteVectorsForBaseFile.first;
      auto deletePositionVectors = deleteVectorsForBaseFile.second;
      std::vector<int64_t> deletePositionVector =
          flattenAndDedup(deletePositionVectors, baseFileSizes[baseFileName]);
      flattenedDeletePosVectorsForAllBaseFiles[baseFileName] =
          deletePositionVector;
      totalNumDeletePositions += deletePositionVector.size();
    }

    // Now build the DuckDB queries
    if (totalNumDeletePositions == 0) {
      return "SELECT * FROM tmp";
    } else if (totalNumDeletePositions >= totalNumRowsInAllBaseFiles) {
      return "SELECT * FROM tmp WHERE 1 = 0";
    } else {
      // Convert the delete positions in all base files into column values
      std::vector<int64_t> allDeleteValues;

      int64_t numRowsInPreviousBaseFiles = 0;
      for (auto baseFileSize : baseFileSizes) {
        auto deletePositions =
            flattenedDeletePosVectorsForAllBaseFiles[baseFileSize.first];

        if (numRowsInPreviousBaseFiles > 0) {
          for (int64_t& deleteValue : deletePositions) {
            deleteValue += numRowsInPreviousBaseFiles;
          }
        }

        allDeleteValues.insert(
            allDeleteValues.end(),
            deletePositions.begin(),
            deletePositions.end());

        numRowsInPreviousBaseFiles += baseFileSize.second;
      }

      return fmt::format(
          "SELECT * FROM tmp WHERE c0 NOT IN ({})",
          makeNotInList(allDeleteValues));
    }
  }

  std::vector<int64_t> flattenAndDedup(
      const std::vector<std::vector<int64_t>>& deletePositionVectors,
      int64_t baseFileSize) {
    std::vector<int64_t> deletePositionVector;
    for (auto vec : deletePositionVectors) {
      for (auto pos : vec) {
        if (pos >= 0 && pos < baseFileSize) {
          deletePositionVector.push_back(pos);
        }
      }
    }

    std::sort(deletePositionVector.begin(), deletePositionVector.end());
    auto last =
        std::unique(deletePositionVector.begin(), deletePositionVector.end());
    deletePositionVector.erase(last, deletePositionVector.end());

    return deletePositionVector;
  }

  std::string makeNotInList(const std::vector<int64_t>& deletePositionVector) {
    if (deletePositionVector.empty()) {
      return "";
    }

    return std::accumulate(
        deletePositionVector.begin() + 1,
        deletePositionVector.end(),
        std::to_string(deletePositionVector[0]),
        [](const std::string& a, int64_t b) {
          return a + ", " + std::to_string(b);
        });
  }

  core::PlanNodePtr tableScanNode() {
    return PlanBuilder(pool_.get()).tableScan(rowType_).planNode();
  }

  dwio::common::FileFormat fileFomat_{dwio::common::FileFormat::DWRF};
  std::shared_ptr<dwrf::Config> config_;
  std::function<std::unique_ptr<dwrf::DWRFFlushPolicy>()> flushPolicyFactory_;

  RowTypePtr rowType_{ROW({"c0"}, {BIGINT()})};
  std::shared_ptr<IcebergMetadataColumn> pathColumn_ =
      IcebergMetadataColumn::icebergDeleteFilePathColumn();
  std::shared_ptr<IcebergMetadataColumn> posColumn_ =
      IcebergMetadataColumn::icebergDeletePosColumn();
};

/// This test creates one single data file and one delete file. The parameter
/// passed to assertSingleBaseFileSingleDeleteFile is the delete positions.
TEST_F(HiveIcebergTest, singleBaseFileSinglePositionalDeleteFile) {
  folly::SingletonVault::singleton()->registrationComplete();

  assertSingleBaseFileSingleDeleteFile({{0, 1, 2, 3}});
  // Delete the first and last row in each batch (10000 rows per batch)
  assertSingleBaseFileSingleDeleteFile({{0, 9999, 10000, 19999}});
  // Delete several rows in the second batch (10000 rows per batch)
  assertSingleBaseFileSingleDeleteFile({{10000, 10002, 19999}});
  // Delete random rows
  assertSingleBaseFileSingleDeleteFile({makeRandomIncreasingValues(0, 20000)});
  // Delete 0 rows
  assertSingleBaseFileSingleDeleteFile({});
  // Delete all rows
  assertSingleBaseFileSingleDeleteFile(
      {makeContinuousIncreasingValues(0, 20000)});
  // Delete rows that don't exist
  assertSingleBaseFileSingleDeleteFile({{20000, 29999}});
}

/// This test creates 3 base data files, only the middle one has corresponding
/// delete positions. The parameter passed to
/// assertSingleBaseFileSingleDeleteFile is the delete positions.for the middle
/// base file.
TEST_F(HiveIcebergTest, MultipleBaseFilesSinglePositionalDeleteFile) {
  folly::SingletonVault::singleton()->registrationComplete();

  assertMultipleBaseFileSingleDeleteFile({0, 1, 2, 3});
  assertMultipleBaseFileSingleDeleteFile({0, 9999, 10000, 19999});
  assertMultipleBaseFileSingleDeleteFile({10000, 10002, 19999});
  assertMultipleBaseFileSingleDeleteFile({10000, 10002, 19999});
  assertMultipleBaseFileSingleDeleteFile(
      makeRandomIncreasingValues(0, rowCount));
  assertMultipleBaseFileSingleDeleteFile({});
  assertMultipleBaseFileSingleDeleteFile(
      makeContinuousIncreasingValues(0, rowCount));
}

/// This test creates one base data file/split with multiple delete files. The
/// parameter passed to assertSingleBaseFileMultipleDeleteFiles is the vector of
/// delete files. Each leaf vector represents the delete positions in that
/// delete file.
TEST_F(HiveIcebergTest, singleBaseFileMultiplePositionalDeleteFiles) {
  folly::SingletonVault::singleton()->registrationComplete();

  // Delete row 0, 1, 2, 3 from the first batch out of two.
  assertSingleBaseFileMultipleDeleteFiles({{1}, {2}, {3}, {4}});
  // Delete the first and last row in each batch (10000 rows per batch).
  assertSingleBaseFileMultipleDeleteFiles({{0}, {9999}, {10000}, {19999}});

  assertSingleBaseFileMultipleDeleteFiles({{500, 21000}});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeRandomIncreasingValues(0, 10000),
       makeRandomIncreasingValues(10000, 20000),
       makeRandomIncreasingValues(5000, 15000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeContinuousIncreasingValues(0, 10000),
       makeContinuousIncreasingValues(10000, 20000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeContinuousIncreasingValues(0, 10000),
       makeContinuousIncreasingValues(10000, 20000),
       makeRandomIncreasingValues(5000, 15000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeContinuousIncreasingValues(0, 20000),
       makeContinuousIncreasingValues(0, 20000)});

  assertSingleBaseFileMultipleDeleteFiles(
      {makeRandomIncreasingValues(0, 20000),
       {},
       makeRandomIncreasingValues(5000, 15000)});

  assertSingleBaseFileMultipleDeleteFiles({{}, {}});
}

/// This test creates 2 base data files, and 1 or 2 delete files, with unaligned
/// RowGroup boundaries
TEST_F(HiveIcebergTest, multipleBaseFileMultiplePositionalDeleteFiles) {
  folly::SingletonVault::singleton()->registrationComplete();

  std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles;
  std::unordered_map<
      std::string,
      std::multimap<std::string, std::vector<int64_t>>>
      deleteFilesForBaseDatafiles;

  // Create two data files, each with two RowGroups
  rowGroupSizesForFiles["data_file_1"] = {100, 85};
  rowGroupSizesForFiles["data_file_2"] = {99, 1};

  // Delete 3 rows from the first RowGroup in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {{"data_file_1", {0, 1, 99}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete 3 rows from the second RowGroup in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {100, 101, 184}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete random rows from the both RowGroups in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(0, 185)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete all rows in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeContinuousIncreasingValues(0, 185)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
  //
  // Delete non-existent rows from data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(186, 300)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete several rows from both RowGroups in both data files
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 100, 102, 184}}, {"data_file_2", {1, 98, 99}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // The delete file delete_file_1 contains 3 RowGroups itself, with the first 3
  // deleting some repeating rows in data_file_1, and the last 2 RowGroups
  // deleting some  repeating rows in data_file_2
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 1, 2, 3}},
      {"data_file_1", {1, 2, 3, 4}},
      {"data_file_1", makeRandomIncreasingValues(0, 185)},
      {"data_file_2", {1, 3, 5, 7}},
      {"data_file_2", makeRandomIncreasingValues(0, 100)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // delete_file_2 contains non-overlapping delete rows for each data files in
  // each RowGroup
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 1, 2, 3}}, {"data_file_2", {1, 3, 5, 7}}};
  deleteFilesForBaseDatafiles["delete_file_2"] = {
      {"data_file_1", {1, 2, 3, 4}},
      {"data_file_1", {98, 99, 100, 101, 184}},
      {"data_file_2", {3, 5, 7, 9}},
      {"data_file_2", {98, 99, 100}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Two delete files each containing overlapping delete rows for both data
  // files
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(0, 185)},
      {"data_file_2", makeRandomIncreasingValues(0, 100)}};
  deleteFilesForBaseDatafiles["delete_file_2"] = {
      {"data_file_1", makeRandomIncreasingValues(10, 120)},
      {"data_file_2", makeRandomIncreasingValues(50, 100)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
}

TEST_F(HiveIcebergTest, positionalDeletesMultipleSplits) {
  folly::SingletonVault::singleton()->registrationComplete();

  assertMultipleSplits({1, 2, 3, 4}, 10, 5);
  assertMultipleSplits({1, 2, 3, 4}, 10, 0);
  assertMultipleSplits({1, 2, 3, 4}, 10, 10);
  assertMultipleSplits({0, 9999, 10000, 19999}, 10, 3);
  assertMultipleSplits(makeRandomIncreasingValues(0, 20000), 10, 3);
  assertMultipleSplits(makeContinuousIncreasingValues(0, 20000), 10, 3);
  assertMultipleSplits({}, 10, 3);
}

} // namespace facebook::velox::connector::hive::iceberg
