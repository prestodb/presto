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

#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <unordered_set>
#include "velox/expression/fuzzer/ExpressionFuzzer.h"

namespace facebook::velox::exec::test {

class TableEvolutionFuzzer {
 public:
  struct Config {
    int columnCount;
    int evolutionCount;
    std::vector<dwio::common::FileFormat> formats;
    memory::MemoryPool* pool;
  };

  explicit TableEvolutionFuzzer(const Config& config);

  static const std::string& connectorId();

  unsigned seed() const;

  void setSeed(unsigned seed);

  void reSeed();

  // Parses command-line inputted, comma separated string of file formats to be
  // used in the TableEvolutionFuzzer test run. The output is a vector of
  // dwio-packaged defined file formats.
  static const std::vector<dwio::common::FileFormat> parseFileFormats(
      std::string input);

  void run();

  virtual ~TableEvolutionFuzzer() = default;

 private:
  struct Setup {
    // Potentially with different field names, widened types, and additional
    // fields compared to previous setup.
    RowTypePtr schema;

    // New bucket count, must be a multiple of the bucket count in previous
    // setup.
    int log2BucketCount;

    dwio::common::FileFormat fileFormat;

    int bucketCount() const;
  };

  friend std::ostream& operator<<(
      std::ostream& out,
      const TableEvolutionFuzzer::Setup& setup);

  std::string makeNewName();

  TypePtr makeNewType(int maxDepth);

  RowTypePtr makeInitialSchema(
      const std::vector<std::string>& additionalColumnNames = {},
      const std::vector<TypePtr>& additionalColumnTypes = {});

  TypePtr evolveType(const TypePtr& old);

  RowTypePtr evolveRowType(
      const RowType& old,
      const std::vector<column_index_t>& bucketColumnIndices,
      std::unordered_map<std::string, std::string>* columnNameMapping =
          nullptr);

  std::vector<Setup> makeSetups(
      const std::vector<column_index_t>& bucketColumnIndices,
      const std::vector<std::string>& additionalColumnNames = {},
      const std::vector<TypePtr>& additionalColumnTypes = {},
      std::unordered_map<std::string, std::string>* columnNameMapping =
          nullptr);

  static std::unique_ptr<TaskCursor> makeWriteTask(
      const Setup& setup,
      const RowVectorPtr& data,
      const std::string& outputDir,
      const std::vector<column_index_t>& bucketColumnIndices,
      FuzzerGenerator& rng,
      bool enableFlatMap = false);

  template <typename To, typename From>
  VectorPtr liftToPrimitiveType(
      const FlatVector<From>& input,
      const TypePtr& type);

  VectorPtr liftToType(const VectorPtr& input, const TypePtr& type);

  std::unique_ptr<TaskCursor> makeScanTask(
      const RowTypePtr& tableSchema,
      std::vector<Split> splits,
      const PushdownConfig& pushdownConfig,
      bool useFiltersAsNode);

  /// Randomly generates bucket column indices for partitioning data.
  /// Returns a vector of column indices that will be used for bucketing,
  /// with each column having a 1/(2*columnCount) probability of being selected.
  std::vector<column_index_t> generateBucketColumnIndices();

  /// Creates write tasks for all evolution steps.
  /// Generates test data and creates TaskCursor objects for writing data
  /// to temporary directories. Populates the writeTasks vector and sets
  /// finalExpectedData to the data from the last evolution step.
  void createWriteTasks(
      const std::vector<Setup>& testSetups,
      const std::vector<column_index_t>& bucketColumnIndices,
      const std::string& tableOutputRootDirPath,
      std::vector<std::shared_ptr<TaskCursor>>& writeTasks,
      RowVectorPtr& finalExpectedData);

  /// Creates scan splits from write results.
  /// Converts the output of write tasks into scan splits that can be used
  /// for reading the written data back during the scan phase.
  std::pair<std::vector<Split>, std::vector<Split>>
  createScanSplitsFromWriteResults(
      const std::vector<std::vector<RowVectorPtr>>& writeResults,
      const std::vector<Setup>& testSetups,
      const std::vector<column_index_t>& bucketColumnIndices,
      std::optional<int32_t> selectedBucket,
      const RowVectorPtr& finalExpectedData);

  /// Applies remaining filters with updated column names.
  /// Updates filter expressions to use evolved column names based on the
  /// column name mapping tracked during schema evolution.
  void applyRemainingFilters(
      const fuzzer::ExpressionFuzzer::FuzzedExpressionData&
          generatedRemainingFilters,
      const std::unordered_map<std::string, std::string>& columnNameMapping,
      PushdownConfig& pushdownConfig,
      const std::unordered_set<std::string>& subfieldFilteredFields);

  const Config config_;
  VectorFuzzer vectorFuzzer_;
  unsigned currentSeed_;
  FuzzerGenerator rng_;
  int64_t sequenceNumber_ = 0;
};

} // namespace facebook::velox::exec::test
