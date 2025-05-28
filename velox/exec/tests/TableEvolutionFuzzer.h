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

#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

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

  RowTypePtr makeInitialSchema();

  TypePtr evolveType(const TypePtr& old);

  RowTypePtr evolveRowType(
      const RowType& old,
      const std::vector<column_index_t>& bucketColumnIndices);

  std::vector<Setup> makeSetups(
      const std::vector<column_index_t>& bucketColumnIndices);

  static std::unique_ptr<TaskCursor> makeWriteTask(
      const Setup& setup,
      const RowVectorPtr& data,
      const std::string& outputDir,
      const std::vector<column_index_t>& bucketColumnIndices);

  template <typename To, typename From>
  VectorPtr liftToPrimitiveType(
      const FlatVector<From>& input,
      const TypePtr& type);

  VectorPtr liftToType(const VectorPtr& input, const TypePtr& type);

  std::unique_ptr<TaskCursor> makeScanTask(
      const RowTypePtr& tableSchema,
      std::vector<Split> splits,
      bool hasPushDown,
      const PushdownConfig& pushdownConfig);

  const Config config_;
  VectorFuzzer vectorFuzzer_;
  unsigned currentSeed_;
  FuzzerGenerator rng_;
  int64_t sequenceNumber_ = 0;
};

} // namespace facebook::velox::exec::test
