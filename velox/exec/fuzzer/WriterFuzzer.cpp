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
#include "velox/exec/fuzzer/WriterFuzzer.h"

#include <boost/random/uniform_int_distribution.hpp>

#include "velox/dwio/dwrf/reader/DwrfReader.h"

#include <unordered_set>
#include "velox/common/base/Fs.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 10, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

namespace facebook::velox::exec::test {

namespace {

class WriterFuzzer {
 public:
  WriterFuzzer(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

  void go();

 private:
  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringLength = 10;
    opts.nullRatio = FLAGS_null_ratio;
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  // Generates at least one and up to maxNumColumns columns to be
  // used as columns of table write.
  // Column names are generated using template '<prefix>N', where N is
  // zero-based ordinal number of the column.
  // Data types is chosen from '<columnTypes>' and for nested complex data type,
  // maxDepth limits the max layers of nesting.
  std::vector<std::string> generateColumns(
      int32_t maxNumColumns,
      const std::string& prefix,
      const std::vector<TypePtr>& dataTypes,
      int32_t maxDepth,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  // Generates input data for table write.
  std::vector<RowVectorPtr> generateInputData(
      std::vector<std::string> names,
      std::vector<TypePtr> types,
      size_t partitionOffset);

  void verifyWriter(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& partitionKeys,
      const std::string& outputDirectoryPath);

  // Executes velox query plan and returns the result.
  RowVectorPtr execute(
      const core::PlanNodePtr& plan,
      const int32_t maxDrivers = 2,
      const std::vector<exec::Split>& splits = {});

  RowVectorPtr veloxToPrestoResult(const RowVectorPtr& result);

  // Query Presto to find out table's location on disk.
  std::string getReferenceOutputDirectoryPath(int32_t layers);

  // Compares if two directories have same partitions
  void comparePartitions(
      const std::string& outputDirectoryPath,
      const std::string& referenceOutputDirectoryPath);

  // Returns all the partition names in tableDirectoryPath.
  std::set<std::string> getPartitionNames(
      const std::string& tableDirectoryPath);

  const std::vector<TypePtr> kRegularColumnTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
  };
  // Supported partition key column types
  // According to VectorHasher::typeKindSupportsValueIds and
  // https://github.com/prestodb/presto/blob/master/presto-hive/src/main/java/com/facebook/presto/hive/HiveUtil.java#L575
  const std::vector<TypePtr> kPartitionKeyTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR()};

  FuzzerGenerator rng_;
  size_t currentSeed_{0};
  std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner_;
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
  std::shared_ptr<memory::MemoryPool> writerPool_{
      rootPool_->addAggregateChild("writerFuzzerWriter")};
  VectorFuzzer vectorFuzzer_;
};
} // namespace

void writerFuzzer(
    size_t seed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
  auto writerFuzzer = WriterFuzzer(seed, std::move(referenceQueryRunner));
  writerFuzzer.go();
}

std::vector<std::string> listFolders(std::string_view path) {
  std::vector<std::string> folders;
  auto fileSystem = filesystems::getFileSystem("/", nullptr);
  for (auto& p : std::filesystem::recursive_directory_iterator(
           fileSystem->extractPath(path))) {
    if (p.is_directory())
      folders.push_back(p.path().string());
  }
  return folders;
}

namespace {
template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

WriterFuzzer::WriterFuzzer(
    size_t initialSeed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
    : referenceQueryRunner_{std::move(referenceQueryRunner)},
      vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
  seed(initialSeed);
}

void WriterFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.")

  auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";

    std::vector<std::string> names;
    std::vector<TypePtr> types;
    std::vector<std::string> partitionKeys;

    generateColumns(5, "c", kRegularColumnTypes_, 2, names, types);
    const auto partitionOffset = names.size();
    // 50% of times test partitioned write.
    if (vectorFuzzer_.coinToss(0.5)) {
      partitionKeys =
          generateColumns(3, "p", kPartitionKeyTypes_, 1, names, types);
    }
    auto input = generateInputData(names, types, partitionOffset);

    auto tempDirPath = exec::test::TempDirectoryPath::create();
    verifyWriter(input, partitionKeys, tempDirPath->getPath());

    LOG(INFO) << "==============================> Done with iteration "
              << iteration++;
    reSeed();
  }
}

std::vector<std::string> WriterFuzzer::generateColumns(
    int32_t maxNumColumns,
    const std::string& prefix,
    const std::vector<TypePtr>& dataTypes,
    int32_t maxDepth,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  const auto numColumns =
      boost::random::uniform_int_distribution<uint32_t>(1, maxNumColumns)(rng_);
  std::vector<std::string> columns;
  for (auto i = 0; i < numColumns; ++i) {
    columns.push_back(fmt::format("{}{}", prefix, i));

    // Pick random, possibly complex, type.
    types.push_back(vectorFuzzer_.randType(dataTypes, maxDepth));
    names.push_back(columns.back());
  }
  return columns;
}

std::vector<RowVectorPtr> WriterFuzzer::generateInputData(
    std::vector<std::string> names,
    std::vector<TypePtr> types,
    size_t partitionOffset) {
  const auto size = vectorFuzzer_.getOptions().vectorSize;
  auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;

  // For partition keys, limit the distinct value to 4 to avoid exceeding
  // partition number limit of 100. Since we could have up to 3 partition
  // keys, it would generate up to 64 partitions.
  std::vector<VectorPtr> partitionValues;
  for (auto i = partitionOffset; i < inputType->size(); ++i) {
    partitionValues.push_back(vectorFuzzer_.fuzz(inputType->childAt(i), 4));
  }

  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    std::vector<VectorPtr> children;
    for (auto j = children.size(); j < inputType->size(); ++j) {
      if (j < partitionOffset) {
        // For regular columns.
        children.push_back(vectorFuzzer_.fuzz(inputType->childAt(j), size));
      } else {
        // TODO Add other encoding support here besides DictionaryVector.
        children.push_back(vectorFuzzer_.fuzzDictionary(
            partitionValues.at(j - partitionOffset), size));
      }
    }
    input.push_back(std::make_shared<RowVector>(
        pool_.get(), inputType, nullptr, size, std::move(children)));
  }

  return input;
}

void WriterFuzzer::verifyWriter(
    const std::vector<RowVectorPtr>& input,
    const std::vector<std::string>& partitionKeys,
    const std::string& outputDirectoryPath) {
  const auto plan = PlanBuilder()
                        .values(input)
                        .tableWrite(outputDirectoryPath, partitionKeys)
                        .planNode();

  const auto maxDrivers =
      boost::random::uniform_int_distribution<int32_t>(1, 16)(rng_);
  const auto result = veloxToPrestoResult(execute(plan, maxDrivers));

  const auto dropSql = "DROP TABLE IF EXISTS tmp_write";
  const auto sql = referenceQueryRunner_->toSql(plan).value();
  referenceQueryRunner_->execute(dropSql);
  std::multiset<std::vector<variant>> expectedResult;
  try {
    expectedResult =
        referenceQueryRunner_->execute(sql, input, plan->outputType());
  } catch (...) {
    LOG(WARNING) << "Query failed in the reference DB";
    return;
  }

  // 1. Verifies the table writer output result: the inserted number of rows.
  VELOX_CHECK_EQ(
      expectedResult.size(), // Presto sql only produces one row which is how
                             // many rows are inserted.
      1,
      "Query returned unexpected result in the reference DB");
  VELOX_CHECK(
      assertEqualResults(expectedResult, plan->outputType(), {result}),
      "Velox and reference DB results don't match");

  // 2. Verifies directory layout.
  const auto referencedOutputDirectoryPath =
      getReferenceOutputDirectoryPath(partitionKeys.size());
  comparePartitions(outputDirectoryPath, referencedOutputDirectoryPath);

  // 3. Verifies data itself.
  auto splits = makeSplits(input, outputDirectoryPath, writerPool_);
  auto readPlan =
      PlanBuilder().tableScan(asRowType(input[0]->type())).planNode();
  auto actual = execute(readPlan, maxDrivers, splits);
  auto reference_data =
      referenceQueryRunner_->execute("SELECT * FROM tmp_write");
  VELOX_CHECK(
      assertEqualResults(reference_data, {actual}),
      "Velox and reference DB results don't match");

  LOG(INFO) << "Verified results against reference DB";
}

RowVectorPtr WriterFuzzer::execute(
    const core::PlanNodePtr& plan,
    const int32_t maxDrivers,
    const std::vector<exec::Split>& splits) {
  LOG(INFO) << "Executing query plan: " << std::endl
            << plan->toString(true, true);
  fuzzer::ResultOrError resultOrError;
  AssertQueryBuilder builder(plan);
  if (!splits.empty()) {
    builder.splits(splits);
  }
  return builder.maxDrivers(maxDrivers).copyResults(pool_.get());
}

RowVectorPtr WriterFuzzer::veloxToPrestoResult(const RowVectorPtr& result) {
  // Velox TableWrite node produces results of following layout
  // row     fragments     context
  // X         null          X
  // null       X            X
  // null       X            X
  // Extract inserted rows from velox execution result.
  std::vector<VectorPtr> insertedRows = {result->childAt(0)->slice(0, 1)};
  return std::make_shared<RowVector>(
      pool_.get(),
      ROW({"count"}, {insertedRows[0]->type()}),
      nullptr,
      1,
      insertedRows);
}

std::string WriterFuzzer::getReferenceOutputDirectoryPath(int32_t layers) {
  auto filePath =
      referenceQueryRunner_->execute("SELECT \"$path\" FROM tmp_write");
  auto tableDirectoryPath =
      fs::path(extractSingleValue<StringView>(filePath)).parent_path();
  while (layers-- > 0) {
    tableDirectoryPath = tableDirectoryPath.parent_path();
  }
  return tableDirectoryPath.string();
}

void WriterFuzzer::comparePartitions(
    const std::string& outputDirectoryPath,
    const std::string& referenceOutputDirectoryPath) {
  const auto partitions = getPartitionNames(outputDirectoryPath);
  LOG(INFO) << "Velox written partitions:" << std::endl;
  for (const std::string& partition : partitions) {
    LOG(INFO) << partition << std::endl;
  }

  const auto referencedPartitions =
      getPartitionNames(referenceOutputDirectoryPath);
  LOG(INFO) << "Presto written partitions:" << std::endl;
  for (const std::string& partition : referencedPartitions) {
    LOG(INFO) << partition << std::endl;
  }

  VELOX_CHECK(
      partitions == referencedPartitions,
      "Velox and reference DB output directory hierarchies don't match");
}

std::set<std::string> WriterFuzzer::getPartitionNames(
    const std::string& tableDirectoryPath) {
  auto fileSystem = filesystems::getFileSystem("/", nullptr);
  auto directories = listFolders(tableDirectoryPath);
  std::set<std::string> partitionNames;

  for (std::string directory : directories) {
    // If it's a hidden directory, ignore
    if (directory.find("/.") != std::string::npos) {
      continue;
    }

    // Remove the path prefix to get the partition name
    // For example: /test/tmp_write/p0=1/p1=2020
    // partition name is /p0=1/p1=2020
    directory.erase(0, fileSystem->extractPath(tableDirectoryPath).length());
    partitionNames.insert(directory);
  }

  return partitionNames;
}

} // namespace
} // namespace facebook::velox::exec::test
