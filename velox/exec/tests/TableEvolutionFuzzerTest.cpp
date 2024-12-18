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

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <filesystem>

DEFINE_uint32(seed, 0, "");
DEFINE_int32(duration_sec, 30, "");
DEFINE_int32(column_count, 5, "");
DEFINE_int32(evolution_count, 5, "");

namespace facebook::velox::exec::test {

namespace {

constexpr int kVectorSize = 101;

VectorFuzzer::Options makeVectorFuzzerOptions() {
  VectorFuzzer::Options options;
  options.vectorSize = kVectorSize;
  options.allowSlice = false;
  return options;
}

} // namespace

class TableEvolutionFuzzer {
 public:
  struct Config {
    int columnCount;
    int evolutionCount;
    std::vector<dwio::common::FileFormat> formats;
    memory::MemoryPool* pool;
  };

  static const std::string& connectorId() {
    static const std::string connectorId(PlanBuilder::kHiveDefaultConnectorId);
    return connectorId;
  }

  explicit TableEvolutionFuzzer(const Config& config)
      : config_(config), vectorFuzzer_(makeVectorFuzzerOptions(), config.pool) {
    VELOX_CHECK_GT(config_.columnCount, 0);
    VELOX_CHECK_GT(config_.evolutionCount, 1);
    VELOX_CHECK(!config_.formats.empty());
  }

  unsigned seed() const {
    return currentSeed_;
  }

  void setSeed(unsigned seed) {
    currentSeed_ = seed;
    rng_.seed(seed);
    vectorFuzzer_.reSeed(rng_());
  }

  void reSeed() {
    setSeed(rng_());
  }

  void run();

 private:
  struct Setup {
    // Potentially with different field names, widened types, and additional
    // fields compared to previous setup.
    RowTypePtr schema;

    // New bucket count, must be a multiple of the bucket count in previous
    // setup.
    int log2BucketCount;

    dwio::common::FileFormat fileFormat;

    int bucketCount() const {
      return 1 << log2BucketCount;
    }
  };

  friend std::ostream& operator<<(
      std::ostream& out,
      const TableEvolutionFuzzer::Setup& setup);

  std::string makeNewName() {
    return fmt::format("name_{}", ++sequenceNumber_);
  }

  TypePtr makeNewType(int maxDepth) {
    // All types that can be written to file directly.
    static const std::vector<TypePtr> scalarTypes = {
        BOOLEAN(),
        TINYINT(),
        SMALLINT(),
        INTEGER(),
        BIGINT(),
        REAL(),
        DOUBLE(),
        VARCHAR(),
        VARBINARY(),
    };
    return vectorFuzzer_.randType(scalarTypes, maxDepth);
  }

  RowTypePtr makeInitialSchema() {
    std::vector<std::string> names(config_.columnCount);
    std::vector<TypePtr> types(config_.columnCount);
    for (int i = 0; i < config_.columnCount; ++i) {
      names[i] = makeNewName();
      types[i] = makeNewType(3);
    }
    return ROW(std::move(names), std::move(types));
  }

  TypePtr evolveType(const TypePtr& old) {
    switch (old->kind()) {
      case TypeKind::ARRAY:
        return ARRAY(evolveType(old->asArray().elementType()));
      case TypeKind::MAP: {
        auto& mapType = old->asMap();
        return MAP(
            evolveType(mapType.keyType()), evolveType(mapType.valueType()));
      }
      case TypeKind::ROW:
        return evolveRowType(old->asRow(), {});
      default:
        if (!folly::Random::oneIn(4, rng_)) {
          return old;
        }
    }

    switch (old->kind()) {
      case TypeKind::TINYINT:
        return SMALLINT();
      case TypeKind::SMALLINT:
        return INTEGER();
      case TypeKind::INTEGER:
        return BIGINT();
      case TypeKind::REAL:
        return DOUBLE();
      default:
        return old;
    }
  }

  RowTypePtr evolveRowType(
      const RowType& old,
      const std::vector<column_index_t>& bucketColumnIndices) {
    auto names = old.names();
    auto types = old.children();
    for (int i = 0, j = 0; i < old.size(); ++i) {
      // Skip evolving bucket column.
      while (j < bucketColumnIndices.size() && bucketColumnIndices[j] < i) {
        ++j;
      }
      if (j < bucketColumnIndices.size() && bucketColumnIndices[j] == i) {
        continue;
      }
      if (folly::Random::oneIn(4, rng_)) {
        names[i] = makeNewName();
      }
      types[i] = evolveType(types[i]);
    }
    if (folly::Random::oneIn(4, rng_)) {
      names.push_back(makeNewName());
      types.push_back(makeNewType(2));
    }
    return ROW(std::move(names), std::move(types));
  }

  std::vector<Setup> makeSetups(
      const std::vector<column_index_t>& bucketColumnIndices) {
    std::vector<Setup> setups(config_.evolutionCount);
    for (int i = 0; i < config_.evolutionCount; ++i) {
      if (i == 0) {
        setups[i].schema = makeInitialSchema();
      } else {
        setups[i].schema =
            evolveRowType(*setups[i - 1].schema, bucketColumnIndices);
      }
      if (!bucketColumnIndices.empty()) {
        if (i == 0) {
          setups[i].log2BucketCount = folly::Random::rand32(1, 4, rng_);
        } else {
          setups[i].log2BucketCount = std::min<int>(
              8,
              setups[i - 1].log2BucketCount + folly::Random::rand32(3, rng_));
        }
      } else {
        setups[i].log2BucketCount = 0;
      }
      setups[i].fileFormat =
          config_.formats[folly::Random::rand32(config_.formats.size(), rng_)];
      VLOG(1) << "Setup " << i << ": " << setups[i];
    }
    return setups;
  }

  static std::unique_ptr<TaskCursor> makeWriteTask(
      const Setup& setup,
      const RowVectorPtr& data,
      const std::string& outputDir,
      const std::vector<column_index_t>& bucketColumnIndices) {
    auto builder = PlanBuilder().values({data});
    if (bucketColumnIndices.empty()) {
      builder.tableWrite(outputDir, setup.fileFormat);
    } else {
      std::vector<std::string> bucketColumnNames;
      bucketColumnNames.reserve(bucketColumnIndices.size());
      for (auto i : bucketColumnIndices) {
        bucketColumnNames.push_back(setup.schema->nameOf(i));
      }
      builder.tableWrite(
          outputDir,
          /*partitionBy=*/{},
          setup.bucketCount(),
          bucketColumnNames,
          setup.fileFormat);
    }
    CursorParameters params;
    params.serialExecution = true;
    params.planNode = builder.planNode();
    return TaskCursor::create(params);
  }

  template <typename To, typename From>
  VectorPtr liftToPrimitiveType(
      const FlatVector<From>& input,
      const TypePtr& type) {
    auto targetBuffer = AlignedBuffer::allocate<To>(input.size(), config_.pool);
    auto* rawTargetValues = targetBuffer->template asMutable<To>();
    auto* rawSourceValues = input.rawValues();
    for (vector_size_t i = 0; i < input.size(); ++i) {
      rawTargetValues[i] = rawSourceValues[i];
    }
    return std::make_shared<FlatVector<To>>(
        config_.pool,
        type,
        input.nulls(),
        input.size(),
        std::move(targetBuffer),
        std::vector<BufferPtr>({}));
  }

  VectorPtr liftToType(const VectorPtr& input, const TypePtr& type);

  std::unique_ptr<TaskCursor> makeScanTask(
      const RowTypePtr& tableSchema,
      std::vector<Split> splits) {
    CursorParameters params;
    params.serialExecution = true;
    // TODO: Mix in filter and aggregate pushdowns.
    params.planNode = PlanBuilder()
                          .tableScan(
                              tableSchema,
                              /*subfieldFilters=*/{},
                              /*remainingFilter=*/"",
                              tableSchema)
                          .planNode();
    auto cursor = TaskCursor::create(params);
    for (auto& split : splits) {
      cursor->task()->addSplit("0", std::move(split));
    }
    cursor->task()->noMoreSplits("0");
    return cursor;
  }

  const Config config_;
  VectorFuzzer vectorFuzzer_;
  unsigned currentSeed_;
  FuzzerGenerator rng_;
  int64_t sequenceNumber_ = 0;
};

namespace {

std::vector<std::vector<RowVectorPtr>> runTaskCursors(
    const std::vector<std::unique_ptr<TaskCursor>>& cursors,
    folly::Executor& executor) {
  std::vector<folly::SemiFuture<std::vector<RowVectorPtr>>> futures;
  for (int i = 0; i < cursors.size(); ++i) {
    auto [promise, future] =
        folly::makePromiseContract<std::vector<RowVectorPtr>>();
    futures.push_back(std::move(future));
    executor.add([&, i, promise = std::move(promise)]() mutable {
      std::vector<RowVectorPtr> results;
      try {
        while (cursors[i]->moveNext()) {
          auto& result = cursors[i]->current();
          result->loadedVector();
          results.push_back(std::move(result));
        }
        promise.setValue(std::move(results));
      } catch (const std::exception& e) {
        LOG(ERROR) << e.what();
        promise.setException(e);
      }
    });
  }
  std::vector<std::vector<RowVectorPtr>> results;
  constexpr std::chrono::seconds kTaskTimeout(10);
  for (auto& future : futures) {
    results.push_back(std::move(future).get(kTaskTimeout));
  }
  return results;
}

// `tableBucketCount' is the bucket count of current table setup when reading.
// `partitionBucketCount' is the bucket count when the partition was written.
// `tableBucketCount' must be a multiple of `partitionBucketCount'.
void buildScanSplitFromTableWriteResult(
    const RowTypePtr& tableSchema,
    const std::vector<column_index_t>& bucketColumnIndices,
    std::optional<int32_t> tableBucket,
    int tableBucketCount,
    int partitionBucketCount,
    dwio::common::FileFormat fileFormat,
    const std::vector<RowVectorPtr>& writeResult,
    std::vector<Split>& splits) {
  VELOX_CHECK_EQ(writeResult.size(), 1);
  auto* fragments =
      writeResult[0]->childAt(1)->asChecked<SimpleVector<StringView>>();
  for (int i = 1; i < writeResult[0]->size(); ++i) {
    auto fragment = folly::parseJson(fragments->valueAt(i));
    auto fileName = fragment["fileWriteInfos"][0]["writeFileName"].asString();
    auto hiveSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
        TableEvolutionFuzzer::connectorId(),
        fmt::format("{}/{}", fragment["writePath"].asString(), fileName),
        fileFormat);
    if (!tableBucket.has_value()) {
      splits.emplace_back(std::move(hiveSplit));
      continue;
    }

    auto fileBucketEnd = fileName.find('_');
    VELOX_CHECK_NE(fileBucketEnd, fileName.npos);
    auto fileBucket = folly::to<int32_t>(fileName.substr(0, fileBucketEnd));
    if (*tableBucket % partitionBucketCount != fileBucket) {
      continue;
    }
    hiveSplit->tableBucketNumber = tableBucket;
    if (partitionBucketCount != tableBucketCount) {
      auto& bucketConversion = hiveSplit->bucketConversion.emplace();
      bucketConversion.tableBucketCount = tableBucketCount;
      bucketConversion.partitionBucketCount = partitionBucketCount;
      for (auto bucketColumnIndex : bucketColumnIndices) {
        auto handle = std::make_unique<connector::hive::HiveColumnHandle>(
            tableSchema->nameOf(bucketColumnIndex),
            connector::hive::HiveColumnHandle::ColumnType::kRegular,
            tableSchema->childAt(bucketColumnIndex),
            tableSchema->childAt(bucketColumnIndex));
        bucketConversion.bucketColumnHandles.push_back(std::move(handle));
      }
    }
    splits.emplace_back(std::move(hiveSplit));
  }
}

void checkResultsEqual(
    const std::vector<RowVectorPtr>& actual,
    const std::vector<RowVectorPtr>& expected) {
  int actualVectorIndex = 0;
  int expectedVectorIndex = 0;
  int actualRowIndex = 0, expectedRowIndex = 0;
  while (actualVectorIndex < actual.size() &&
         expectedVectorIndex < expected.size()) {
    if (actualRowIndex == actual[actualVectorIndex]->size()) {
      ++actualVectorIndex;
      actualRowIndex = 0;
      continue;
    }
    if (expectedRowIndex == expected[expectedVectorIndex]->size()) {
      ++expectedVectorIndex;
      expectedRowIndex = 0;
      continue;
    }
    VELOX_CHECK(actual[actualVectorIndex]->equalValueAt(
        expected[expectedVectorIndex].get(), actualRowIndex, expectedRowIndex));
    ++actualRowIndex;
    ++expectedRowIndex;
  }
  if (actualVectorIndex < actual.size() &&
      actualRowIndex == actual[actualVectorIndex]->size()) {
    ++actualVectorIndex;
    actualRowIndex = 0;
  }
  if (expectedVectorIndex < expected.size() &&
      expectedRowIndex == expected[expectedVectorIndex]->size()) {
    ++expectedVectorIndex;
    expectedRowIndex = 0;
  }
  VELOX_CHECK_EQ(actualVectorIndex, actual.size());
  VELOX_CHECK_EQ(expectedVectorIndex, expected.size());
}

} // namespace

std::ostream& operator<<(
    std::ostream& out,
    const TableEvolutionFuzzer::Setup& setup) {
  out << "schema=" << setup.schema->toString()
      << " log2BucketCount=" << setup.log2BucketCount
      << " fileFormat=" << setup.fileFormat;
  return out;
}

VectorPtr TableEvolutionFuzzer::liftToType(
    const VectorPtr& input,
    const TypePtr& type) {
  switch (input->typeKind()) {
    case TypeKind::TINYINT: {
      auto* typed = input->asChecked<FlatVector<int8_t>>();
      switch (type->kind()) {
        case TypeKind::TINYINT:
          return input;
        case TypeKind::SMALLINT:
          return liftToPrimitiveType<int16_t>(*typed, type);
        case TypeKind::INTEGER:
          return liftToPrimitiveType<int32_t>(*typed, type);
        case TypeKind::BIGINT:
          return liftToPrimitiveType<int64_t>(*typed, type);
        default:
          VELOX_UNREACHABLE();
      }
    }
    case TypeKind::SMALLINT: {
      auto* typed = input->asChecked<FlatVector<int16_t>>();
      switch (type->kind()) {
        case TypeKind::SMALLINT:
          return input;
        case TypeKind::INTEGER:
          return liftToPrimitiveType<int32_t>(*typed, type);
        case TypeKind::BIGINT:
          return liftToPrimitiveType<int64_t>(*typed, type);
        default:
          VELOX_UNREACHABLE();
      }
    }
    case TypeKind::INTEGER: {
      auto* typed = input->asChecked<FlatVector<int32_t>>();
      switch (type->kind()) {
        case TypeKind::INTEGER:
          return input;
        case TypeKind::BIGINT:
          return liftToPrimitiveType<int64_t>(*typed, type);
        default:
          VELOX_UNREACHABLE();
      }
    }
    case TypeKind::REAL: {
      auto* typed = input->asChecked<FlatVector<float>>();
      switch (type->kind()) {
        case TypeKind::REAL:
          return input;
        case TypeKind::DOUBLE:
          return liftToPrimitiveType<double>(*typed, type);
        default:
          VELOX_UNREACHABLE();
      }
    }
    case TypeKind::ARRAY: {
      VELOX_CHECK_EQ(type->kind(), TypeKind::ARRAY);
      auto* array = input->asChecked<ArrayVector>();
      return std::make_shared<ArrayVector>(
          config_.pool,
          type,
          array->nulls(),
          array->size(),
          array->offsets(),
          array->sizes(),
          liftToType(array->elements(), type->asArray().elementType()));
    }
    case TypeKind::MAP: {
      VELOX_CHECK_EQ(type->kind(), TypeKind::MAP);
      auto& mapType = type->asMap();
      auto* map = input->asChecked<MapVector>();
      return std::make_shared<MapVector>(
          config_.pool,
          type,
          map->nulls(),
          map->size(),
          map->offsets(),
          map->sizes(),
          liftToType(map->mapKeys(), mapType.keyType()),
          liftToType(map->mapValues(), mapType.valueType()));
    }
    case TypeKind::ROW: {
      VELOX_CHECK_EQ(type->kind(), TypeKind::ROW);
      auto& rowType = type->asRow();
      auto* row = input->asChecked<RowVector>();
      auto children = row->children();
      for (int i = 0; i < rowType.size(); ++i) {
        auto& childType = rowType.childAt(i);
        if (i < children.size()) {
          children[i] = liftToType(children[i], childType);
        } else {
          children.push_back(BaseVector::createNullConstant(
              childType, row->size(), config_.pool));
        }
      }
      return std::make_shared<RowVector>(
          config_.pool, type, row->nulls(), row->size(), std::move(children));
    }
    default:
      return input;
  }
}

void TableEvolutionFuzzer::run() {
  std::vector<column_index_t> bucketColumnIndices;
  for (int i = 0; i < config_.columnCount; ++i) {
    if (folly::Random::oneIn(2 * config_.columnCount, rng_)) {
      bucketColumnIndices.push_back(i);
    }
  }
  VLOG(1) << "bucketColumnIndices: [" << folly::join(", ", bucketColumnIndices)
          << "]";
  auto testSetups = makeSetups(bucketColumnIndices);
  auto tableOutputRootDir = TempDirectoryPath::create();
  std::vector<std::unique_ptr<TaskCursor>> writeTasks(
      2 * config_.evolutionCount - 1);
  for (int i = 0; i < config_.evolutionCount; ++i) {
    auto data = vectorFuzzer_.fuzzRow(testSetups[i].schema, kVectorSize, false);
    for (auto& child : data->children()) {
      BaseVector::flattenVector(child);
    }
    auto actualDir =
        fmt::format("{}/actual_{}", tableOutputRootDir->getPath(), i);
    VELOX_CHECK(std::filesystem::create_directory(actualDir));
    writeTasks[2 * i] =
        makeWriteTask(testSetups[i], data, actualDir, bucketColumnIndices);
    if (i == config_.evolutionCount - 1) {
      continue;
    }
    auto expectedDir =
        fmt::format("{}/expected_{}", tableOutputRootDir->getPath(), i);
    VELOX_CHECK(std::filesystem::create_directory(expectedDir));
    auto expectedData = std::static_pointer_cast<RowVector>(
        liftToType(data, testSetups.back().schema));
    writeTasks[2 * i + 1] = makeWriteTask(
        testSetups.back(), expectedData, expectedDir, bucketColumnIndices);
  }
  auto executor = folly::getGlobalCPUExecutor();
  auto writeResults = runTaskCursors(writeTasks, *executor);

  std::optional<int32_t> selectedBucket;
  if (!bucketColumnIndices.empty()) {
    selectedBucket =
        folly::Random::rand32(testSetups.back().bucketCount(), rng_);
    VLOG(1) << "selectedBucket=" << *selectedBucket;
  }

  std::vector<Split> actualSplits, expectedSplits;
  for (int i = 0; i < config_.evolutionCount; ++i) {
    auto* result = &writeResults[2 * i];
    buildScanSplitFromTableWriteResult(
        testSetups.back().schema,
        bucketColumnIndices,
        selectedBucket,
        testSetups.back().bucketCount(),
        testSetups[i].bucketCount(),
        testSetups[i].fileFormat,
        *result,
        actualSplits);
    if (i < config_.evolutionCount - 1) {
      result = &writeResults[2 * i + 1];
    }
    buildScanSplitFromTableWriteResult(
        testSetups.back().schema,
        bucketColumnIndices,
        selectedBucket,
        testSetups.back().bucketCount(),
        testSetups.back().bucketCount(),
        testSetups.back().fileFormat,
        *result,
        expectedSplits);
  }
  std::vector<std::unique_ptr<TaskCursor>> scanTasks(2);
  scanTasks[0] =
      makeScanTask(testSetups.back().schema, std::move(actualSplits));
  scanTasks[1] =
      makeScanTask(testSetups.back().schema, std::move(expectedSplits));
  auto scanResults = runTaskCursors(scanTasks, *executor);
  checkResultsEqual(scanResults[0], scanResults[1]);
}

namespace {

void registerFactories(folly::Executor* ioExecutor) {
  filesystems::registerLocalFileSystem();
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              TableEvolutionFuzzer::connectorId(),
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()),
              ioExecutor);
  connector::registerConnector(hiveConnector);
  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();
}

TEST(TableEvolutionFuzzerTest, run) {
  auto pool = memory::memoryManager()->addLeafPool("TableEvolutionFuzzer");
  exec::test::TableEvolutionFuzzer::Config config;
  config.pool = pool.get();
  config.columnCount = FLAGS_column_count;
  config.evolutionCount = FLAGS_evolution_count;
  config.formats = {dwio::common::FileFormat::DWRF};
  exec::test::TableEvolutionFuzzer fuzzer(config);
  fuzzer.setSeed(FLAGS_seed);
  const auto startTime = std::chrono::system_clock::now();
  const auto deadline = startTime + std::chrono::seconds(FLAGS_duration_sec);
  for (int i = 0; std::chrono::system_clock::now() < deadline; ++i) {
    LOG(INFO) << "Starting iteration " << i << ", seed=" << fuzzer.seed();
    fuzzer.run();
    fuzzer.reSeed();
  }
}

} // namespace

} // namespace facebook::velox::exec::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv);
  if (gflags::GetCommandLineFlagInfoOrDie("seed").is_default) {
    FLAGS_seed = std::random_device{}();
    LOG(INFO) << "Use generated random seed " << FLAGS_seed;
  }
  facebook::velox::memory::MemoryManager::initialize({});
  auto ioExecutor = folly::getGlobalIOExecutor();
  facebook::velox::exec::test::registerFactories(ioExecutor.get());
  return RUN_ALL_TESTS();
}
