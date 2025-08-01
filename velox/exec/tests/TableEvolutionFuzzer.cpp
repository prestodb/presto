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

#include "velox/exec/tests/TableEvolutionFuzzer.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/tests/utils/FilterGenerator.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <filesystem>

DEFINE_bool(
    enable_oom_injection_write_path,
    false,
    "When enabled OOMs will randomly be triggered while executing the write path "
    "The goal of this mode is to ensure unexpected exceptions "
    "aren't thrown and the process isn't killed in the process of cleaning "
    "up after failures. Therefore, results are not compared when this is "
    "enabled. Note that this option only works in debug builds.");

DEFINE_bool(
    enable_oom_injection_read_path,
    false,
    "When enabled OOMs will randomly be triggered while executing scan "
    "plans. The goal of this mode is to ensure unexpected exceptions "
    "aren't thrown and the process isn't killed in the process of cleaning "
    "up after failures. Therefore, results are not compared when this is "
    "enabled. Note that this option only works in debug builds.");

namespace facebook::velox::exec::test {

std::ostream& operator<<(
    std::ostream& out,
    const TableEvolutionFuzzer::Setup& setup) {
  out << "schema=" << setup.schema->toString()
      << " log2BucketCount=" << setup.log2BucketCount
      << " fileFormat=" << setup.fileFormat;
  return out;
}

namespace {

constexpr int kVectorSize = 101;

VectorFuzzer::Options makeVectorFuzzerOptions() {
  VectorFuzzer::Options options;
  options.vectorSize = kVectorSize;
  options.allowSlice = false;
  return options;
}

} // namespace

TableEvolutionFuzzer::TableEvolutionFuzzer(const Config& config)
    : config_(config), vectorFuzzer_(makeVectorFuzzerOptions(), config.pool) {
  VELOX_CHECK_GT(config_.columnCount, 0);
  VELOX_CHECK_GT(config_.evolutionCount, 1);
}

const std::string& TableEvolutionFuzzer::connectorId() {
  static const std::string connectorId(PlanBuilder::kHiveDefaultConnectorId);
  return connectorId;
}

unsigned TableEvolutionFuzzer::seed() const {
  return currentSeed_;
}

void TableEvolutionFuzzer::setSeed(unsigned seed) {
  currentSeed_ = seed;
  rng_.seed(seed);
  vectorFuzzer_.reSeed(rng_());
}

void TableEvolutionFuzzer::reSeed() {
  setSeed(rng_());
}

const std::vector<dwio::common::FileFormat>
TableEvolutionFuzzer::parseFileFormats(std::string input) {
  std::vector<std::string> formatsAsStrings;
  folly::split(",", input, formatsAsStrings);
  VELOX_CHECK(!formatsAsStrings.empty(), "No file formats specified");
  std::vector<dwio::common::FileFormat> formats;
  for (const auto& formatAsString : formatsAsStrings) {
    auto format = dwio::common::toFileFormat(formatAsString);
    VELOX_CHECK_NE(
        format,
        dwio::common::FileFormat::UNKNOWN,
        "Config contains UNKNOWN file format");
    formats.push_back(format);
  }
  return formats;
}

namespace {

std::vector<std::vector<RowVectorPtr>> runTaskCursors(
    const std::vector<std::shared_ptr<TaskCursor>>& cursors,
    folly::Executor& executor) {
  std::vector<folly::SemiFuture<std::vector<RowVectorPtr>>> futures;
  for (int i = 0; i < cursors.size(); ++i) {
    auto [promise, future] =
        folly::makePromiseContract<std::vector<RowVectorPtr>>();
    futures.push_back(std::move(future));
    auto cursorPtr = cursors[i];
    auto task = cursorPtr->task();
    executor.add([cursorPtr, task, promise = std::move(promise)]() mutable {
      std::vector<RowVectorPtr> results;
      try {
        while (cursorPtr->moveNext()) {
          auto& result = cursorPtr->current();
          result->loadedVector();
          results.push_back(std::move(result));
        }
        promise.setValue(std::move(results));
      } catch (VeloxRuntimeError& e) {
        if (FLAGS_enable_oom_injection_write_path &&
            e.errorCode() == facebook::velox::error_code::kMemCapExceeded &&
            e.message() == ScopedOOMInjector::kErrorMessage) {
          // If we enabled OOM injection we expect the exception thrown by the
          // ScopedOOMInjector.
          LOG(INFO) << "OOM injection triggered in write path: " << e.what();
          promise.setValue(std::move(results));
        } else if (
            FLAGS_enable_oom_injection_read_path &&
            e.errorCode() == facebook::velox::error_code::kMemCapExceeded &&
            e.message() == ScopedOOMInjector::kErrorMessage) {
          // If we enabled OOM injection we expect the exception thrown by the
          // ScopedOOMInjector.
          LOG(INFO) << "OOM injection triggered in read path: " << e.what();
          promise.setValue(std::move(results));
        } else {
          LOG(ERROR) << e.what();
          promise.setException(e);
        }
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
  if (FLAGS_enable_oom_injection_write_path) {
    return;
  }
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

common::SubfieldFilters generateSubfieldFilters(
    RowTypePtr& rowType,
    const RowVectorPtr& finalExpectedData) {
  dwio::common::MutationSpec mutations;
  std::vector<uint64_t> hitRows;

  std::unique_ptr<velox::dwio::common::FilterGenerator> filterGenerator =
      std::make_unique<velox::dwio::common::FilterGenerator>(rowType, 0);

  auto subfieldsVector = filterGenerator->makeFilterables(rowType->size(), 100);

  const auto& filterSpecs =
      filterGenerator->makeRandomSpecs(subfieldsVector, 100);

  return filterGenerator->makeSubfieldFilters(
      filterSpecs, {finalExpectedData}, &mutations, hitRows);
}

} // namespace

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
  ScopedOOMInjector oomInjectorWritePath(
      []() -> bool { return folly::Random::oneIn(10); },
      10); // Check the condition every 10 ms.
  if (FLAGS_enable_oom_injection_write_path) {
    oomInjectorWritePath.enable();
  }

  // Step 1: Test setup and bucketColumnIndices generation
  auto bucketColumnIndices = generateBucketColumnIndices();
  auto testSetups = makeSetups(bucketColumnIndices);

  // Step 2: Create and execute write tasks
  auto tableOutputRootDir = TempDirectoryPath::create();
  std::vector<std::shared_ptr<TaskCursor>> writeTasks(
      2 * config_.evolutionCount - 1);
  RowVectorPtr finalExpectedData;

  createWriteTasks(
      testSetups,
      bucketColumnIndices,
      tableOutputRootDir->getPath(),
      writeTasks,
      finalExpectedData);

  auto executor = folly::getGlobalCPUExecutor();
  auto writeResults = runTaskCursors(writeTasks, *executor);

  // Step 3: Create scan splits from write results
  std::optional<int32_t> selectedBucket;
  if (!bucketColumnIndices.empty()) {
    selectedBucket =
        folly::Random::rand32(testSetups.back().bucketCount(), rng_);
    VLOG(1) << "selectedBucket=" << *selectedBucket;
  }

  auto [actualSplits, expectedSplits] = createScanSplitsFromWriteResults(
      writeResults,
      testSetups,
      bucketColumnIndices,
      selectedBucket,
      finalExpectedData);

  // Step 4: Setup scan tasks with filters
  auto rowType = testSetups.back().schema;
  PushdownConfig subfieldFilterConfig;
  subfieldFilterConfig.subfieldFiltersMap =
      generateSubfieldFilters(rowType, finalExpectedData);

  std::vector<std::shared_ptr<TaskCursor>> scanTasks(2);
  scanTasks[0] = makeScanTask(
      rowType, std::move(actualSplits), subfieldFilterConfig, false);
  scanTasks[1] = makeScanTask(
      rowType, std::move(expectedSplits), subfieldFilterConfig, true);

  ScopedOOMInjector oomInjectorReadPath(
      []() -> bool { return folly::Random::oneIn(10); },
      10); // Check the condition every 10 ms.
  if (FLAGS_enable_oom_injection_read_path) {
    oomInjectorReadPath.enable();
  }

  // Step 5: Execute scan tasks and verify results
  auto scanResults = runTaskCursors(scanTasks, *executor);

  // Skip result verification when OOM injection is enabled
  if (!FLAGS_enable_oom_injection_write_path &&
      !FLAGS_enable_oom_injection_read_path) {
    checkResultsEqual(scanResults[0], scanResults[1]);
  }
}

int TableEvolutionFuzzer::Setup::bucketCount() const {
  return 1 << log2BucketCount;
}

std::string TableEvolutionFuzzer::makeNewName() {
  return fmt::format("name_{}", ++sequenceNumber_);
}

TypePtr TableEvolutionFuzzer::makeNewType(int maxDepth) {
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

RowTypePtr TableEvolutionFuzzer::makeInitialSchema() {
  std::vector<std::string> names(config_.columnCount);
  std::vector<TypePtr> types(config_.columnCount);
  for (int i = 0; i < config_.columnCount; ++i) {
    names[i] = makeNewName();
    types[i] = makeNewType(3);
  }
  return ROW(std::move(names), std::move(types));
}

TypePtr TableEvolutionFuzzer::evolveType(const TypePtr& old) {
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

RowTypePtr TableEvolutionFuzzer::evolveRowType(
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

std::vector<TableEvolutionFuzzer::Setup> TableEvolutionFuzzer::makeSetups(
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
            8, setups[i - 1].log2BucketCount + folly::Random::rand32(3, rng_));
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

std::unique_ptr<TaskCursor> TableEvolutionFuzzer::makeWriteTask(
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
VectorPtr TableEvolutionFuzzer::liftToPrimitiveType(
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

std::unique_ptr<TaskCursor> TableEvolutionFuzzer::makeScanTask(
    const RowTypePtr& tableSchema,
    std::vector<Split> splits,
    const PushdownConfig& pushdownConfig,
    bool useFiltersAsNode) {
  CursorParameters params;
  params.serialExecution = true;
  // TODO: Mix in filter and aggregate pushdowns.
  params.planNode = PlanBuilder()
                        .filtersAsNode(useFiltersAsNode)
                        .tableScanWithPushDown(
                            tableSchema,
                            /*pushdownConfig=*/pushdownConfig,
                            tableSchema,
                            {})
                        .planNode();
  auto cursor = TaskCursor::create(params);
  for (auto& split : splits) {
    cursor->task()->addSplit("0", std::move(split));
  }
  cursor->task()->noMoreSplits("0");
  return cursor;
}

std::vector<column_index_t>
TableEvolutionFuzzer::generateBucketColumnIndices() {
  std::vector<column_index_t> bucketColumnIndices;
  for (int i = 0; i < config_.columnCount; ++i) {
    if (folly::Random::oneIn(2 * config_.columnCount, rng_)) {
      bucketColumnIndices.push_back(i);
    }
  }
  VLOG(1) << "bucketColumnIndices: [" << folly::join(", ", bucketColumnIndices)
          << "]";
  return bucketColumnIndices;
}

std::pair<std::vector<Split>, std::vector<Split>>
TableEvolutionFuzzer::createScanSplitsFromWriteResults(
    const std::vector<std::vector<RowVectorPtr>>& writeResults,
    const std::vector<Setup>& testSetups,
    const std::vector<column_index_t>& bucketColumnIndices,
    std::optional<int32_t> selectedBucket,
    const RowVectorPtr& finalExpectedData) {
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

  return {std::move(actualSplits), std::move(expectedSplits)};
}

void TableEvolutionFuzzer::createWriteTasks(
    const std::vector<Setup>& testSetups,
    const std::vector<column_index_t>& bucketColumnIndices,
    const std::string& tableOutputRootDirPath,
    std::vector<std::shared_ptr<TaskCursor>>& writeTasks,
    RowVectorPtr& finalExpectedData) {
  for (int i = 0; i < config_.evolutionCount; ++i) {
    auto data = vectorFuzzer_.fuzzRow(testSetups[i].schema, kVectorSize, false);
    for (auto& child : data->children()) {
      BaseVector::flattenVector(child);
    }
    auto actualDir = fmt::format("{}/actual_{}", tableOutputRootDirPath, i);
    VELOX_CHECK(std::filesystem::create_directory(actualDir));
    writeTasks[2 * i] =
        makeWriteTask(testSetups[i], data, actualDir, bucketColumnIndices);

    if (i == config_.evolutionCount - 1) {
      finalExpectedData = std::move(data);
      continue;
    }
    auto expectedDir = fmt::format("{}/expected_{}", tableOutputRootDirPath, i);
    VELOX_CHECK(std::filesystem::create_directory(expectedDir));
    auto expectedData = std::static_pointer_cast<RowVector>(
        liftToType(data, testSetups.back().schema));

    writeTasks[2 * i + 1] = makeWriteTask(
        testSetups.back(), expectedData, expectedDir, bucketColumnIndices);
  }
}

} // namespace facebook::velox::exec::test
