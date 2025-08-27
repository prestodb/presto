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
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/fuzzer/ExpressionFuzzer.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <filesystem>

#include <re2/re2.h>

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

bool hasUnsupportedMapKey(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::MAP: {
      auto mapType = type->asMap();
      // FlatMapColumnWriter only supports TINYINT, SMALLINT, INTEGER, BIGINT,
      // VARCHAR, VARBINARY as KeyType
      auto keyKind = mapType.keyType()->kind();
      if (keyKind != TypeKind::TINYINT && keyKind != TypeKind::SMALLINT &&
          keyKind != TypeKind::INTEGER && keyKind != TypeKind::BIGINT &&
          keyKind != TypeKind::VARCHAR && keyKind != TypeKind::VARBINARY) {
        return true;
      }
      return hasUnsupportedMapKey(mapType.valueType());
    }
    case TypeKind::ARRAY:
      return hasUnsupportedMapKey(type->asArray().elementType());
    case TypeKind::ROW: {
      auto& rowType = type->asRow();
      for (int i = 0; i < rowType.size(); ++i) {
        if (hasUnsupportedMapKey(rowType.childAt(i))) {
          return true;
        }
      }
      return false;
    }
    default:
      return false;
  }
}

bool hasMapColumns(const RowTypePtr& schema) {
  VLOG(1) << "Checking if schema has map columns";
  for (int i = 0; i < schema->size(); ++i) {
    if (schema->childAt(i)->isMap()) {
      return true;
    }
  }
  return false;
}

bool hasEmptyElement(const RowVectorPtr& data, int columnIndex) {
  auto mapVector = data->childAt(columnIndex)->as<MapVector>();
  if (!mapVector) {
    return true;
  }

  // Check if any map entry is empty (null or size = 0)
  auto sizes = mapVector->sizes();
  for (int j = 0; j < mapVector->size(); ++j) {
    if (mapVector->isNullAt(j) || sizes->as<vector_size_t>()[j] == 0) {
      return true; // Found an empty map
    }
  }
  return false; // No empty maps found
}

bool hasDuplicateMapKeys(const RowVectorPtr& data, int columnIndex) {
  auto mapVector = data->childAt(columnIndex)->as<MapVector>();
  if (!mapVector) {
    return false;
  }

  auto keys = mapVector->mapKeys();
  auto totalKeyCount = keys->size();

  for (int i = 0; i < totalKeyCount; ++i) {
    for (int j = i + 1; j < totalKeyCount; ++j) {
      if (keys->equalValueAt(keys.get(), i, j)) {
        return true;
      }
    }
  }

  return false;
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

fuzzer::ExpressionFuzzer::FuzzedExpressionData generateRemainingFilters(
    const TableEvolutionFuzzer::Config& config,
    unsigned currentSeed) {
  // Use ExpressionFuzzer to generate complex expressions, but use the actual
  // data types from finalExpectedData
  // Configure ExpressionFuzzer to generate simpler expressions suitable for
  // filters
  fuzzer::ExpressionFuzzer::Options options;
  options.enableComplexTypes = false; // Disable complex types to avoid issues
  options.enableDecimalType = false; // Disable decimal types
  options.maxLevelOfNesting = 3; // Reduce nesting to avoid complexity
  options.nullRatio = 0.0; // No null values to avoid type resolution issues
  // Only use simple comparison and logical functions suitable for filters
  options.useOnlyFunctions = "eq,neq,lt,lte,gt,gte,and,or,not";
  options.specialForms = "and,or"; // Only simple special forms

  // Skip complex functions that generate unparseable expressions
  options.skipFunctions = {
      "regexp_like",
      "regexp_extract",
      "replace",
      "replace_first",
      "json_format",
      "json_extract",
      "json_parse",
      "from_utf8",
      "to_utf8",
      "reverse",
      "upper",
      "lower",
      "st_coorddim",
      "is_null",
      "is_not_null"};

  auto signatureMap = getVectorFunctionSignatures();

  // Configure VectorFuzzer to avoid null values and use the actual data types
  VectorFuzzer::Options vectorFuzzerOptions;
  vectorFuzzerOptions.nullRatio = 0.0; // No nulls
  vectorFuzzerOptions.vectorSize = 100;
  auto vectorFuzzer =
      std::make_shared<VectorFuzzer>(vectorFuzzerOptions, config.pool);

  fuzzer::ExpressionFuzzer expressionFuzzer(
      signatureMap, currentSeed, vectorFuzzer, options);

  return expressionFuzzer.fuzzExpressions(1);
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

  // Step 1: Randomly decide whether to generate remaining filters (50% chance)
  bool shouldGenerateRemainingFilters = folly::Random::oneIn(2, rng_);

  fuzzer::ExpressionFuzzer::FuzzedExpressionData generatedRemainingFilters;
  std::vector<std::string> additionalColumnNames;
  std::vector<TypePtr> additionalColumnTypes;

  if (shouldGenerateRemainingFilters) {
    // Generate remaining filters and extract new columns
    generatedRemainingFilters = generateRemainingFilters(config_, currentSeed_);

    LOG(INFO) << "Generated remaining filters from expression fuzzer: "
              << generatedRemainingFilters.expressions[0]->toString();

    // Extract all columns from generatedRemainingFilters.inputType
    if (generatedRemainingFilters.inputType) {
      for (int i = 0; i < generatedRemainingFilters.inputType->size(); ++i) {
        const auto& columnName = generatedRemainingFilters.inputType->nameOf(i);
        additionalColumnNames.push_back(columnName);
        additionalColumnTypes.push_back(
            generatedRemainingFilters.inputType->childAt(i));
      }
    }

    if (!additionalColumnNames.empty()) {
      VLOG(1)
          << "Found " << additionalColumnNames.size()
          << " columns from generateRemainingFilters, will add to schema evolution";
    }
  } else {
    LOG(INFO) << "Skipping remaining filter generation (50% randomization)";
  }

  // Step 2: Test setup and bucketColumnIndices generation with additional
  // columns
  auto bucketColumnIndices = generateBucketColumnIndices();

  // Track column name mappings during evolution
  std::unordered_map<std::string, std::string> columnNameMapping;
  for (const auto& columnName : additionalColumnNames) {
    columnNameMapping[columnName] = columnName; // Initially map to itself
  }

  auto testSetups = makeSetups(
      bucketColumnIndices,
      additionalColumnNames,
      additionalColumnTypes,
      &columnNameMapping);

  // Step 3: Create and execute write tasks
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

  // Step 4: Create scan splits from write results
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

  // Step 5: Setup scan tasks with filters
  auto rowType = testSetups.back().schema;
  PushdownConfig pushownConfig;

  // Generate subfield filters first
  pushownConfig.subfieldFiltersMap =
      generateSubfieldFilters(rowType, finalExpectedData);

  // Extract field names used by subfield filters to avoid conflicts
  std::unordered_set<std::string> subfieldFilteredFields;
  for (const auto& [subfield, filter] : pushownConfig.subfieldFiltersMap) {
    auto fieldName = subfield.toString();
    LOG(INFO) << "Raw subfield: " << fieldName;
    // Extract the root field name (before any nested access)
    size_t dotPos = fieldName.find('.');
    if (dotPos != std::string::npos) {
      fieldName = fieldName.substr(0, dotPos);
    }
    subfieldFilteredFields.insert(fieldName);
    LOG(INFO) << "Subfield filter targets field: " << fieldName;
  }

  LOG(INFO) << "All subfield filtered fields:";
  for (const auto& field : subfieldFilteredFields) {
    LOG(INFO) << "  - " << field;
  }

  // Apply generated remaining filters with updated column names, avoiding
  // conflicts
  if (shouldGenerateRemainingFilters) {
    // Apply generated remaining filters
    applyRemainingFilters(
        generatedRemainingFilters,
        columnNameMapping,
        pushownConfig,
        subfieldFilteredFields);
  }

  std::vector<std::shared_ptr<TaskCursor>> scanTasks(2);
  scanTasks[0] =
      makeScanTask(rowType, std::move(actualSplits), pushownConfig, false);
  scanTasks[1] =
      makeScanTask(rowType, std::move(expectedSplits), pushownConfig, true);

  ScopedOOMInjector oomInjectorReadPath(
      []() -> bool { return folly::Random::oneIn(10); },
      10); // Check the condition every 10 ms.
  if (FLAGS_enable_oom_injection_read_path) {
    oomInjectorReadPath.enable();
  }

  // Step 6: Execute scan tasks and verify results
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

RowTypePtr TableEvolutionFuzzer::makeInitialSchema(
    const std::vector<std::string>& additionalColumnNames,
    const std::vector<TypePtr>& additionalColumnTypes) {
  std::vector<std::string> names(config_.columnCount);
  std::vector<TypePtr> types(config_.columnCount);
  for (int i = 0; i < config_.columnCount; ++i) {
    names[i] = makeNewName();
    types[i] = makeNewType(3);
  }

  // Add additional columns from generateRemainingFilters
  for (int i = 0; i < additionalColumnNames.size(); ++i) {
    names.push_back(additionalColumnNames[i]);
    types.push_back(additionalColumnTypes[i]);
    VLOG(1) << "Adding additional column to initial schema: "
            << additionalColumnNames[i] << " of type "
            << additionalColumnTypes[i]->toString();
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
      // Don't evolve DATE type to BIGINT
      if (old->isDate()) {
        return old;
      }
      return BIGINT();
    case TypeKind::REAL:
      return DOUBLE();
    default:
      return old;
  }
}

RowTypePtr TableEvolutionFuzzer::evolveRowType(
    const RowType& old,
    const std::vector<column_index_t>& bucketColumnIndices,
    std::unordered_map<std::string, std::string>* columnNameMapping) {
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
      auto oldName = names[i];
      auto newName = makeNewName();
      names[i] = newName;

      // Update column name mapping if provided
      if (columnNameMapping) {
        // Find if this column was originally from generateRemainingFilters
        for (auto& [originalName, currentName] : *columnNameMapping) {
          if (currentName == oldName) {
            currentName = newName;
            VLOG(1) << "Updated column name mapping: " << originalName << " -> "
                    << newName;
            break;
          }
        }
      }
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
    const std::vector<column_index_t>& bucketColumnIndices,
    const std::vector<std::string>& additionalColumnNames,
    const std::vector<TypePtr>& additionalColumnTypes,
    std::unordered_map<std::string, std::string>* columnNameMapping) {
  std::vector<Setup> setups(config_.evolutionCount);
  for (int i = 0; i < config_.evolutionCount; ++i) {
    if (i == 0) {
      setups[i].schema =
          makeInitialSchema(additionalColumnNames, additionalColumnTypes);
    } else {
      setups[i].schema = evolveRowType(
          *setups[i - 1].schema, bucketColumnIndices, columnNameMapping);
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

  // Create serdeParameters using proper dwrf::Config for flatmap configuration
  std::unordered_map<std::string, std::string> serdeParameters;

  if (hasMapColumns(setup.schema)) {
    // Find all top-level map column indices that support flatmap
    std::vector<uint32_t> supportedMapColumnIndices;

    for (int i = 0; i < setup.schema->size(); ++i) {
      if (setup.schema->childAt(i)->isMap()) {
        // Check if this specific map column has any empty elements
        if (hasEmptyElement(data, i)) {
          continue;
        }

        // Check if this specific map column has duplicate elements
        if (hasDuplicateMapKeys(data, i)) {
          continue;
        }

        if (!hasUnsupportedMapKey(setup.schema->childAt(i))) {
          // %50 chance to enable flatmap for this map column.
          if (folly::Random::oneIn(2)) {
            supportedMapColumnIndices.push_back(static_cast<uint32_t>(i));
          }
        }
      }
    }

    if (!supportedMapColumnIndices.empty()) {
      auto config = std::make_shared<dwrf::Config>();
      config->set(dwrf::Config::FLATTEN_MAP, true);
      config->set<const std::vector<uint32_t>>(
          dwrf::Config::MAP_FLAT_COLS, supportedMapColumnIndices);

      // Convert to serdeParameters
      auto configParams = config->toSerdeParams();
      serdeParameters.insert(configParams.begin(), configParams.end());
    } else {
      LOG(INFO)
          << "No map columns support flatmap, skipping flatmap configuration";
    }
  }

  if (bucketColumnIndices.empty()) {
    if (!serdeParameters.empty()) {
      builder.tableWrite(
          outputDir,
          /*partitionBy=*/{},
          /*bucketCount=*/0,
          /*bucketedBy=*/{},
          /*sortBy=*/{},
          setup.fileFormat,
          /*aggregates=*/{},
          /*connectorId=*/PlanBuilder::kHiveDefaultConnectorId,
          serdeParameters);
    } else {
      builder.tableWrite(outputDir, setup.fileFormat);
    }
  } else {
    std::vector<std::string> bucketColumnNames;
    bucketColumnNames.reserve(bucketColumnIndices.size());
    for (auto i : bucketColumnIndices) {
      bucketColumnNames.push_back(setup.schema->nameOf(i));
    }
    if (!serdeParameters.empty()) {
      builder.tableWrite(
          outputDir,
          /*partitionBy=*/{},
          setup.bucketCount(),
          bucketColumnNames,
          /*sortBy=*/{},
          setup.fileFormat,
          /*aggregates=*/{},
          /*connectorId=*/PlanBuilder::kHiveDefaultConnectorId,
          serdeParameters);
    } else {
      builder.tableWrite(
          outputDir,
          /*partitionBy=*/{},
          setup.bucketCount(),
          bucketColumnNames,
          /*sortBy=*/{},
          setup.fileFormat);
    }
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

void TableEvolutionFuzzer::applyRemainingFilters(
    const fuzzer::ExpressionFuzzer::FuzzedExpressionData&
        generatedRemainingFilters,
    const std::unordered_map<std::string, std::string>& columnNameMapping,
    PushdownConfig& pushownConfig,
    const std::unordered_set<std::string>& subfieldFilteredFields) {
  if (generatedRemainingFilters.expressions.empty() ||
      columnNameMapping.empty()) {
    return;
  }

  std::vector<std::string> filterStrings;
  for (const auto& expr : generatedRemainingFilters.expressions) {
    auto filterString = expr->toString();
    LOG(INFO) << "Processing remaining filter expression: " << filterString;

    // First, update column names in the filter string using columnNameMapping
    for (const auto& [originalName, currentName] : columnNameMapping) {
      // Simple string replacement - this is a basic approach
      // In a more robust implementation, we would parse the expression tree
      size_t pos = 0;
      while ((pos = filterString.find(originalName, pos)) !=
             std::string::npos) {
        // Check if this is a complete word (not part of another identifier)
        bool isCompleteWord = true;
        if (pos > 0 &&
            (std::isalnum(filterString[pos - 1]) ||
             filterString[pos - 1] == '_')) {
          isCompleteWord = false;
        }
        if (pos + originalName.length() < filterString.length() &&
            (std::isalnum(filterString[pos + originalName.length()]) ||
             filterString[pos + originalName.length()] == '_')) {
          isCompleteWord = false;
        }

        if (isCompleteWord) {
          filterString.replace(pos, originalName.length(), currentName);
          pos += currentName.length();
        } else {
          pos += originalName.length();
        }
      }
    }

    LOG(INFO) << "After column name mapping: " << filterString;

    // Now check if this filter expression conflicts with subfield filters
    bool hasConflict = false;
    for (const auto& subfieldField : subfieldFilteredFields) {
      // Check if the filter string contains references to fields that are
      // already filtered by subfield filters
      size_t pos = 0;
      while ((pos = filterString.find(subfieldField, pos)) !=
             std::string::npos) {
        // Check if this is a complete word (not part of another identifier)
        bool isCompleteWord = true;
        if (pos > 0 &&
            (std::isalnum(filterString[pos - 1]) ||
             filterString[pos - 1] == '_')) {
          isCompleteWord = false;
        }
        if (pos + subfieldField.length() < filterString.length() &&
            (std::isalnum(filterString[pos + subfieldField.length()]) ||
             filterString[pos + subfieldField.length()] == '_')) {
          isCompleteWord = false;
        }

        if (isCompleteWord) {
          hasConflict = true;
          LOG(INFO)
              << "CONFLICT DETECTED! Skipping remaining filter due to conflict with subfield filter on field: "
              << subfieldField << ", filter: " << filterString;
          break;
        }
        pos += subfieldField.length();
      }
      if (hasConflict) {
        break;
      }
    }

    // Skip this filter if it conflicts with subfield filters
    if (hasConflict) {
      LOG(INFO) << "Skipping filter due to conflict: " << filterString;
      continue;
    }

    LOG(INFO) << "No conflict detected, proceeding with filter: "
              << filterString;

    // Fix DATE literal format: convert bare date to DATE literal format
    // to prevent DuckDB parser from interpreting it as arithmetic expression
    RE2 datePattern(R"(\b(\d{4}-\d{2}-\d{2})\b)");
    RE2::GlobalReplace(&filterString, datePattern, "DATE '\\1'");

    filterStrings.push_back(filterString);
    VLOG(1) << "Updated filter expression: " << filterString;
  }

  if (filterStrings.size() == 1) {
    pushownConfig.remainingFilter = filterStrings[0];
  } else if (filterStrings.size() > 1) {
    pushownConfig.remainingFilter =
        "(" + folly::join(") AND (", filterStrings) + ")";
  }
}

} // namespace facebook::velox::exec::test
