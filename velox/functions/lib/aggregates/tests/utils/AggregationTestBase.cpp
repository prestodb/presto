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

#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/AggregateCompanionSignatures.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Spill.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/expression/Expr.h"
#include "velox/expression/SignatureBinder.h"

using facebook::velox::exec::Spiller;
using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::CursorParameters;
using facebook::velox::exec::test::PlanBuilder;
using facebook::velox::test::VectorMaker;

namespace facebook::velox::functions::aggregate::test {

namespace {
constexpr const char* kHiveConnectorId = "test-hive";

void enableAbandonPartialAggregation(AssertQueryBuilder& queryBuilder) {
  queryBuilder.config(core::QueryConfig::kAbandonPartialAggregationMinRows, 1)
      .config(core::QueryConfig::kAbandonPartialAggregationMinPct, 0)
      .config(core::QueryConfig::kMaxPartialAggregationMemory, 0)
      .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, 0)
      .maxDrivers(1);
}

} // namespace

std::vector<RowVectorPtr> AggregationTestBase::makeVectors(
    const RowTypePtr& rowType,
    vector_size_t size,
    int numVectors) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, size, *pool_));
    vectors.push_back(vector);
  }
  return vectors;
}

void AggregationTestBase::SetUp() {
  OperatorTestBase::SetUp();
  filesystems::registerLocalFileSystem();
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, std::make_shared<core::MemConfig>());
  connector::registerConnector(hiveConnector);
}

void AggregationTestBase::TearDown() {
  connector::unregisterConnector(kHiveConnectorId);
  OperatorTestBase::TearDown();
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql,
    const std::unordered_map<std::string, std::string>& config) {
  SCOPED_TRACE(duckDbSql);
  testAggregations(data, groupingKeys, aggregates, {}, duckDbSql, config);
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<RowVectorPtr>& expectedResult,
    const std::unordered_map<std::string, std::string>& config) {
  testAggregations(data, groupingKeys, aggregates, {}, expectedResult, config);
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::string& duckDbSql,
    const std::unordered_map<std::string, std::string>& config) {
  SCOPED_TRACE(duckDbSql);
  testAggregations(
      [&](PlanBuilder& builder) { builder.values(data); },
      groupingKeys,
      aggregates,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(duckDbSql); },
      config);
}

namespace {
// Returns total spilled bytes inside 'task'.
int64_t spilledBytes(const exec::Task& task) {
  auto stats = task.taskStats();
  int64_t spilledBytes = 0;
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledBytes += op.spilledBytes;
    }
  }
  return spilledBytes;
}

// Returns total spilled rows inside 'task'.
int64_t spilledRows(const exec::Task& task) {
  auto stats = task.taskStats();
  int64_t spilledRows = 0;
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledRows += op.spilledRows;
    }
  }
  return spilledRows;
}

// Returns total spilled input bytes inside 'task'.
int64_t spilledInputBytes(const exec::Task& task) {
  auto stats = task.taskStats();
  int64_t spilledInputBytes = 0;
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledInputBytes += op.spilledInputBytes;
    }
  }
  return spilledInputBytes;
}

// Returns total spilled files inside 'task'.
int64_t spilledFiles(const exec::Task& task) {
  auto stats = task.taskStats();
  int64_t spilledFiles = 0;
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledFiles += op.spilledFiles;
    }
  }
  return spilledFiles;
}

// Add a BIGINT column of 4 distinct values to data.
RowVectorPtr addExtraGroupingKeyImpl(
    const RowVectorPtr& data,
    const std::vector<std::string>& fieldNames,
    VectorMaker& vectorMaker) {
  auto children = data->children();
  children.push_back(vectorMaker.flatVector<int64_t>(
      data->size(), [](auto row) { return row % 4; }));
  return vectorMaker.rowVector(fieldNames, children);
}

// Add a BIGINT column of 4 distinct values to every RowVector in data.
std::vector<RowVectorPtr> addExtraGroupingKey(
    const std::vector<RowVectorPtr>& data,
    const std::string& extraKeyName) {
  VELOX_CHECK(!data.empty());
  auto names = asRowType(data[0]->type())->names();
  names.push_back(extraKeyName);

  std::vector<RowVectorPtr> newData{data.size()};
  VectorMaker vectorMaker{data[0]->pool()};
  std::transform(
      data.begin(),
      data.end(),
      newData.begin(),
      [&names, &vectorMaker](const RowVectorPtr& vector) {
        return addExtraGroupingKeyImpl(vector, names, vectorMaker);
      });
  return newData;
}

// Given the name of the original aggregation function and the argument types,
// return the name of the extract companion function with a suffix of the result
// type. Extract function names with suffixes should only be used when the
// original aggregation function has multiple signatures with the same
// intermediate type.
std::string getExtractFunctionNameWithSuffix(
    const std::string& name,
    const std::vector<TypePtr>& argTypes) {
  auto signatures = exec::getAggregateFunctionSignatures(name);
  VELOX_CHECK(signatures.has_value());

  for (const auto& signature : signatures.value()) {
    exec::SignatureBinder binder{*signature, argTypes};
    if (binder.tryBind()) {
      if (auto resultType = binder.tryResolveReturnType()) {
        return exec::CompanionSignatures::extractFunctionNameWithSuffix(
            name, signature->returnType());
      }
    }
  }
  VELOX_UNREACHABLE("Could not find a function bound to argTypes.");
}

// Given the `index`-th aggregation expression, e.g., "avg(c0)", return three
// strings of its partial companion aggregation expression, merge companion
// aggregation expression, and extract companion expression, e.g.,
// {"avg_partial(c0)", "avg_merge(a0)", "avg_extract_double(a0)"} when `index`
// is 0.
std::tuple<std::string, std::string, std::string> getCompanionAggregates(
    column_index_t index,
    const exec::CompanionFunctionSignatureMap& companionFunctions,
    const std::string& functionName,
    const std::string& aggregateArgs,
    const std::vector<TypePtr>& argTypes) {
  VELOX_CHECK_EQ(companionFunctions.partial.size(), 1);
  auto partialAggregate = fmt::format(
      "{}({})", companionFunctions.partial.front().functionName, aggregateArgs);

  VELOX_CHECK_EQ(companionFunctions.merge.size(), 1);
  auto mergeAggregate = fmt::format(
      "{}(a{})", companionFunctions.merge.front().functionName, index);

  // Construct the extract expression. Rename the result of the extract
  // expression to be the same as the original aggregation result, so that
  // post-aggregation proejctions, if exist, can apply with no change.
  std::string extractExpression;
  if (companionFunctions.extract.size() == 1) {
    extractExpression = fmt::format(
        "{0}(a{1}) as a{1}",
        companionFunctions.extract.front().functionName,
        index);
  } else {
    VELOX_CHECK_GT(companionFunctions.extract.size(), 1);
    auto extractFunctionName =
        getExtractFunctionNameWithSuffix(functionName, argTypes);
    extractExpression =
        fmt::format("{0}(a{1}) as a{1}", extractFunctionName, index);
  }
  return std::make_tuple(partialAggregate, mergeAggregate, extractExpression);
}

// Given a list of aggregation expressions, e.g., {"avg(c0)"}, return a list of
// aggregate function names and a list of arguments in these expressions, e.g.,
// {{"avg"}, {"c0"}}.
std::tuple<std::vector<std::string>, std::vector<std::string>>
getFunctionNamesAndArgs(const std::vector<std::string>& aggregates) {
  std::vector<std::string> functionNames;
  std::vector<std::string> aggregateArgs;
  for (const auto& aggregate : aggregates) {
    std::vector<std::string> tokens;
    folly::split('(', aggregate, tokens);
    VELOX_CHECK_EQ(tokens.size(), 2);
    functionNames.push_back(tokens[0]);
    aggregateArgs.push_back(tokens[1].substr(0, tokens[1].find(')')));
  }
  return std::make_pair(functionNames, aggregateArgs);
}
} // namespace

void AggregationTestBase::testAggregationsWithCompanion(
    const std::vector<RowVectorPtr>& data,
    const std::function<void(PlanBuilder&)>& preAggregationProcessing,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::vector<TypePtr>>& aggregatesArgTypes,
    const std::vector<std::string>& postAggregationProjections,
    const std::string& duckDbSql,
    const std::unordered_map<std::string, std::string>& config) {
  testAggregationsWithCompanion(
      data,
      preAggregationProcessing,
      groupingKeys,
      aggregates,
      aggregatesArgTypes,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(duckDbSql); },
      config);
}

void AggregationTestBase::testAggregationsWithCompanion(
    const std::vector<RowVectorPtr>& data,
    const std::function<void(PlanBuilder&)>& preAggregationProcessing,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::vector<TypePtr>>& aggregatesArgTypes,
    const std::vector<std::string>& postAggregationProjections,
    const std::vector<RowVectorPtr>& expectedResult,
    const std::unordered_map<std::string, std::string>& config) {
  testAggregationsWithCompanion(
      data,
      preAggregationProcessing,
      groupingKeys,
      aggregates,
      aggregatesArgTypes,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(expectedResult); },
      config);
}

void AggregationTestBase::testAggregationsWithCompanion(
    const std::vector<RowVectorPtr>& data,
    const std::function<void(PlanBuilder&)>& preAggregationProcessing,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::vector<TypePtr>>& aggregatesArgTypes,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(exec::test::AssertQueryBuilder&)>
        assertResults,
    const std::unordered_map<std::string, std::string>& config) {
  auto dataWithExtraGroupingKey = addExtraGroupingKey(data, "k0");
  auto groupingKeysWithPartialKey = groupingKeys;
  groupingKeysWithPartialKey.push_back("k0");

  std::vector<std::string> paritialAggregates;
  std::vector<std::string> mergeAggregates;
  std::vector<std::string> extractExpressions;
  extractExpressions.insert(
      extractExpressions.end(), groupingKeys.begin(), groupingKeys.end());

  auto [functionNames, aggregateArgs] = getFunctionNamesAndArgs(aggregates);
  for (size_t i = 0, size = functionNames.size(); i < size; ++i) {
    const auto& companionSignatures =
        exec::getCompanionFunctionSignatures(functionNames[i]);
    VELOX_CHECK(companionSignatures.has_value());

    const auto& [paritialAggregate, mergeAggregate, extractAggregate] =
        getCompanionAggregates(
            i,
            *companionSignatures,
            functionNames[i],
            aggregateArgs[i],
            aggregatesArgTypes[i]);
    paritialAggregates.push_back(paritialAggregate);
    mergeAggregates.push_back(mergeAggregate);
    extractExpressions.push_back(extractAggregate);
  }

  {
    SCOPED_TRACE("Run partial + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtraGroupingKey);
    preAggregationProcessing(builder);
    builder.partialAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .finalAggregation()
        .partialAggregation(groupingKeys, mergeAggregates)
        .finalAggregation()
        .project(extractExpressions);

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty() && allowInputShuffle_) {
    SCOPED_TRACE("Run partial + final with spilling");
    PlanBuilder builder(pool());
    builder.values(dataWithExtraGroupingKey);
    preAggregationProcessing(builder);

    // Spilling needs at least 2 batches of input. Use round-robin
    // repartitioning to split input into multiple batches.
    core::PlanNodeId partialNodeId;
    builder.localPartitionRoundRobinRow()
        .partialAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .capturePlanNodeId(partialNodeId)
        .localPartition(groupingKeysWithPartialKey)
        .finalAggregation()
        .partialAggregation(groupingKeys, mergeAggregates)
        .capturePlanNodeId(partialNodeId)
        .localPartition(groupingKeys)
        .finalAggregation()
        .project(extractExpressions);

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    auto spillDirectory = exec::test::TempDirectoryPath::create();

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config)
        .config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kAggregationSpillEnabled, true)
        .spillDirectory(spillDirectory->path)
        .maxDrivers(4);

    exec::TestScopedSpillInjection scopedSpillInjection(100);
    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    const auto inputRows =
        toPlanStats(task->taskStats()).at(partialNodeId).inputRows;
    if (inputRows > 1) {
      EXPECT_LT(0, spilledBytes(*task))
          << "inputRows: " << inputRows
          << " spilledRows: " << spilledRows(*task)
          << " spilledInputBytes: " << spilledInputBytes(*task)
          << " spilledFiles: " << spilledFiles(*task)
          << " injectedSpills: " << exec::injectedSpillCount();
    } else {
      EXPECT_EQ(0, spilledBytes(*task))
          << "inputRows: " << inputRows
          << " spilledRows: " << spilledRows(*task)
          << " spilledInputBytes: " << spilledInputBytes(*task)
          << " spilledFiles: " << spilledFiles(*task)
          << " injectedSpills: " << exec::injectedSpillCount();
    }
  }

  {
    SCOPED_TRACE("Run single");
    PlanBuilder builder(pool());
    builder.values(dataWithExtraGroupingKey);
    preAggregationProcessing(builder);
    builder.singleAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .singleAggregation(groupingKeys, mergeAggregates)
        .project(extractExpressions);

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  {
    SCOPED_TRACE("Run partial + intermediate + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtraGroupingKey);
    preAggregationProcessing(builder);
    builder.partialAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .intermediateAggregation()
        .finalAggregation()
        .partialAggregation(groupingKeys, mergeAggregates)
        .intermediateAggregation()
        .finalAggregation()
        .project(extractExpressions);

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty()) {
    SCOPED_TRACE("Run partial + local exchange + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtraGroupingKey);
    preAggregationProcessing(builder);
    builder.partialAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .localPartition(groupingKeysWithPartialKey)
        .finalAggregation()
        .partialAggregation(groupingKeys, mergeAggregates)
        .localPartition(groupingKeys)
        .finalAggregation()
        .project(extractExpressions);

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config).maxDrivers(4);
    assertResults(queryBuilder);
  }

  {
    SCOPED_TRACE(
        "Run partial + local exchange + intermediate + local exchange + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtraGroupingKey);
    preAggregationProcessing(builder);
    builder.partialAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .localPartition(groupingKeysWithPartialKey)
        .intermediateAggregation()
        .localPartition(groupingKeysWithPartialKey)
        .finalAggregation()
        .partialAggregation(groupingKeys, mergeAggregates);

    if (groupingKeys.empty()) {
      builder.localPartitionRoundRobinRow();
    } else {
      builder.localPartition(groupingKeys);
    }

    builder.intermediateAggregation()
        .localPartition(groupingKeys)
        .finalAggregation()
        .project(extractExpressions);

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config).maxDrivers(2);
    assertResults(queryBuilder);
  }

  if (testStreaming_ && groupingKeys.empty() &&
      postAggregationProjections.empty()) {
    {
      SCOPED_TRACE("Streaming partial");
      auto partialResult = validateStreamingInTestAggregations(
          [&](auto& builder) { builder.values(dataWithExtraGroupingKey); },
          paritialAggregates,
          config);

      validateStreamingInTestAggregations(
          [&](auto& builder) { builder.values({partialResult}); },
          mergeAggregates,
          config);
    }
  }
}

namespace {

void writeToFile(
    const std::string& path,
    const VectorPtr& vector,
    memory::MemoryPool* pool) {
  dwrf::WriterOptions options;
  options.schema = vector->type();
  options.memoryPool = pool;
  auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  dwrf::Writer writer(std::move(sink), options);
  writer.write(vector);
  writer.close();
}

template <typename T>
class ScopedChange {
 public:
  ScopedChange(T* value, const T& newValue) : value_(value) {
    oldValue_ = *value_;
    *value_ = newValue;
  }

  ~ScopedChange() {
    *value_ = oldValue_;
  }

 private:
  T oldValue_;
  T* value_;
};

bool isTableScanSupported(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW && type->size() == 0) {
    return false;
  }
  if (type->kind() == TypeKind::UNKNOWN) {
    return false;
  }
  if (type->kind() == TypeKind::HUGEINT) {
    return false;
  }
  // Disable testing with TableScan when input contains TIMESTAMP type, due to
  // the issue #8127.
  if (type->kind() == TypeKind::TIMESTAMP) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isTableScanSupported(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

} // namespace

void AggregationTestBase::testReadFromFiles(
    std::function<void(exec::test::PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(exec::test::AssertQueryBuilder&)>
        assertResults,
    const std::unordered_map<std::string, std::string>& config) {
  PlanBuilder builder(pool());
  makeSource(builder);

  if (!isTableScanSupported(builder.planNode()->outputType())) {
    return;
  }

  auto input = AssertQueryBuilder(builder.planNode()).copyResults(pool());
  if (input->size() == 0) {
    return;
  }

  std::vector<std::shared_ptr<exec::test::TempFilePath>> files;
  std::vector<exec::Split> splits;
  std::vector<VectorPtr> inputs;
  auto writerPool = rootPool_->addAggregateChild("AggregationTestBase.writer");

  // Splits and writes the input vectors into two files, to some extent,
  // involves shuffling of the inputs. So only split input if allowInputShuffle_
  // is true. Otherwise, only write into a single file.
  if (allowInputShuffle_ && input->size() >= 2) {
    auto size1 = input->size() / 2;
    auto size2 = input->size() - size1;
    auto input1 = input->slice(0, size1);
    auto input2 = input->slice(size1, size2);
    inputs = {input1, input2};
  } else {
    inputs.push_back(input);
  }

  for (auto& vector : inputs) {
    auto file = exec::test::TempFilePath::create();
    writeToFile(file->path, vector, writerPool.get());
    files.push_back(file);
    splits.emplace_back(std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId, file->path, dwio::common::FileFormat::DWRF));
  }
  // No need to test streaming as the streaming test generates its own inputs,
  // so it would be the same as the original test.
  {
    ScopedChange<bool> disableTestStreaming(&testStreaming_, false);
    testAggregationsImpl(
        [&](auto& builder) { builder.tableScan(asRowType(input->type())); },
        groupingKeys,
        aggregates,
        postAggregationProjections,
        [&](auto& builder) { return assertResults(builder.splits(splits)); },
        config);
  }

  for (const auto& file : files) {
    remove(file->path.c_str());
  }
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::vector<RowVectorPtr>& expectedResult,
    const std::unordered_map<std::string, std::string>& config) {
  testAggregations(
      [&](PlanBuilder& builder) { builder.values(data); },
      groupingKeys,
      aggregates,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(expectedResult); },
      config);
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql,
    const std::unordered_map<std::string, std::string>& config) {
  testAggregations(
      makeSource,
      groupingKeys,
      aggregates,
      {},
      [&](auto& builder) { return builder.assertResults(duckDbSql); },
      config);
}

namespace {

std::vector<VectorPtr> extractArgColumns(
    const core::CallTypedExprPtr& aggregateExpr,
    const RowVectorPtr& input,
    memory::MemoryPool* pool) {
  auto type = input->type()->asRow();
  std::vector<VectorPtr> columns;
  for (const auto& arg : aggregateExpr->inputs()) {
    if (auto field = core::TypedExprs::asFieldAccess(arg)) {
      columns.push_back(input->childAt(field->name()));
    }
    if (core::TypedExprs::isConstant(arg)) {
      auto constant = core::TypedExprs::asConstant(arg);
      columns.push_back(constant->toConstantVector(pool));
    }
    if (auto lambda = core::TypedExprs::asLambda(arg)) {
      for (const auto& name : lambda->signature()->names()) {
        if (auto captureIndex = type.getChildIdxIfExists(name)) {
          columns.push_back(input->childAt(captureIndex.value()));
        }
      }
    }
  }
  return columns;
}

} // namespace

RowVectorPtr AggregationTestBase::validateStreamingInTestAggregations(
    const std::function<void(PlanBuilder&)>& makeSource,
    const std::vector<std::string>& aggregates,
    const std::unordered_map<std::string, std::string>& config) {
  PlanBuilder builder(pool());
  makeSource(builder);
  auto input = AssertQueryBuilder(builder.planNode())
                   .configs(config)
                   .copyResults(pool());
  if (input->size() < 2) {
    return nullptr;
  }
  auto size1 = input->size() / 2;
  auto size2 = input->size() - size1;
  builder.singleAggregation({}, aggregates);
  auto expected = AssertQueryBuilder(builder.planNode())
                      .configs(config)
                      .copyResults(pool());
  EXPECT_EQ(expected->size(), 1);
  auto& aggregationNode =
      static_cast<const core::AggregationNode&>(*builder.planNode());
  EXPECT_EQ(expected->childrenSize(), aggregationNode.aggregates().size());
  for (int i = 0; i < aggregationNode.aggregates().size(); ++i) {
    const auto& aggregate = aggregationNode.aggregates()[i];
    if (aggregate.distinct || !aggregate.sortingKeys.empty() ||
        aggregate.mask != nullptr) {
      // TODO Add support for all these cases.
      return nullptr;
    }

    const auto& aggregateExpr = aggregate.call;
    const auto& name = aggregateExpr->name();

    SCOPED_TRACE(name);
    std::vector<VectorPtr> rawInput1, rawInput2;
    for (const auto& arg : aggregateExpr->inputs()) {
      VectorPtr column;
      auto channel = exec::exprToChannel(arg.get(), input->type());
      if (channel == kConstantChannel) {
        auto constant = dynamic_cast<const core::ConstantTypedExpr*>(arg.get());
        column = constant->toConstantVector(pool());
      } else {
        column = input->childAt(channel);
      }
      rawInput1.push_back(column->slice(0, size1));
      rawInput2.push_back(column->slice(size1, size2));
    }

    auto actualResult1 =
        testStreaming(name, true, rawInput1, size1, rawInput2, size2, config);
    velox::exec::test::assertEqualResults(
        {makeRowVector({expected->childAt(i)})},
        {makeRowVector({actualResult1})});
    auto actualResult2 =
        testStreaming(name, false, rawInput1, size1, rawInput2, size2, config);
    velox::exec::test::assertEqualResults(
        {makeRowVector({expected->childAt(i)})},
        {makeRowVector({actualResult2})});
  }
  return expected;
}

void AggregationTestBase::testAggregationsImpl(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(AssertQueryBuilder&)>
        assertResults,
    const std::unordered_map<std::string, std::string>& config) {
  {
    SCOPED_TRACE("Run partial + final");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.partialAggregation(groupingKeys, aggregates).finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty() && allowInputShuffle_) {
    SCOPED_TRACE("Run partial + final with spilling");
    PlanBuilder builder(pool());
    makeSource(builder);

    // Spilling needs at least 2 batches of input. Use round-robin
    // repartitioning to split input into multiple batches.
    core::PlanNodeId partialNodeId;
    builder.localPartitionRoundRobinRow()
        .partialAggregation(groupingKeys, aggregates)
        .capturePlanNodeId(partialNodeId)
        .localPartition(groupingKeys)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    auto spillDirectory = exec::test::TempDirectoryPath::create();

    ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
    const auto peakSpillMemoryUsage =
        memory::spillMemoryPool()->stats().peakBytes;
    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config)
        .config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kAggregationSpillEnabled, true)
        .spillDirectory(spillDirectory->path)
        .maxDrivers(4);

    exec::TestScopedSpillInjection scopedSpillInjection(100);
    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    auto inputRows = toPlanStats(task->taskStats()).at(partialNodeId).inputRows;
    if (inputRows > 1) {
      EXPECT_LT(0, spilledBytes(*task))
          << "inputRows: " << inputRows
          << " spilledRows: " << spilledRows(*task)
          << " spilledInputBytes: " << spilledInputBytes(*task)
          << " spilledFiles: " << spilledFiles(*task)
          << " injectedSpills: " << exec::injectedSpillCount();
      ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
      ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
      ASSERT_GE(
          memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
    } else {
      EXPECT_EQ(0, spilledBytes(*task))
          << "inputRows: " << inputRows
          << " spilledRows: " << spilledRows(*task)
          << " spilledInputBytes: " << spilledInputBytes(*task)
          << " spilledFiles: " << spilledFiles(*task)
          << " injectedSpills: " << exec::injectedSpillCount();
    }
  }

  if (!groupingKeys.empty()) {
    SCOPED_TRACE("Run partial + final with abandon partial agg");
    PlanBuilder builder(pool());
    makeSource(builder);

    core::PlanNodeId partialNodeId;
    core::PlanNodeId intermediateNodeId;
    builder.partialAggregation(groupingKeys, aggregates)
        .capturePlanNodeId(partialNodeId)
        .intermediateAggregation()
        .capturePlanNodeId(intermediateNodeId)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    enableAbandonPartialAggregation(queryBuilder);

    auto task = assertResults(queryBuilder);

    // Expect partial aggregation was turned off if there were more than 1 input
    // batches.
    auto taskStats = toPlanStats(task->taskStats());
    auto inputVectors = taskStats.at(partialNodeId).inputVectors;
    auto partialStats = taskStats.at(partialNodeId).customStats;
    auto intermediateStats = taskStats.at(intermediateNodeId).customStats;
    if (inputVectors > 1) {
      EXPECT_LT(0, partialStats.at("abandonedPartialAggregation").count);
      EXPECT_LT(0, intermediateStats.at("abandonedPartialAggregation").count);
    }
  }

  {
    SCOPED_TRACE("Run single");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.singleAggregation(groupingKeys, aggregates);
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  // TODO: turn on this after spilling for VarianceAggregationTest pass.
#if 0
  if (!groupingKeys.empty() && allowInputShuffle_) {
    SCOPED_TRACE("Run single with spilling");
    PlanBuilder builder(pool());
    makeSource(builder);
    core::PlanNodeId aggregationNodeId;
    builder.singleAggregation(groupingKeys, aggregates)
        .capturePlanNodeId(aggregationNodeId);
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    auto spillDirectory = exec::test::TempDirectoryPath::create();

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config)
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(core::QueryConfig::kAggregationSpillEnabled, "true")
        .spillDirectory(spillDirectory->path);

    TestScopedSpillInjection scopedSpillInjection(100);
    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    auto inputRows =
        toPlanStats(task->taskStats()).at(aggregationNodeId).inputRows;
    if (inputRows > 1) {
      EXPECT_LT(0, spilledBytes(*task));
    } else {
      EXPECT_EQ(0, spilledBytes(*task));
    }
  }
#endif

  {
    SCOPED_TRACE("Run partial + intermediate + final");
    PlanBuilder builder(pool());
    makeSource(builder);

    builder.partialAggregation(groupingKeys, aggregates)
        .intermediateAggregation()
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty()) {
    SCOPED_TRACE("Run partial + local exchange + final");
    PlanBuilder builder(pool());
    makeSource(builder);

    builder.partialAggregation(groupingKeys, aggregates)
        .localPartition(groupingKeys)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder.maxDrivers(4));
  }

  {
    SCOPED_TRACE(
        "Run partial + local exchange + intermediate + local exchange + final");
    PlanBuilder builder(pool());
    makeSource(builder);

    builder.partialAggregation(groupingKeys, aggregates);

    if (groupingKeys.empty()) {
      builder.localPartitionRoundRobinRow();
    } else {
      builder.localPartition(groupingKeys);
    }

    builder.intermediateAggregation()
        .localPartition(groupingKeys)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder.maxDrivers(2));
  }

  if (testStreaming_ && groupingKeys.empty() &&
      postAggregationProjections.empty()) {
    SCOPED_TRACE("Streaming");
    validateStreamingInTestAggregations(makeSource, aggregates, config);
  }
}

void AggregationTestBase::testStreamingAggregationsImpl(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(AssertQueryBuilder&)>
        assertResults,
    const std::unordered_map<std::string, std::string>& config) {
  {
    SCOPED_TRACE("Test StreamingAggregation: run single");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.orderBy(groupingKeys, false)
        .streamingAggregation(
            groupingKeys,
            aggregates,
            {},
            core::AggregationNode::Step::kSingle,
            false);
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  {
    SCOPED_TRACE(
        "Test StreamingAggregation: run partial + final StreamingAggregation");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.orderBy(groupingKeys, false)
        .partialStreamingAggregation(groupingKeys, aggregates)
        .finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }

  {
    SCOPED_TRACE(
        "Test StreamingAggregation: run partial + intermediate + final");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.orderBy(groupingKeys, false)
        .partialStreamingAggregation(groupingKeys, aggregates)
        .intermediateAggregation()
        .finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.configs(config);
    assertResults(queryBuilder);
  }
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(AssertQueryBuilder&)>
        assertResults,
    const std::unordered_map<std::string, std::string>& config) {
  testAggregationsImpl(
      makeSource,
      groupingKeys,
      aggregates,
      postAggregationProjections,
      assertResults,
      config);

  if (testIncremental_) {
    SCOPED_TRACE("testIncrementalAggregation");
    testIncrementalAggregation(makeSource, aggregates, config);
  }

  if (allowInputShuffle_ && !groupingKeys.empty()) {
    testStreamingAggregationsImpl(
        makeSource,
        groupingKeys,
        aggregates,
        postAggregationProjections,
        assertResults,
        config);
  }

  {
    SCOPED_TRACE("Test reading input from table scan");
    testReadFromFiles(
        makeSource,
        groupingKeys,
        aggregates,
        postAggregationProjections,
        assertResults,
        config);
  }
}

namespace {
std::pair<TypePtr, TypePtr> getResultTypes(
    const std::string& name,
    const std::vector<TypePtr>& rawInputTypes) {
  auto signatures = exec::getAggregateFunctionSignatures(name);
  if (!signatures.has_value()) {
    VELOX_FAIL("Aggregate {} not registered", name);
  }
  for (auto& signature : signatures.value()) {
    exec::SignatureBinder binder(*signature, rawInputTypes);
    if (binder.tryBind()) {
      auto intermediateType =
          binder.tryResolveType(signature->intermediateType());
      VELOX_CHECK(
          intermediateType, "failed to resolve intermediate type for {}", name);
      auto finalType = binder.tryResolveType(signature->returnType());
      VELOX_CHECK(finalType, "failed to resolve final type for {}", name);
      return {intermediateType, finalType};
    }
  }
  VELOX_FAIL("Could not infer intermediate type for aggregate {}", name);
}
} // namespace

VectorPtr AggregationTestBase::testStreaming(
    const std::string& functionName,
    bool testGlobal,
    const std::vector<VectorPtr>& rawInput1,
    const std::vector<VectorPtr>& rawInput2,
    const std::unordered_map<std::string, std::string>& config) {
  VELOX_CHECK(!rawInput1.empty());
  VELOX_CHECK(!rawInput2.empty());
  return testStreaming(
      functionName,
      testGlobal,
      rawInput1,
      rawInput1[0]->size(),
      rawInput2,
      rawInput2[0]->size(),
      config);
}

namespace {

constexpr int kRowSizeOffset = 8;
constexpr int kOffset = kRowSizeOffset + 8;

std::unique_ptr<exec::Aggregate> createAggregateFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& inputTypes,
    HashStringAllocator& allocator,
    const std::unordered_map<std::string, std::string>& config) {
  auto [intermediateType, finalType] = getResultTypes(functionName, inputTypes);
  core::QueryConfig queryConfig({config});
  auto func = exec::Aggregate::create(
      functionName,
      core::AggregationNode::Step::kSingle,
      inputTypes,
      finalType,
      queryConfig);
  func->setAllocator(&allocator);
  func->setOffsets(kOffset, 0, 1, kRowSizeOffset);

  VELOX_CHECK(intermediateType->equivalent(
      *func->intermediateType(functionName, inputTypes)));
  VELOX_CHECK(finalType->equivalent(*func->resultType()));

  return func;
}

} // namespace

void AggregationTestBase::testIncrementalAggregation(
    const std::function<void(exec::test::PlanBuilder&)>& makeSource,
    const std::vector<std::string>& aggregates,
    const std::unordered_map<std::string, std::string>& config) {
  PlanBuilder builder(pool());
  makeSource(builder);
  auto data = AssertQueryBuilder(builder.planNode())
                  .configs(config)
                  .copyResults(pool());
  auto inputSize = data->size();
  if (inputSize == 0) {
    return;
  }

  auto& aggregationNode = static_cast<const core::AggregationNode&>(
      *builder.singleAggregation({}, aggregates).planNode());
  for (int i = 0; i < aggregationNode.aggregates().size(); ++i) {
    const auto& aggregate = aggregationNode.aggregates()[i];
    const auto& aggregateExpr = aggregate.call;
    const auto& functionName = aggregateExpr->name();
    auto input = extractArgColumns(aggregateExpr, data, pool());

    HashStringAllocator allocator(pool());
    std::vector<core::LambdaTypedExprPtr> lambdas;
    for (const auto& arg : aggregate.call->inputs()) {
      if (auto lambda =
              std::dynamic_pointer_cast<const core::LambdaTypedExpr>(arg)) {
        lambdas.push_back(lambda);
      }
    }
    auto queryCtxConfig = config;
    auto func = createAggregateFunction(
        functionName, aggregate.rawInputTypes, allocator, config);
    auto queryCtx = std::make_shared<core::QueryCtx>(
        nullptr, core::QueryConfig{queryCtxConfig});

    std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator;
    if (!lambdas.empty()) {
      expressionEvaluator = std::make_shared<exec::SimpleExpressionEvaluator>(
          queryCtx.get(), allocator.pool());
      func->setLambdaExpressions(lambdas, expressionEvaluator);
    }

    std::vector<char> group(kOffset + func->accumulatorFixedWidthSize());
    std::vector<char*> groups(inputSize, group.data());
    std::vector<vector_size_t> indices(1, 0);
    func->initializeNewGroups(groups.data(), indices);
    func->addSingleGroupRawInput(
        group.data(), SelectivityVector(inputSize), input, false);

    // Extract intermediate result from the same accumulator twice and expect
    // results to be the same.
    auto intermediateType =
        func->intermediateType(functionName, aggregate.rawInputTypes);
    auto intermediateResult1 = BaseVector::create(intermediateType, 1, pool());
    auto intermediateResult2 = BaseVector::create(intermediateType, 1, pool());
    func->extractAccumulators(groups.data(), 1, &intermediateResult1);
    func->extractAccumulators(groups.data(), 1, &intermediateResult2);
    velox::test::assertEqualVectors(intermediateResult1, intermediateResult2);

    // Extract values from the same accumulator twice and expect results to be
    // the same.
    auto result1 = BaseVector::create(func->resultType(), 1, pool());
    auto result2 = BaseVector::create(func->resultType(), 1, pool());
    func->extractValues(groups.data(), 1, &result1);
    func->extractValues(groups.data(), 1, &result2);

    // Destroy accumulators to avoid memory leak.
    if (func->accumulatorUsesExternalMemory()) {
      func->destroy(folly::Range(groups.data(), 1));
    }

    velox::test::assertEqualVectors(result1, result2);
  }
}

VectorPtr AggregationTestBase::testStreaming(
    const std::string& functionName,
    bool testGlobal,
    const std::vector<VectorPtr>& rawInput1,
    vector_size_t rawInput1Size,
    const std::vector<VectorPtr>& rawInput2,
    vector_size_t rawInput2Size,
    const std::unordered_map<std::string, std::string>& config) {
  std::vector<TypePtr> rawInputTypes(rawInput1.size());
  std::transform(
      rawInput1.begin(),
      rawInput1.end(),
      rawInputTypes.begin(),
      [](const VectorPtr& vec) { return vec->type(); });

  HashStringAllocator allocator(pool());
  auto func =
      createAggregateFunction(functionName, rawInputTypes, allocator, config);
  int maxRowCount = std::max(rawInput1Size, rawInput2Size);
  std::vector<char> group(kOffset + func->accumulatorFixedWidthSize());
  std::vector<char*> groups(maxRowCount, group.data());
  std::vector<vector_size_t> indices(maxRowCount, 0);
  func->initializeNewGroups(groups.data(), indices);
  if (testGlobal) {
    func->addSingleGroupRawInput(
        group.data(), SelectivityVector(rawInput1Size), rawInput1, false);
  } else {
    func->addRawInput(
        groups.data(), SelectivityVector(rawInput1Size), rawInput1, false);
  }
  auto intermediateType = func->intermediateType(functionName, rawInputTypes);
  auto intermediate = BaseVector::create(intermediateType, 1, pool());
  func->extractAccumulators(groups.data(), 1, &intermediate);
  // Destroy accumulators to avoid memory leak.
  if (func->accumulatorUsesExternalMemory()) {
    func->destroy(folly::Range(groups.data(), 1));
  }

  // Create a new function picking up the intermediate result.
  auto func2 =
      createAggregateFunction(functionName, rawInputTypes, allocator, config);
  func2->initializeNewGroups(groups.data(), indices);
  if (testGlobal) {
    func2->addSingleGroupIntermediateResults(
        group.data(), SelectivityVector(1), {intermediate}, false);
  } else {
    func2->addIntermediateResults(
        groups.data(), SelectivityVector(1), {intermediate}, false);
  }
  // Add more raw input.
  if (testGlobal) {
    func2->addSingleGroupRawInput(
        group.data(), SelectivityVector(rawInput2Size), rawInput2, false);
  } else {
    func2->addRawInput(
        groups.data(), SelectivityVector(rawInput2Size), rawInput2, false);
  }
  auto result = BaseVector::create(func2->resultType(), 1, pool());
  func2->extractValues(groups.data(), 1, &result);
  // Destroy accumulators to avoid memory leak.
  if (func2->accumulatorUsesExternalMemory()) {
    func2->destroy(folly::Range(groups.data(), 1));
  }

  return result;
}

void AggregationTestBase::testFailingAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& expectedMessage,
    const std::unordered_map<std::string, std::string>& config) {
  {
    SCOPED_TRACE("Run single");
    auto builder = PlanBuilder().values(data);
    builder.singleAggregation(groupingKeys, aggregates);
    AssertQueryBuilder queryBuilder(builder.planNode());
    queryBuilder.configs(config);
    VELOX_ASSERT_THROW(queryBuilder.copyResults(pool()), expectedMessage);
  }

  {
    SCOPED_TRACE("Run partial + final");
    auto builder = PlanBuilder().values(data);
    builder.partialAggregation(groupingKeys, aggregates).finalAggregation();
    AssertQueryBuilder queryBuilder(builder.planNode());
    queryBuilder.configs(config);
    VELOX_ASSERT_THROW(queryBuilder.copyResults(pool()), expectedMessage);
  }

  {
    SCOPED_TRACE("Run partial + final with abandon partial agg");
    auto builder = PlanBuilder().values(data);
    builder.partialAggregation(groupingKeys, aggregates)
        .intermediateAggregation()
        .finalAggregation();
    AssertQueryBuilder queryBuilder(builder.planNode());
    queryBuilder.configs(config);
    enableAbandonPartialAggregation(queryBuilder);
    VELOX_ASSERT_THROW(queryBuilder.copyResults(pool()), expectedMessage);
  }

  {
    SCOPED_TRACE("Run partial + intermediate + final");
    auto builder = PlanBuilder().values(data);
    builder.partialAggregation(groupingKeys, aggregates)
        .intermediateAggregation()
        .finalAggregation();
    AssertQueryBuilder queryBuilder(builder.planNode());
    queryBuilder.configs(config);
    VELOX_ASSERT_THROW(queryBuilder.copyResults(pool()), expectedMessage);
  }

  if (!groupingKeys.empty()) {
    SCOPED_TRACE("Run partial + local exchange + final");
    auto builder = PlanBuilder().values(data);
    builder.partialAggregation(groupingKeys, aggregates)
        .localPartition(groupingKeys)
        .finalAggregation();
    AssertQueryBuilder queryBuilder(builder.planNode());
    queryBuilder.configs(config);
    VELOX_ASSERT_THROW(queryBuilder.copyResults(pool()), expectedMessage);
  }

  {
    SCOPED_TRACE(
        "Run partial + local exchange + intermediate + local exchange + final");
    auto builder = PlanBuilder().values(data);
    builder.partialAggregation(groupingKeys, aggregates);

    if (groupingKeys.empty()) {
      builder.localPartitionRoundRobinRow();
    } else {
      builder.localPartition(groupingKeys);
    }

    builder.intermediateAggregation()
        .localPartition(groupingKeys)
        .finalAggregation();

    AssertQueryBuilder queryBuilder(builder.planNode());
    queryBuilder.configs(config);
    VELOX_ASSERT_THROW(queryBuilder.copyResults(pool()), expectedMessage);
  }

  if (testStreaming_ && groupingKeys.empty()) {
    SCOPED_TRACE("Streaming");
    auto makeSource = [&](PlanBuilder& builder) { builder.values(data); };
    VELOX_ASSERT_THROW(
        validateStreamingInTestAggregations(makeSource, aggregates, config),
        expectedMessage);
  }
}
} // namespace facebook::velox::functions::aggregate::test
