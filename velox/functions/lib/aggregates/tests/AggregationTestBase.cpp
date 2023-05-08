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

#include "velox/functions/lib/aggregates/tests/AggregationTestBase.h"

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/AggregateCompanionSignatures.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::CursorParameters;
using facebook::velox::exec::test::PlanBuilder;
using facebook::velox::test::VectorMaker;

namespace facebook::velox::functions::aggregate::test {

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
  exec::test::OperatorTestBase::SetUp();
  filesystems::registerLocalFileSystem();
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql) {
  SCOPED_TRACE(duckDbSql);
  testAggregations(data, groupingKeys, aggregates, {}, duckDbSql);
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<RowVectorPtr>& expectedResult) {
  testAggregations(data, groupingKeys, aggregates, {}, expectedResult);
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::string& duckDbSql) {
  SCOPED_TRACE(duckDbSql);
  testAggregations(
      [&](PlanBuilder& builder) { builder.values(data); },
      groupingKeys,
      aggregates,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(duckDbSql); });
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

// Given a row type `type` and a list of field names, e.g., "c0, c1, c2",
// return a vector of TypePtr of the corresponding field types in the row type
// 'type'.
std::vector<TypePtr> getArgTypes(
    const TypePtr& type,
    const std::string& argList) {
  std::vector<folly::StringPiece> argNames;
  folly::split(',', argList, argNames);

  std::vector<TypePtr> result;
  auto rowType = asRowType(type);
  VELOX_CHECK_NOT_NULL(rowType);
  for (const auto& name : argNames) {
    auto childType = rowType->findChild(name);
    VELOX_CHECK_NOT_NULL(childType);
    result.push_back(std::move(childType));
  }
  return result;
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
    const TypePtr& inputType) {
  VELOX_CHECK_EQ(companionFunctions.partial.size(), 1);
  auto partialAggregate = fmt::format(
      "{}({})", companionFunctions.partial.front().functionName, aggregateArgs);

  VELOX_CHECK_EQ(companionFunctions.merge.size(), 1);
  auto mergeAggregate = fmt::format(
      "{}(a{})", companionFunctions.merge.front().functionName, index);

  std::string extractExpression;
  if (companionFunctions.extract.size() == 1) {
    extractExpression = fmt::format(
        "{}(a{})", companionFunctions.extract.front().functionName, index);
  } else {
    VELOX_CHECK_GT(companionFunctions.extract.size(), 1);
    auto extractFunctionName = getExtractFunctionNameWithSuffix(
        functionName, getArgTypes(inputType, aggregateArgs));
    extractExpression = fmt::format("{}(a{})", extractFunctionName, index);
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
    folly::split("(", aggregate, tokens);
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
    const std::vector<std::string>& postAggregationProjections,
    const std::string& duckDbSql) {
  testAggregationsWithCompanion(
      data,
      preAggregationProcessing,
      groupingKeys,
      aggregates,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(duckDbSql); });
}

void AggregationTestBase::testAggregationsWithCompanion(
    const std::vector<RowVectorPtr>& data,
    const std::function<void(PlanBuilder&)>& preAggregationProcessing,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(exec::test::AssertQueryBuilder&)>
        assertResults) {
  auto dataWithExtaGroupingKey = addExtraGroupingKey(data, "k0");
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
            data.front()->type());
    paritialAggregates.push_back(paritialAggregate);
    mergeAggregates.push_back(mergeAggregate);
    extractExpressions.push_back(extractAggregate);
  }

  {
    SCOPED_TRACE("Run partial + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtaGroupingKey);
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
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty() && allowInputShuffle_) {
    SCOPED_TRACE("Run partial + final with spilling");
    PlanBuilder builder(pool());
    builder.values(dataWithExtaGroupingKey);
    preAggregationProcessing(builder);

    // Spilling needs at least 2 batches of input. Use round-robin
    // repartitioning to split input into multiple batches.
    core::PlanNodeId partialNodeId;
    builder.localPartitionRoundRobin()
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
    queryBuilder.config(core::QueryConfig::kTestingSpillPct, "100")
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(core::QueryConfig::kAggregationSpillEnabled, "true")
        .spillDirectory(spillDirectory->path)
        .maxDrivers(4);

    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    auto inputRows = toPlanStats(task->taskStats()).at(partialNodeId).inputRows;
    if (inputRows > 1) {
      EXPECT_LT(0, spilledBytes(*task));
    } else {
      EXPECT_EQ(0, spilledBytes(*task));
    }
  }

  {
    SCOPED_TRACE("Run single");
    PlanBuilder builder(pool());
    builder.values(dataWithExtaGroupingKey);
    preAggregationProcessing(builder);
    builder.singleAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .singleAggregation(groupingKeys, mergeAggregates)
        .project(extractExpressions);

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    assertResults(queryBuilder);
  }

  {
    SCOPED_TRACE("Run partial + intermediate + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtaGroupingKey);
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
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty()) {
    SCOPED_TRACE("Run partial + local exchange + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtaGroupingKey);
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
    queryBuilder.maxDrivers(4);
    assertResults(queryBuilder);
  }

  {
    SCOPED_TRACE(
        "Run partial + local exchange + intermediate + local exchange + final");
    PlanBuilder builder(pool());
    builder.values(dataWithExtaGroupingKey);
    preAggregationProcessing(builder);
    builder.partialAggregation(groupingKeysWithPartialKey, paritialAggregates)
        .localPartition(groupingKeysWithPartialKey)
        .intermediateAggregation()
        .localPartition(groupingKeysWithPartialKey)
        .finalAggregation()
        .partialAggregation(groupingKeys, mergeAggregates);

    if (groupingKeys.empty()) {
      builder.localPartitionRoundRobin();
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
    queryBuilder.maxDrivers(2);
    assertResults(queryBuilder);
  }

  if (testStreaming_ && groupingKeys.empty() &&
      postAggregationProjections.empty()) {
    {
      SCOPED_TRACE("Streaming partial");
      auto partialResult = validateStreamingInTestAggregations(
          [&](auto& builder) { builder.values(dataWithExtaGroupingKey); },
          paritialAggregates);

      validateStreamingInTestAggregations(
          [&](auto& builder) { builder.values({partialResult}); },
          mergeAggregates);
    }
  }
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::vector<RowVectorPtr>& expectedResult) {
  testAggregations(
      [&](PlanBuilder& builder) { builder.values(data); },
      groupingKeys,
      aggregates,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(expectedResult); });
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql) {
  testAggregations(
      makeSource, groupingKeys, aggregates, {}, [&](auto& builder) {
        return builder.assertResults(duckDbSql);
      });
}

RowVectorPtr AggregationTestBase::validateStreamingInTestAggregations(
    const std::function<void(PlanBuilder&)>& makeSource,
    const std::vector<std::string>& aggregates) {
  PlanBuilder builder(pool());
  makeSource(builder);
  auto input = AssertQueryBuilder(builder.planNode()).copyResults(pool());
  if (input->size() < 2) {
    return nullptr;
  }
  auto size1 = input->size() / 2;
  auto size2 = input->size() - size1;
  builder.singleAggregation({}, aggregates);
  auto expected = AssertQueryBuilder(builder.planNode()).copyResults(pool());
  EXPECT_EQ(expected->size(), 1);
  auto& aggregationNode =
      static_cast<const core::AggregationNode&>(*builder.planNode());
  EXPECT_EQ(expected->childrenSize(), aggregationNode.aggregates().size());
  for (int i = 0; i < aggregationNode.aggregates().size(); ++i) {
    auto& aggregate = aggregationNode.aggregates()[i];
    SCOPED_TRACE(aggregate->name());
    std::vector<VectorPtr> rawInput1, rawInput2;
    for (auto& arg : aggregate->inputs()) {
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
    velox::test::assertEqualVectors(
        expected->childAt(i),
        testStreaming(
            aggregate->name(), true, rawInput1, size1, rawInput2, size2));
    velox::test::assertEqualVectors(
        expected->childAt(i),
        testStreaming(
            aggregate->name(), false, rawInput1, size1, rawInput2, size2));
  }
  return expected;
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(AssertQueryBuilder&)>
        assertResults) {
  {
    SCOPED_TRACE("Run partial + final");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.partialAggregation(groupingKeys, aggregates).finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty() && allowInputShuffle_) {
    SCOPED_TRACE("Run partial + final with spilling");
    PlanBuilder builder(pool());
    makeSource(builder);

    // Spilling needs at least 2 batches of input. Use round-robin
    // repartitioning to split input into multiple batches.
    core::PlanNodeId partialNodeId;
    builder.localPartitionRoundRobin()
        .partialAggregation(groupingKeys, aggregates)
        .capturePlanNodeId(partialNodeId)
        .localPartition(groupingKeys)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    auto spillDirectory = exec::test::TempDirectoryPath::create();

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.config(core::QueryConfig::kTestingSpillPct, "100")
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(core::QueryConfig::kAggregationSpillEnabled, "true")
        .spillDirectory(spillDirectory->path)
        .maxDrivers(4);

    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    auto inputRows = toPlanStats(task->taskStats()).at(partialNodeId).inputRows;
    if (inputRows > 1) {
      EXPECT_LT(0, spilledBytes(*task));
    } else {
      EXPECT_EQ(0, spilledBytes(*task));
    }
  }

  if (!groupingKeys.empty() && allowInputShuffle_) {
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
    queryBuilder
        .config(core::QueryConfig::kAbandonPartialAggregationMinRows, "1")
        .config(core::QueryConfig::kAbandonPartialAggregationMinPct, "0")
        .config(core::QueryConfig::kMaxPartialAggregationMemory, "0")
        .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, "0")
        .maxDrivers(1);

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
    queryBuilder.config(core::QueryConfig::kTestingSpillPct, "100")
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(core::QueryConfig::kAggregationSpillEnabled, "true")
        .spillDirectory(spillDirectory->path);

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
    assertResults(queryBuilder.maxDrivers(4));
  }

  {
    SCOPED_TRACE(
        "Run partial + local exchange + intermediate + local exchange + final");
    PlanBuilder builder(pool());
    makeSource(builder);

    builder.partialAggregation(groupingKeys, aggregates);

    if (groupingKeys.empty()) {
      builder.localPartitionRoundRobin();
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
    assertResults(queryBuilder.maxDrivers(2));
  }

  if (testStreaming_ && groupingKeys.empty() &&
      postAggregationProjections.empty()) {
    SCOPED_TRACE("Streaming");
    validateStreamingInTestAggregations(makeSource, aggregates);
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
    const std::vector<VectorPtr>& rawInput2) {
  VELOX_CHECK(!rawInput1.empty());
  VELOX_CHECK(!rawInput2.empty());
  return testStreaming(
      functionName,
      testGlobal,
      rawInput1,
      rawInput1[0]->size(),
      rawInput2,
      rawInput2[0]->size());
}

VectorPtr AggregationTestBase::testStreaming(
    const std::string& functionName,
    bool testGlobal,
    const std::vector<VectorPtr>& rawInput1,
    vector_size_t rawInput1Size,
    const std::vector<VectorPtr>& rawInput2,
    vector_size_t rawInput2Size) {
  constexpr int kRowSizeOffset = 8;
  constexpr int kOffset = kRowSizeOffset + 8;
  HashStringAllocator allocator(pool());
  std::vector<TypePtr> rawInputTypes;
  for (auto& vec : rawInput1) {
    rawInputTypes.push_back(vec->type());
  }
  auto [intermediateType, finalType] =
      getResultTypes(functionName, rawInputTypes);
  auto createFunction = [&, &finalType = finalType] {
    auto func = exec::Aggregate::create(
        functionName,
        core::AggregationNode::Step::kSingle,
        rawInputTypes,
        finalType);
    func->setAllocator(&allocator);
    func->setOffsets(kOffset, 0, 1, kRowSizeOffset);
    return func;
  };
  auto func = createFunction();
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
  auto intermediate = BaseVector::create(intermediateType, 1, pool());
  func->extractAccumulators(groups.data(), 1, &intermediate);
  // Create a new function picking up the intermediate result.
  auto func2 = createFunction();
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
  auto result = BaseVector::create(finalType, 1, pool());
  func2->extractValues(groups.data(), 1, &result);
  return result;
}

} // namespace facebook::velox::functions::aggregate::test
