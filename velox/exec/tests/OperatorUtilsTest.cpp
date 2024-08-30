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
#include "velox/exec/OperatorUtils.h"
#include <gtest/gtest.h>
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class OperatorUtilsTest : public OperatorTestBase {
 protected:
  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    OperatorTestBase::TearDown();
  }

  OperatorUtilsTest() {
    VectorMaker vectorMaker{pool_.get()};
    std::vector<RowVectorPtr> values = {vectorMaker.rowVector(
        {vectorMaker.flatVector<int32_t>(1, [](auto row) { return row; })})};
    core::PlanFragment planFragment;
    const core::PlanNodeId id{"0"};
    planFragment.planNode = std::make_shared<core::ValuesNode>(id, values);
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);

    task_ = Task::create(
        "SpillOperatorGroupTest_task",
        std::move(planFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        Task::ExecutionMode::kParallel);
    driver_ = Driver::testingCreate();
    driverCtx_ = std::make_unique<DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();
  }

  void gatherCopyTest(
      const std::shared_ptr<const RowType>& targetType,
      const std::shared_ptr<const RowType>& sourceType,
      int numSources) {
    folly::Random::DefaultGenerator rng(1);
    const int kNumRows = 500;
    const int kNumColumns = sourceType->size();

    // Build source vectors with nulls.
    std::vector<RowVectorPtr> sources;
    for (int i = 0; i < numSources; ++i) {
      sources.push_back(std::static_pointer_cast<RowVector>(
          BatchMaker::createBatch(sourceType, kNumRows, *pool_)));
      for (int j = 0; j < kNumColumns; ++j) {
        auto vector = sources.back()->childAt(j);
        int nullRow = (folly::Random::rand32() % kNumRows) / 4;
        while (nullRow < kNumRows) {
          vector->setNull(nullRow, true);
          nullRow +=
              std::max<int>(1, (folly::Random::rand32() % kNumColumns) / 4);
        }
      }
    }

    std::vector<IdentityProjection> columnMap;
    if (sourceType != targetType) {
      for (column_index_t sourceChannel = 0; sourceChannel < kNumColumns;
           ++sourceChannel) {
        const auto columnName = sourceType->nameOf(sourceChannel);
        const column_index_t targetChannel =
            targetType->getChildIdx(columnName);
        columnMap.emplace_back(sourceChannel, targetChannel);
      }
    }

    std::vector<const RowVector*> sourcesVectors(kNumRows);
    std::vector<vector_size_t> sourceIndices(kNumRows);
    for (int iter = 0; iter < 5; ++iter) {
      const int count =
          folly::Random::oneIn(10) ? 0 : folly::Random::rand32() % kNumRows;
      const int targetIndex = folly::Random::rand32() % (kNumRows - count);
      for (int i = 0; i < count; ++i) {
        sourcesVectors[i] = sources[folly::Random::rand32() % numSources].get();
        sourceIndices[i] = sourceIndices[folly::Random::rand32() % kNumRows];
      }
      auto targetVector =
          BaseVector::create<RowVector>(targetType, kNumRows, pool_.get());
      for (int32_t childIdx = 0; childIdx < targetVector->childrenSize();
           ++childIdx) {
        targetVector->childAt(childIdx)->resize(kNumRows);
      }
      gatherCopy(
          targetVector.get(),
          targetIndex,
          count,
          sourcesVectors,
          sourceIndices,
          columnMap);

      // Verify the copied data in target.
      for (int i = 0; i < kNumColumns; ++i) {
        const column_index_t sourceColumnChannel =
            columnMap.empty() ? i : columnMap[i].inputChannel;
        const column_index_t targetColumnChannel =
            columnMap.empty() ? i : columnMap[i].outputChannel;
        auto vector = targetVector->childAt(targetColumnChannel);
        for (int j = 0; j < count; ++j) {
          auto source = sourcesVectors[j]->childAt(sourceColumnChannel).get();
          if (vector->isNullAt(targetIndex + j)) {
            ASSERT_TRUE(source->isNullAt(sourceIndices[j]));
          } else {
            ASSERT_TRUE(vector->equalValueAt(
                source, targetIndex + j, sourceIndices[j]));
          }
        }
      }
    }
  }

  void setTaskOutputBatchConfig(
      uint32_t preferredBatchSize,
      uint32_t maxRows,
      uint64_t preferredBytes) {
    std::unordered_map<std::string, std::string> configs;
    configs[core::QueryConfig::kPreferredOutputBatchRows] =
        std::to_string(preferredBatchSize);
    configs[core::QueryConfig::kMaxOutputBatchRows] = std::to_string(maxRows);
    configs[core::QueryConfig::kPreferredOutputBatchBytes] =
        std::to_string(preferredBytes);
    task_->queryCtx()->testingOverrideConfigUnsafe(std::move(configs));
  }

  class MockOperator : public Operator {
   public:
    MockOperator(
        DriverCtx* driverCtx,
        RowTypePtr rowType,
        std::string operatorType = "MockType")
        : Operator(
              driverCtx,
              std::move(rowType),
              0,
              "MockOperator",
              operatorType) {}

    bool needsInput() const override {
      return false;
    }

    void addInput(RowVectorPtr input) override {}

    RowVectorPtr getOutput() override {
      return nullptr;
    }

    BlockingReason isBlocked(ContinueFuture* future) override {
      return BlockingReason::kNotBlocked;
    }

    bool isFinished() override {
      return false;
    }

    vector_size_t outputRows(
        std::optional<uint64_t> averageRowSize = std::nullopt) const {
      return outputBatchRows(averageRowSize);
    }
  };

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<Task> task_;
  std::shared_ptr<Driver> driver_;
  std::unique_ptr<DriverCtx> driverCtx_;
};

TEST_F(OperatorUtilsTest, wrapChildConstant) {
  auto constant = makeConstant(11, 1'000);

  BufferPtr mapping = allocateIndices(1'234, pool_.get());
  auto rawMapping = mapping->asMutable<vector_size_t>();
  for (auto i = 0; i < 1'234; i++) {
    rawMapping[i] = i / 2;
  }

  auto wrapped = exec::wrapChild(1'234, mapping, constant);
  ASSERT_EQ(wrapped->size(), 1'234);
  ASSERT_TRUE(wrapped->isConstantEncoding());
  ASSERT_TRUE(wrapped->equalValueAt(constant.get(), 100, 100));
}

TEST_F(OperatorUtilsTest, gatherCopy) {
  std::shared_ptr<const RowType> rowType;
  std::shared_ptr<const RowType> reversedRowType;
  {
    std::vector<std::string> names = {
        "bool_val",
        "tiny_val",
        "small_val",
        "int_val",
        "long_val",
        "ordinal",
        "float_val",
        "double_val",
        "string_val",
        "array_val",
        "struct_val",
        "map_val"};
    std::vector<std::string> reversedNames = names;
    std::reverse(reversedNames.begin(), reversedNames.end());

    std::vector<std::shared_ptr<const Type>> types = {
        BOOLEAN(),
        TINYINT(),
        SMALLINT(),
        INTEGER(),
        BIGINT(),
        BIGINT(),
        REAL(),
        DOUBLE(),
        VARCHAR(),
        ARRAY(VARCHAR()),
        ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}}),
        MAP(VARCHAR(),
            MAP(BIGINT(),
                ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))};
    std::vector<std::shared_ptr<const Type>> reversedTypes = types;
    std::reverse(reversedTypes.begin(), reversedTypes.end());

    rowType = ROW(std::move(names), std::move(types));
    reversedRowType = ROW(std::move(reversedNames), std::move(reversedTypes));
  }

  // Gather copy with identical column mapping.
  gatherCopyTest(rowType, rowType, 1);
  gatherCopyTest(rowType, rowType, 5);
  // Gather copy with non-identical column mapping.
  gatherCopyTest(rowType, reversedRowType, 1);
  gatherCopyTest(rowType, reversedRowType, 5);

  // Test with UNKNOWN type.
  int kNumRows = 100;
  auto sourceVector = makeRowVector({
      makeFlatVector<int64_t>(kNumRows, [](auto row) { return row % 7; }),
      BaseVector::createNullConstant(UNKNOWN(), kNumRows, pool()),
  });
  std::vector<const RowVector*> sourceVectors(kNumRows);
  std::vector<vector_size_t> sourceIndices(kNumRows);
  for (int i = 0; i < kNumRows; ++i) {
    sourceVectors[i] = sourceVector.get();
    sourceIndices[i] = kNumRows - i - 1;
  }
  auto targetVector = BaseVector::create<RowVector>(
      sourceVector->type(), kNumRows, pool_.get());
  for (int32_t childIdx = 0; childIdx < targetVector->childrenSize();
       ++childIdx) {
    targetVector->childAt(childIdx)->resize(kNumRows);
  }

  gatherCopy(targetVector.get(), 0, kNumRows, sourceVectors, sourceIndices);
  // Verify the copied data in target.
  for (int i = 0; i < targetVector->type()->size(); ++i) {
    auto vector = targetVector->childAt(i);
    for (int j = 0; j < kNumRows; ++j) {
      auto source = sourceVectors[j]->childAt(i).get();
      ASSERT_TRUE(vector->equalValueAt(source, j, sourceIndices[j]));
    }
  }
}

TEST_F(OperatorUtilsTest, makeOperatorSpillPath) {
  EXPECT_EQ("spill/3_1_100", makeOperatorSpillPath("spill", 3, 1, 100));
}

TEST_F(OperatorUtilsTest, wrap) {
  auto rowType = ROW({
      {"bool_val", BOOLEAN()},
      {"tiny_val", TINYINT()},
      {"small_val", SMALLINT()},
      {"int_val", INTEGER()},
      {"long_val", BIGINT()},
      {"ordinal", BIGINT()},
      {"float_val", REAL()},
      {"double_val", DOUBLE()},
      {"string_val", VARCHAR()},
      {"array_val", ARRAY(VARCHAR())},
      {"struct_val", ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
      {"map_val",
       MAP(VARCHAR(),
           MAP(BIGINT(),
               ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))},
  });

  VectorFuzzer fuzzer({}, pool());
  auto vector = fuzzer.fuzzFlat(rowType);
  auto rowVector = vector->as<RowVector>();

  for (int32_t iter = 0; iter < 20; ++iter) {
    folly::Random::DefaultGenerator rng;
    rng.seed(iter);
    const int32_t wrapVectorSize =
        iter == 0 ? 0 : 1 + folly::Random().rand32(2 * rowVector->size(), rng);
    BufferPtr wrapIndices =
        makeIndices(wrapVectorSize, [&](vector_size_t /* unused */) {
          return folly::Random().rand32(rowVector->size(), rng);
        });
    auto* rawWrapIndices = wrapIndices->as<vector_size_t>();

    auto wrapVector = wrap(
        wrapVectorSize, wrapIndices, rowType, rowVector->children(), pool());
    ASSERT_EQ(wrapVector->size(), wrapVectorSize);
    for (int32_t i = 0; i < wrapVectorSize; ++i) {
      wrapVector->equalValueAt(vector.get(), i, rawWrapIndices[i]);
    }

    wrapVector =
        wrap(wrapVectorSize, nullptr, rowType, rowVector->children(), pool());
    ASSERT_EQ(wrapVector->size(), 0);
  }
}

TEST_F(OperatorUtilsTest, addOperatorRuntimeStats) {
  std::unordered_map<std::string, RuntimeMetric> stats;
  const std::string statsName("stats");
  const RuntimeCounter minStatsValue(100, RuntimeCounter::Unit::kBytes);
  const RuntimeCounter maxStatsValue(200, RuntimeCounter::Unit::kBytes);
  addOperatorRuntimeStats(statsName, minStatsValue, stats);
  ASSERT_EQ(stats[statsName].count, 1);
  ASSERT_EQ(stats[statsName].sum, 100);
  ASSERT_EQ(stats[statsName].max, 100);
  ASSERT_EQ(stats[statsName].min, 100);

  addOperatorRuntimeStats(statsName, maxStatsValue, stats);
  ASSERT_EQ(stats[statsName].count, 2);
  ASSERT_EQ(stats[statsName].sum, 300);
  ASSERT_EQ(stats[statsName].max, 200);
  ASSERT_EQ(stats[statsName].min, 100);

  addOperatorRuntimeStats(statsName, maxStatsValue, stats);
  ASSERT_EQ(stats[statsName].count, 3);
  ASSERT_EQ(stats[statsName].sum, 500);
  ASSERT_EQ(stats[statsName].max, 200);
  ASSERT_EQ(stats[statsName].min, 100);
}

TEST_F(OperatorUtilsTest, initializeRowNumberMapping) {
  BufferPtr mapping;
  auto rawMapping = initializeRowNumberMapping(mapping, 10, pool());
  ASSERT_TRUE(mapping != nullptr);
  ASSERT_GE(mapping->size(), 10);

  rawMapping = initializeRowNumberMapping(mapping, 100, pool());
  ASSERT_GE(mapping->size(), 100);

  rawMapping = initializeRowNumberMapping(mapping, 60, pool());
  ASSERT_GE(mapping->size(), 100);

  ASSERT_EQ(mapping->refCount(), 1);
  auto otherMapping = mapping;
  ASSERT_EQ(mapping->refCount(), 2);
  ASSERT_EQ(mapping.get(), otherMapping.get());
  rawMapping = initializeRowNumberMapping(mapping, 10, pool());
  ASSERT_NE(mapping.get(), otherMapping.get());
}

TEST_F(OperatorUtilsTest, projectChildren) {
  const vector_size_t srcVectorSize{10};
  const auto srcRowType = ROW({
      {"bool_val", BOOLEAN()},
      {"int_val", INTEGER()},
      {"double_val", DOUBLE()},
      {"string_val", VARCHAR()},
  });
  VectorFuzzer fuzzer({}, pool());
  auto srcRowVector{fuzzer.fuzzRow(srcRowType, srcVectorSize)};

  {
    std::vector<IdentityProjection> emptyProjection;
    std::vector<VectorPtr> projectedChildren(srcRowType->size());
    projectChildren(
        projectedChildren,
        srcRowVector,
        emptyProjection,
        srcVectorSize,
        nullptr);
    for (vector_size_t i = 0; i < projectedChildren.size(); ++i) {
      ASSERT_EQ(projectedChildren[i], nullptr);
    }
  }

  {
    std::vector<IdentityProjection> identicalProjections{};
    for (auto i = 0; i < srcRowType->size(); ++i) {
      identicalProjections.emplace_back(i, i);
    }
    std::vector<VectorPtr> projectedChildren(srcRowType->size());
    projectChildren(
        projectedChildren,
        srcRowVector,
        identicalProjections,
        srcVectorSize,
        nullptr);
    for (const auto& projection : identicalProjections) {
      ASSERT_EQ(
          projectedChildren[projection.outputChannel].get(),
          srcRowVector->childAt(projection.inputChannel).get());
    }
  }

  {
    const auto destRowType = ROW({
        {"double_val", DOUBLE()},
        {"bool_val", BOOLEAN()},
    });
    std::vector<IdentityProjection> projections{};
    projections.emplace_back(2, 0);
    projections.emplace_back(0, 1);
    std::vector<VectorPtr> projectedChildren(srcRowType->size());
    projectChildren(
        projectedChildren, srcRowVector, projections, srcVectorSize, nullptr);
    for (const auto& projection : projections) {
      ASSERT_EQ(
          projectedChildren[projection.outputChannel].get(),
          srcRowVector->childAt(projection.inputChannel).get());
    }
  }
}

TEST_F(OperatorUtilsTest, reclaimableSectionGuard) {
  RowTypePtr rowType = ROW({"c0"}, {INTEGER()});

  MockOperator mockOp(driverCtx_.get(), rowType);
  ASSERT_FALSE(mockOp.testingNonReclaimable());
  {
    Operator::NonReclaimableSectionGuard guard(&mockOp);
    ASSERT_TRUE(mockOp.testingNonReclaimable());
    {
      Operator::NonReclaimableSectionGuard guard(&mockOp);
      ASSERT_TRUE(mockOp.testingNonReclaimable());
    }
    ASSERT_TRUE(mockOp.testingNonReclaimable());
    {
      Operator::ReclaimableSectionGuard guard(&mockOp);
      ASSERT_FALSE(mockOp.testingNonReclaimable());
      {
        Operator::NonReclaimableSectionGuard guard(&mockOp);
        ASSERT_TRUE(mockOp.testingNonReclaimable());
      }
      ASSERT_FALSE(mockOp.testingNonReclaimable());
      {
        Operator::NonReclaimableSectionGuard guard(&mockOp);
        ASSERT_TRUE(mockOp.testingNonReclaimable());
      }
      ASSERT_FALSE(mockOp.testingNonReclaimable());
    }
    ASSERT_TRUE(mockOp.testingNonReclaimable());
  }
  ASSERT_FALSE(mockOp.testingNonReclaimable());
}

TEST_F(OperatorUtilsTest, memStatsFromPool) {
  auto leafPool = rootPool_->addLeafChild("leaf-1.0");
  void* buffer;
  buffer = leafPool->allocate(2L << 20);
  leafPool->free(buffer, 2L << 20);
  const auto stats = MemoryStats::memStatsFromPool(leafPool.get());
  ASSERT_EQ(stats.userMemoryReservation, 0);
  ASSERT_EQ(stats.systemMemoryReservation, 0);
  ASSERT_EQ(stats.peakUserMemoryReservation, 2L << 20);
  ASSERT_EQ(stats.peakSystemMemoryReservation, 0);
  ASSERT_EQ(stats.numMemoryAllocations, 1);
}

TEST_F(OperatorUtilsTest, dynamicFilterStats) {
  DynamicFilterStats dynamicFilterStats;
  ASSERT_TRUE(dynamicFilterStats.empty());
  const std::string nodeId1{"node1"};
  const std::string nodeId2{"node2"};
  dynamicFilterStats.producerNodeIds.emplace(nodeId1);
  ASSERT_FALSE(dynamicFilterStats.empty());
  DynamicFilterStats dynamicFilterStatsToMerge;
  dynamicFilterStatsToMerge.producerNodeIds.emplace(nodeId1);
  ASSERT_FALSE(dynamicFilterStatsToMerge.empty());
  dynamicFilterStats.add(dynamicFilterStatsToMerge);
  ASSERT_EQ(dynamicFilterStats.producerNodeIds.size(), 1);
  ASSERT_EQ(
      dynamicFilterStats.producerNodeIds,
      std::unordered_set<core::PlanNodeId>({nodeId1}));

  dynamicFilterStatsToMerge.producerNodeIds.emplace(nodeId2);
  dynamicFilterStats.add(dynamicFilterStatsToMerge);
  ASSERT_EQ(dynamicFilterStats.producerNodeIds.size(), 2);
  ASSERT_EQ(
      dynamicFilterStats.producerNodeIds,
      std::unordered_set<core::PlanNodeId>({nodeId1, nodeId2}));

  dynamicFilterStats.clear();
  ASSERT_TRUE(dynamicFilterStats.empty());
}

TEST_F(OperatorUtilsTest, outputBatchRows) {
  RowTypePtr rowType = ROW({"c0"}, {INTEGER()});
  {
    setTaskOutputBatchConfig(10, 20, 234);
    MockOperator mockOp(driverCtx_.get(), rowType, "MockType1");
    ASSERT_EQ(10, mockOp.outputRows(std::nullopt));
    ASSERT_EQ(20, mockOp.outputRows(1));
    ASSERT_EQ(20, mockOp.outputRows(0));
    ASSERT_EQ(1, mockOp.outputRows(UINT64_MAX));
    ASSERT_EQ(1, mockOp.outputRows(1000));
    ASSERT_EQ(234 / 40, mockOp.outputRows(40));
  }
  {
    setTaskOutputBatchConfig(10, INT32_MAX, 3'000'000'000'000);
    MockOperator mockOp(driverCtx_.get(), rowType, "MockType2");
    ASSERT_EQ(1000, mockOp.outputRows(3'000'000'000));
  }
}
