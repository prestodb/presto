/*
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
#include <folly/Uri.h>
#include "folly/init/Init.h"

#include <boost/algorithm/cxx11/iota.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/sort.hpp>

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/operators/LocalShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleExchangeSource.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/ExchangeClient.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/row/CompactRow.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;
using namespace facebook::presto;
using namespace facebook::presto::operators;
using namespace ::testing;

namespace facebook::presto::operators::test {
namespace {

std::string makeTaskId(
    const std::string& prefix,
    int num,
    const std::string& shuffleInfo = "") {
  auto url = fmt::format("batch://{}-{}", prefix, num);
  if (shuffleInfo.empty()) {
    return url;
  }
  return url + "?shuffleInfo=" + shuffleInfo;
}

vector_size_t countNulls(const VectorPtr& vector) {
  vector_size_t numNulls = 0;
  for (auto i = 0; i < vector->size(); ++i) {
    if (vector->isNullAt(i)) {
      ++numNulls;
    }
  }
  return numNulls;
}

void cleanupDirectory(const std::string& rootPath) {
  auto fileSystem = velox::filesystems::getFileSystem(rootPath, nullptr);
  auto files = fileSystem->list(rootPath);
  for (auto& file : files) {
    fileSystem->remove(file);
  }
}

RowTypePtr createSerdeLayoutType(
    const RowTypePtr& originalType,
    const std::vector<std::string>& layout) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (const auto& name : layout) {
    auto idx = originalType->getChildIdx(name);
    names.push_back(name);
    types.push_back(originalType->childAt(idx));
  }
  return ROW(std::move(names), std::move(types));
}

using json = nlohmann::json;

struct TestShuffleInfo {
  uint32_t numPartitions;
  uint32_t maxBytesPerPartition;

  static TestShuffleInfo deserializeShuffleInfo(const std::string& info) {
    auto jsonReadInfo = nlohmann::json::parse(info);
    TestShuffleInfo shuffleInfo;
    jsonReadInfo.at("numPartitions").get_to(shuffleInfo.numPartitions);
    jsonReadInfo.at("maxBytesPerPartition")
        .get_to(shuffleInfo.maxBytesPerPartition);
    return shuffleInfo;
  }
};

std::vector<int> getSortOrder(const std::vector<std::string>& keys) {
  std::vector<int> indices(keys.size());
  boost::algorithm::iota(indices, 0);
  boost::range::sort(
      indices, [&keys](int a, int b) { return compareKeys(keys[a], keys[b]); });
  return indices;
}

// Register an ExchangeSource factory that creates ShuffleExchangeSource
// instances.
void registerExchangeSource(const std::string& shuffleName) {
  exec::ExchangeSource::factories().clear();
  exec::ExchangeSource::registerFactory(
      [shuffleName](
          const std::string& taskId,
          int destination,
          const std::shared_ptr<exec::ExchangeQueue>& queue,
          memory::MemoryPool* pool) -> std::shared_ptr<exec::ExchangeSource> {
        if (!taskId.starts_with("batch://")) {
          return nullptr;
        }
        auto uri = folly::Uri(taskId);
        auto queryParams = uri.getQueryParams();
        auto it = boost::range::find_if(queryParams, [](const auto& pair) {
          return pair.first == "shuffleInfo";
        });
        EXPECT_NE(it, queryParams.end())
            << "No shuffle read info provided in taskId: " << taskId;

        // Create ShuffleExchangeSource with the shuffle reader
        return std::make_shared<ShuffleExchangeSource>(
            taskId,
            destination,
            queue,
            ShuffleInterfaceFactory::factory(shuffleName)
                ->createReader(it->second, destination, pool),
            pool);
      });
}

std::string localShuffleWriteInfo(
    const std::string& rootPath,
    uint32_t numPartitions,
    bool sortedShuffle = false) {
  return LocalShuffleWriteInfo{
      .rootPath = rootPath,
      .queryId = "query_id",
      .numPartitions = numPartitions,
      .shuffleId = 0,
      .sortedShuffle = sortedShuffle}
      .serialize();
}

std::string localShuffleReadInfo(
    const std::string& rootPath,
    uint32_t partition,
    bool sortedShuffle = false) {
  return LocalShuffleReadInfo{
      .rootPath = rootPath,
      .queryId = "query_id",
      .partitionIds = {fmt::format("shuffle_0_0_{}", partition)},
      .sortedShuffle = sortedShuffle}
      .serialize();
}

} // namespace

class ShuffleTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();

    // Register shuffle operators once
    exec::Operator::registerOperator(
        std::make_unique<PartitionAndSerializeTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());
    exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());

    shuffleName_ = LocalPersistentShuffleFactory::kShuffleName;
    exec::ExchangeSource::factories().clear();
    ShuffleInterfaceFactory::registerFactory(
        std::string(shuffleName_),
        std::make_unique<LocalPersistentShuffleFactory>());
    registerExchangeSource(std::string(shuffleName_));
    // Create a temporary directory for shuffle files
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  void TearDown() override {
    exec::test::waitForAllTasksToBeDeleted();
    exec::ExchangeSource::factories().clear();
    exec::test::OperatorTestBase::TearDown();
  }

  std::shared_ptr<exec::Task> makeTask(
      const std::string& taskId,
      core::PlanNodePtr planNode,
      int destination,
      core::QueryConfig&& queryConfig) {
    auto queryCtx =
        core::QueryCtx::create(executor_.get(), std::move(queryConfig));
    core::PlanFragment planFragment{planNode};
    return exec::Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        exec::Task::ExecutionMode::kParallel);
  }

  void testPartitionAndSerialize(
      const RowVectorPtr& data,
      const VectorPtr& expectedReplicate = nullptr) {
    const bool replicateNullsAndAny = expectedReplicate != nullptr;
    auto plan =
        exec::test::PlanBuilder()
            .values({data})
            .addNode(addPartitionAndSerializeNode(7, replicateNullsAndAny))
            .planNode();

    auto results = exec::test::AssertQueryBuilder(plan).copyResults(pool());

    // Verify partition values are in [0, 7) range.
    auto partitions = results->childAt(0)->as<SimpleVector<int32_t>>();
    for (auto i = 0; i < results->size(); ++i) {
      ASSERT_GE(partitions->valueAt(i), 0);
      ASSERT_LT(partitions->valueAt(i), 7);
    }

    // Verify 'data'.
    auto deserialized = deserializeResult(results, asRowType(data->type()));
    velox::test::assertEqualVectors(data, deserialized);

    // Verify 'replicate' flags.
    if (replicateNullsAndAny) {
      velox::test::assertEqualVectors(results->childAt(3), expectedReplicate);
    }
  }

  void testPartitionAndSerialize(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected,
      const exec::CursorParameters params,
      const std::optional<uint32_t> expectedOutputCount = std::nullopt) {
    auto [taskCursor, serializedResults] = exec::test::readCursor(params);

    RowVectorPtr result =
        BaseVector::create<RowVector>(expected->type(), 0, pool());
    for (auto& serializedResult : serializedResults) {
      // Verify that serialized data can be deserialized successfully into the
      // original data.
      auto deserialized =
          deserializeResult(serializedResult, asRowType(expected->type()));
      if (deserialized != nullptr) {
        result->append(deserialized.get());
      }
    }
    velox::test::assertEqualVectors(expected, result);
    if (expectedOutputCount) {
      ASSERT_EQ(expectedOutputCount.value(), serializedResults.size());
    }
  }

  void testPartitionAndSerialize(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expected) {
    exec::CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = 2;
    testPartitionAndSerialize(plan, expected, params);
  }

  std::pair<std::unique_ptr<exec::TaskCursor>, std::vector<RowVectorPtr>>
  runShuffleReadTask(
      const exec::CursorParameters& params,
      const std::string& shuffleInfo) {
    return exec::test::readCursor(params, [&](exec::TaskCursor* taskCursor) {
      if (taskCursor->noMoreSplits()) {
        return;
      }

      auto& task = taskCursor->task();
      auto remoteSplit = std::make_shared<exec::RemoteConnectorSplit>(
          makeTaskId("read", 0, shuffleInfo));
      task->addSplit("0", exec::Split{remoteSplit});
      task->noMoreSplits("0");
      taskCursor->setNoMoreSplits();
    });
  }

  void runShuffleTest(
      const std::string& shuffleName,
      const std::string& serializedShuffleWriteInfo,
      const std::function<std::string(uint32_t partition)>&
          serializedShuffleReadInfo,
      bool replicateNullsAndAny,
      size_t numPartitions,
      size_t numMapDrivers,
      const std::vector<RowVectorPtr>& data,
      uint64_t backgroundCpuTimeNanos = 0,
      const std::optional<std::vector<velox::core::SortOrder>>& sortOrders =
          std::nullopt,
      const std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>&
          fields = std::nullopt,
      const std::optional<std::vector<std::vector<int>>>& expectedOrdering = {},
      core::QueryConfig&& queryConfig = core::QueryConfig({})) {
    // Register new shuffle related operators.
    exec::Operator::registerOperator(
        std::make_unique<PartitionAndSerializeTranslator>());
    exec::Operator::registerOperator(
        std::make_unique<ShuffleWriteTranslator>());
    exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());

    // Flatten the inputs to avoid issues assertEqualResults referred here:
    // https://github.com/facebookincubator/velox/issues/2859
    auto dataType = asRowType(data[0]->type());

    auto writerPlan =
        exec::test::PlanBuilder()
            .values(data, true)
            .addNode(addPartitionAndSerializeNode(
                numPartitions, replicateNullsAndAny, {}, sortOrders, fields))
            .localPartition(std::vector<std::string>{})
            .addNode(addShuffleWriteNode(
                numPartitions, shuffleName, serializedShuffleWriteInfo))
            .planNode();

    auto writerTaskId = makeTaskId("leaf", 0);
    auto writerTask =
        makeTask(writerTaskId, writerPlan, 0, std::move(queryConfig));
    writerTask->start(numMapDrivers);

    ASSERT_TRUE(exec::test::waitForTaskCompletion(writerTask.get(), 5'000'000));

    // Verify that shuffle stats got propagated to the ShuffleWrite operator.
    const auto shuffleWriteOperatorStats =
        writerTask->taskStats().pipelineStats[0].operatorStats.back();
    // Test if background CPU stats have been folded into finishTiming for
    // ShuffleWrite
    ASSERT_EQ(
        backgroundCpuTimeNanos > 0 ? 1 : 0,
        shuffleWriteOperatorStats.backgroundTiming.count);
    ASSERT_EQ(
        backgroundCpuTimeNanos,
        shuffleWriteOperatorStats.backgroundTiming.cpuNanos);
    const auto shuffleRuntimeStats = shuffleWriteOperatorStats.runtimeStats;
    ASSERT_EQ(
        1, shuffleRuntimeStats.count(fmt::format("{}.write", shuffleName)));

    // NOTE: each map driver processes the input once.
    std::vector<RowVectorPtr> expectedOutputVectors;
    for (const auto& input : data) {
      for (int i = 0; i < numMapDrivers; i++) {
        expectedOutputVectors.push_back(input);
      }
    }
    std::vector<RowVectorPtr> outputVectors;
    folly::F14FastSet<int32_t> emptyPartitions;
    folly::F14FastMap<int32_t, int32_t> numNulls;
    // Create and run multiple downstream tasks, one per partition, to read data
    // from shuffle.
    for (auto partition = 0; partition < numPartitions; ++partition) {
      auto plan = exec::test::PlanBuilder()
                      .addNode(addShuffleReadNode(dataType))
                      .project(dataType->names())
                      .planNode();

      exec::CursorParameters params;
      params.planNode = plan;
      params.destination = partition;

      auto [taskCursor, results] =
          runShuffleReadTask(params, serializedShuffleReadInfo(partition));

      // Verify that shuffle stats got propagated to the Exchange operator.
      const auto shuffleReadOperatorStats =
          taskCursor->task()->taskStats().pipelineStats[0].operatorStats[0];
      // Test if background CPU stats have been folded into finishTiming for
      // ShuffleRead
      ASSERT_EQ(
          backgroundCpuTimeNanos > 0 ? 1 : 0,
          shuffleReadOperatorStats.backgroundTiming.count);
      ASSERT_EQ(
          backgroundCpuTimeNanos,
          shuffleReadOperatorStats.backgroundTiming.cpuNanos);
      const auto exchangeStats = shuffleReadOperatorStats.runtimeStats;
      ASSERT_EQ(1, exchangeStats.count(fmt::format("{}.read", shuffleName)));

      vector_size_t numResults = 0;
      std::vector<RowVectorPtr> partitionResults;
      for (const auto& result : results) {
        auto copied = copyResultVector(result);
        outputVectors.push_back(copied);
        partitionResults.push_back(copied);
        numResults += result->size();
        numNulls[partition] += countNulls(result->childAt(0));
      }
      if (numResults == 0) {
        emptyPartitions.insert(partition);
      }

      // Verify ordering for sorted shuffle
      if (sortOrders.has_value() && !partitionResults.empty() &&
          fields.has_value()) {
        // Concatenate all result vectors for this partition
        auto totalSize = 0;
        for (const auto& vec : partitionResults) {
          totalSize += vec->size();
        }
        auto result = BaseVector::create<RowVector>(
            partitionResults[0]->type(), totalSize, pool());
        auto offset = 0;
        for (const auto& vec : partitionResults) {
          result->copy(vec.get(), offset, 0, vec->size());
          offset += vec->size();
        }

        // Verify that rows are sorted according to sortOrders
        const auto& orders = sortOrders.value();
        const auto& sortFields = fields.value();

        for (vector_size_t row = 1; row < result->size(); ++row) {
          for (size_t i = 0; i < orders.size(); ++i) {
            const auto& sortOrder = orders[i];
            const auto& field = sortFields[i];

            // Find the column index for this field
            auto fieldName = field->name();
            auto channel = dataType->getChildIdx(fieldName);

            auto column = result->childAt(channel);
            auto prevIsNull = column->isNullAt(row - 1);
            auto currIsNull = column->isNullAt(row);

            // Handle nulls
            if (prevIsNull && currIsNull) {
              continue; // Both null, equal, check next key
            }
            if (prevIsNull) {
              // Previous is null, current is not
              ASSERT_TRUE(sortOrder.isNullsFirst())
                  << "Partition " << partition << ": Null at row " << (row - 1)
                  << " should come first for field " << fieldName;
              break;
            }
            if (currIsNull) {
              // Current is null, previous is not
              ASSERT_FALSE(sortOrder.isNullsFirst())
                  << "Partition " << partition << ": Null at row " << row
                  << " should come last for field " << fieldName;
              break;
            }

            auto cmp = column->compare(column.get(), row - 1, row);
            if (cmp == 0) {
              continue;
            }

            bool inOrder = sortOrder.isAscending() ? (cmp < 0) : (cmp > 0);
            ASSERT_TRUE(inOrder)
                << "Partition " << partition << ": Row " << row
                << " is out of order on sort field " << fieldName
                << " (ascending=" << sortOrder.isAscending() << ")";
            break;
          }
        }
      }
    }

    if (replicateNullsAndAny) {
      EXPECT_EQ(emptyPartitions.size(), 0);
      ASSERT_EQ(numPartitions, numNulls.size());

      vector_size_t expectedNullCount = 0;
      for (const auto& input : data) {
        expectedNullCount += countNulls(input->childAt(0));
      }
      expectedNullCount *= numMapDrivers;

      for (auto i = 0; i < numPartitions; ++i) {
        ASSERT_TRUE(numNulls.contains(i));
        ASSERT_EQ(expectedNullCount, numNulls[i]);
      }
      // TODO: Add assertContainResults for the remaining elements
    } else {
      velox::exec::test::assertEqualResults(
          expectedOutputVectors, outputVectors);
    }
  }

  void partitionAndSerializeWithThresholds(
      vector_size_t outputRowLimit,
      size_t outputSizeLimit,
      vector_size_t inputRows,
      size_t expectedOutputCount,
      bool sorted = false) {
    VectorFuzzer::Options opts;
    opts.vectorSize = 10;
    opts.nullRatio = 0;
    opts.dictionaryHasNulls = false;
    opts.stringLength = 10000;
    opts.containerLength = 10000;
    opts.stringVariableLength = false;
    opts.containerVariableLength = false;
    auto seed = folly::Random::rand32();
    VectorFuzzer fuzzer(opts, pool_.get(), seed);
    // Create a deeply nested row, such that each row exceeds the output batch
    // limit.
    RowVectorPtr data;
    std::optional<std::vector<velox::core::SortOrder>> ordering = std::nullopt;
    std::optional<
        std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>>
        fields = std::nullopt;

    if (sorted) {
      data = makeRowVector(
          // value
          {fuzzer.fuzzConstant(INTEGER(), inputRows),
           // sort key
           fuzzer.fuzzRow(
               {fuzzer.fuzzConstant(VARCHAR(), 100),
                fuzzer.fuzzArray(
                    fuzzer.fuzzArray(fuzzer.fuzzFlat(DOUBLE()), 100), 100)},
               inputRows)});
      ordering = {velox::core::SortOrder(velox::core::kAscNullsFirst)};
      fields =
          std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>{
              std::make_shared<const velox::core::FieldAccessTypedExpr>(
                  INTEGER(), "c1")};
    } else {
      data = makeRowVector({fuzzer.fuzzMap(
          fuzzer.fuzzConstant(VARCHAR(), 100),
          fuzzer.fuzzArray(
              fuzzer.fuzzArray(fuzzer.fuzzFlat(DOUBLE()), 100), 100),
          inputRows)});
    }

    auto plan = exec::test::PlanBuilder()
                    .values({data}, false)
                    .addNode(addPartitionAndSerializeNode(
                        2, true, {"c0"}, ordering, fields))
                    .planNode();

    auto properties = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kPreferredOutputBatchBytes,
         std::to_string(outputSizeLimit)},
        {core::QueryConfig::kPreferredOutputBatchRows,
         std::to_string(outputRowLimit)}};

    auto queryCtx =
        core::QueryCtx::create(executor_.get(), core::QueryConfig(properties));
    auto params = exec::CursorParameters();
    params.planNode = plan;
    params.queryCtx = queryCtx;

    auto expected = makeRowVector({data->childAt(0)});
    testPartitionAndSerialize(plan, expected, params, expectedOutputCount);
  }

  void runPartitionAndSerializeSerdeTest(
      const RowVectorPtr& data,
      size_t numPartitions,
      const std::optional<std::vector<std::string>>& serdeLayout =
          std::nullopt) {
    auto shuffleInfo =
        localShuffleWriteInfo(tempDir_->getPath(), numPartitions);
    auto plan = exec::test::PlanBuilder()
                    .values({data}, true)
                    .addNode(addPartitionAndSerializeNode(
                        numPartitions,
                        false,
                        serdeLayout.value_or(std::vector<std::string>{})))
                    .localPartition(std::vector<std::string>{})
                    .addNode(addShuffleWriteNode(
                        numPartitions, std::string(shuffleName_), shuffleInfo))
                    .planNode();

    exec::CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = 1;

    auto [taskCursor, results] = exec::test::readCursor(params);
    ASSERT_EQ(results.size(), 0);

    // Verify by reading shuffle files
    int totalRows = 0;
    for (size_t partition = 0; partition < numPartitions; ++partition) {
      auto reader = std::make_shared<LocalShuffleReader>(
          tempDir_->getPath(),
          "query_id",
          std::vector<std::string>{fmt::format("shuffle_0_0_{}", partition)},
          false,
          pool());
      reader->initialize();

      while (true) {
        auto batches = reader->next(1 << 20).get();
        if (batches.empty()) {
          break;
        }
        for (auto& batch : batches) {
          totalRows += batch->rows().size();
        }
      }
    }
    ASSERT_EQ(totalRows, data->size());
  }

  void fuzzerTest(bool replicateNullsAndAny, size_t numPartitions) {
    // For unit testing, these numbers are set to relatively small values.
    // For stress testing, the following parameters and the fuzzer vector,
    // string and container sizes can be bumped up.
    size_t numMapDrivers = 1;
    size_t numInputVectors = 5;
    size_t numIterations = 5;

    // Set up the fuzzer parameters.
    VectorFuzzer::Options opts;
    opts.vectorSize = 1000;
    opts.nullRatio = 0.1;
    opts.dictionaryHasNulls = false;
    opts.stringVariableLength = true;

    // UnsafeRows use microseconds to store timestamp.
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
    opts.stringLength = 100;
    opts.containerLength = 10;

    // For the time being, we are not including any MAP or more than three level
    // nested data structures given the limitations of the fuzzer and
    // assertEqualResults:
    // Limitations of assertEqualResults:
    // https://github.com/facebookincubator/velox/issues/2859
    auto rowType = ROW({
        {"c0", INTEGER()},
        {"c1", TINYINT()},
        {"c2", INTEGER()},
        {"c3", BIGINT()},
        {"c4", INTEGER()},
        {"c5", TIMESTAMP()},
        {"c6", REAL()},
        {"c7", TINYINT()},
        {"c8", DOUBLE()},
        {"c9", VARCHAR()},
        {"c10", ROW({VARCHAR(), INTEGER(), TIMESTAMP()})},
        {"c11", INTEGER()},
        {"c12", REAL()},
        {"c13", ARRAY(INTEGER())},
        {"c14", ARRAY(TINYINT())},
        {"c15", ROW({INTEGER(), VARCHAR(), ARRAY(INTEGER())})},
        {"c16", MAP(TINYINT(), REAL())},
    });

    auto rootDirectory = velox::exec::test::TempDirectoryPath::create();
    auto rootPath = rootDirectory->getPath();
    const std::string shuffleWriteInfo =
        localShuffleWriteInfo(rootPath, numPartitions);

    for (int it = 0; it < numIterations; it++) {
      auto seed = folly::Random::rand32();

      SCOPED_TRACE(
          fmt::format(
              "Iteration {}, numPartitions {}, replicateNullsAndAny {}, seed {}",
              it,
              numPartitions,
              replicateNullsAndAny,
              seed));

      VectorFuzzer fuzzer(opts, pool_.get(), seed);
      std::vector<RowVectorPtr> inputVectors;
      // Create input vectors.
      for (size_t i = 0; i < numInputVectors; ++i) {
        auto input = fuzzer.fuzzInputRow(rowType);
        inputVectors.push_back(input);
      }
      velox::exec::ExchangeSource::factories().clear();
      registerExchangeSource(
          std::string(LocalPersistentShuffleFactory::kShuffleName));
      runShuffleTest(
          std::string(LocalPersistentShuffleFactory::kShuffleName),
          shuffleWriteInfo,
          [&](auto partition) {
            return localShuffleReadInfo(rootPath, partition);
          },
          replicateNullsAndAny,
          numPartitions,
          numMapDrivers,
          inputVectors);
      cleanupDirectory(rootPath);
    }
  }

  std::string_view shuffleName_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
  enum class DataType {
    BASIC,
    BASIC_WITH_NULLS,
    SORTED_BIGINT,
    SORTED_VARCHAR
  };

  struct DataGeneratorConfig {
    size_t numRows = 6;
    size_t numBatches = 1;
    double nullRatio = 0.0;
    int32_t seed = 0;
  };

  struct GeneratedTestData {
    std::vector<RowVectorPtr> inputData;
    std::optional<std::vector<velox::core::SortOrder>> ordering;
    std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>> fields;
    std::optional<std::vector<std::vector<int>>> expectedSortingOrder;
    std::optional<core::QueryConfig> queryConfig;
  };

  GeneratedTestData generateTestData(
      const DataGeneratorConfig& genConfig,
      DataType type) {
    GeneratedTestData result;

    VectorFuzzer::Options opts;
    opts.vectorSize = genConfig.numRows;
    opts.nullRatio = genConfig.nullRatio;
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
    VectorFuzzer fuzzer(opts, pool_.get(), genConfig.seed);

    switch (type) {
      case DataType::BASIC: {
        for (size_t i = 0; i < genConfig.numBatches; ++i) {
          result.inputData.push_back(makeRowVector({
              makeFlatVector<int32_t>(
                  genConfig.numRows,
                  [i, genConfig](auto row) {
                    return static_cast<int32_t>(
                        (i * genConfig.numRows) + row + 1);
                  }),
              makeFlatVector<int64_t>(
                  genConfig.numRows,
                  [i, genConfig](auto row) {
                    return static_cast<int64_t>(
                        (i * genConfig.numRows + row + 1) * 10);
                  }),
          }));
        }
        break;
      }

      case DataType::BASIC_WITH_NULLS: {
        for (size_t i = 0; i < genConfig.numBatches; ++i) {
          std::vector<std::optional<int32_t>> col0Values;
          std::vector<int64_t> col1Values;
          col0Values.reserve(genConfig.numRows + 1);
          col1Values.reserve(genConfig.numRows + 1);

          for (size_t row = 0; row < genConfig.numRows; ++row) {
            col0Values.emplace_back(
                static_cast<int32_t>((i * genConfig.numRows) + row + 1));
            col1Values.push_back(
                static_cast<int64_t>((i * genConfig.numRows + row + 1) * 10));
          }
          col0Values.emplace_back(std::nullopt);
          col1Values.push_back(
              static_cast<int64_t>((genConfig.numRows + 1) * 10));

          result.inputData.push_back(makeRowVector({
              makeNullableFlatVector<int32_t>(col0Values),
              makeFlatVector<int64_t>(col1Values),
          }));
        }
        break;
      }

      case DataType::SORTED_BIGINT: {
        const size_t rowsPerBatch = genConfig.numRows / genConfig.numBatches;
        for (size_t i = 0; i < genConfig.numBatches; ++i) {
          std::vector<int32_t> partitions;
          std::vector<int64_t> values;
          partitions.reserve(rowsPerBatch);
          values.reserve(rowsPerBatch);

          for (size_t row = 0; row < rowsPerBatch; ++row) {
            partitions.push_back(static_cast<int32_t>(row % 2));
            values.push_back(
                static_cast<int64_t>((i * rowsPerBatch) + row + 1) * 10);
          }

          result.inputData.push_back(makeRowVector({
              makeFlatVector<int32_t>(partitions),
              makeFlatVector<int64_t>(values),
          }));
        }
        result.ordering = {velox::core::SortOrder(velox::core::kAscNullsFirst)};
        result.fields = std::vector<velox::core::FieldAccessTypedExprPtr>{
            std::make_shared<const velox::core::FieldAccessTypedExpr>(
                velox::BIGINT(), fmt::format("c{}", 1))};
        break;
      }

      case DataType::SORTED_VARCHAR: {
        const size_t rowsPerBatch = genConfig.numRows / genConfig.numBatches;
        for (size_t i = 0; i < genConfig.numBatches; ++i) {
          std::vector<int32_t> partitions;
          std::vector<std::string> keys;
          partitions.reserve(rowsPerBatch);
          keys.reserve(rowsPerBatch);

          for (size_t row = 0; row < rowsPerBatch; ++row) {
            partitions.push_back(static_cast<int32_t>(row % 2));
            keys.push_back(fmt::format("key{:04d}", rowsPerBatch - row));
          }

          result.inputData.push_back(makeRowVector({
              makeFlatVector<int32_t>(partitions),
              makeFlatVector<std::string>(keys),
          }));
        }
        result.ordering = {velox::core::SortOrder(velox::core::kAscNullsFirst)};
        result.fields = std::vector<velox::core::FieldAccessTypedExprPtr>{
            std::make_shared<const velox::core::FieldAccessTypedExpr>(
                velox::VARCHAR(), fmt::format("c{}", 1))};
        auto properties = std::unordered_map<std::string, std::string>{
            {core::QueryConfig::kPreferredOutputBatchBytes,
             std::to_string(1'000'000'000)},
            {core::QueryConfig::kPreferredOutputBatchRows, std::to_string(3)}};
        result.queryConfig = core::QueryConfig(properties);
        break;
      }
    }

    return result;
  }

 private:
  RowVectorPtr deserializeResult(
      const RowVectorPtr& serializedResult,
      const RowTypePtr& rowType) {
    auto serializedData =
        serializedResult->childAt(2)->as<FlatVector<StringView>>();
    auto* rawValues = serializedData->rawValues();

    std::vector<std::string_view> rows;
    rows.reserve(serializedData->size());
    for (auto i = 0; i < serializedData->size(); ++i) {
      const auto& serializedRow = rawValues[i];
      rows.push_back(
          std::string_view(serializedRow.data(), serializedRow.size()));
    }

    return std::dynamic_pointer_cast<RowVector>(
        row::CompactRow::deserialize(rows, rowType, pool()));
  }

  RowVectorPtr copyResultVector(const RowVectorPtr& result) {
    auto vector = std::static_pointer_cast<RowVector>(
        BaseVector::create(result->type(), result->size(), pool()));
    vector->copy(result.get(), 0, 0, result->size());
    VELOX_CHECK_EQ(vector->size(), result->size());
    return vector;
  }

  RowVectorPtr reorderColumns(
      const RowVectorPtr& data,
      const std::vector<std::string>& newLayout) {
    auto rowType = asRowType(data->type());
    std::vector<VectorPtr> columns;
    for (const auto& name : newLayout) {
      columns.push_back(data->childAt(rowType->getChildIdx(name)));
    }
    return makeRowVector(columns);
  }
};

TEST_F(ShuffleTest, operators) {
  const std::string shuffleRootPath = tempDir_->getPath();
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });
  auto info = localShuffleWriteInfo(shuffleRootPath, 4);
  auto plan =
      exec::test::PlanBuilder()
          .values({data}, true)
          .addNode(addPartitionAndSerializeNode(4, false))
          .localPartition(std::vector<std::string>{})
          .addNode(addShuffleWriteNode(4, std::string(shuffleName_), info))
          .planNode();

  auto results = exec::test::AssertQueryBuilder(plan).copyResults(pool());
  // Shuffle write returns empty results
  EXPECT_EQ(results->size(), 0);
}

DEBUG_ONLY_TEST_F(ShuffleTest, shuffleWriterExceptions) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });
  auto info = localShuffleWriteInfo(tempDir_->getPath(), 4);

  SCOPED_TESTVALUE_SET(
      "facebook::presto::operators::LocalShuffleWriter::collect",
      std::function<void(LocalShuffleWriter*)>(
          [&](LocalShuffleWriter* /*writer*/) {
            // Trigger a std::bad_function_call exception.
            std::function<bool()> nullFunction = nullptr;
            VELOX_CHECK(nullFunction());
          }));

  exec::CursorParameters params;
  params.planNode =
      exec::test::PlanBuilder()
          .values({data})
          .addNode(addPartitionAndSerializeNode(4, false))
          .addNode(addShuffleWriteNode(4, std::string(shuffleName_), info))
          .planNode();

  VELOX_ASSERT_THROW(
      exec::test::readCursor(params), "ShuffleWriter::collect failed");
}

DEBUG_ONLY_TEST_F(ShuffleTest, shuffleReaderExceptions) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });

  auto writeInfo = localShuffleWriteInfo(tempDir_->getPath(), 4);

  exec::CursorParameters params;
  params.planNode =
      exec::test::PlanBuilder()
          .values({data})
          .addNode(addPartitionAndSerializeNode(2, false))
          .addNode(addShuffleWriteNode(2, std::string(shuffleName_), writeInfo))
          .planNode();

  ASSERT_NO_THROW(exec::test::readCursor(params));

  std::function<void(LocalShuffleReader*)> injectFailure =
      [&](LocalShuffleReader* /*reader*/) {
        // Trigger a std::bad_function_call exception.
        std::function<bool()> nullFunction = nullptr;
        VELOX_CHECK(nullFunction());
      };

  exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());
  velox::exec::ExchangeSource::factories().clear();
  registerExchangeSource(std::string(shuffleName_));

  params.planNode = exec::test::PlanBuilder()
                        .addNode(addShuffleReadNode(asRowType(data->type())))
                        .planNode();

  {
    SCOPED_TESTVALUE_SET(
        "facebook::presto::operators::LocalShuffleReader::next", injectFailure);

    auto readInfo = localShuffleReadInfo(tempDir_->getPath(), 2, 0);
    VELOX_ASSERT_THROW(
        runShuffleReadTask(params, readInfo), "ShuffleReader::next failed");
  }
}

TEST_F(ShuffleTest, shuffleDeserialization) {
  std::string serializedWriteInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1,\n"
      "  \"numPartitions\": 11\n"
      "}";
  LocalShuffleWriteInfo shuffleWriteInfo =
      LocalShuffleWriteInfo::deserialize(serializedWriteInfo);
  EXPECT_EQ(shuffleWriteInfo.rootPath, "abc");
  EXPECT_EQ(shuffleWriteInfo.queryId, "query_id");
  EXPECT_EQ(shuffleWriteInfo.shuffleId, 1);
  EXPECT_EQ(shuffleWriteInfo.numPartitions, 11);

  serializedWriteInfo =
      "{\n"
      "  \"rootPath\": \"efg\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1,\n"
      "  \"numPartitions\": 12\n"
      "}";
  shuffleWriteInfo = LocalShuffleWriteInfo::deserialize(serializedWriteInfo);
  EXPECT_EQ(shuffleWriteInfo.rootPath, "efg");
  EXPECT_EQ(shuffleWriteInfo.queryId, "query_id");
  EXPECT_EQ(shuffleWriteInfo.shuffleId, 1);
  EXPECT_EQ(shuffleWriteInfo.numPartitions, 12);

  std::string serializedReadInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"partitionIds\": [ \"shuffle1\" ]\n"
      "}";
  std::vector<std::string> partitionIds{"shuffle1"};
  LocalShuffleReadInfo shuffleReadInfo =
      LocalShuffleReadInfo::deserialize(serializedReadInfo);
  EXPECT_EQ(shuffleReadInfo.rootPath, "abc");
  EXPECT_EQ(shuffleReadInfo.queryId, "query_id");
  EXPECT_EQ(shuffleReadInfo.partitionIds, partitionIds);

  std::string badSerializedInfo =
      "{\n"
      "  \"rootpath\": \"efg\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": 1\n"
      "}";
  EXPECT_THROW(
      LocalShuffleWriteInfo::deserialize(badSerializedInfo),
      nlohmann::detail::out_of_range);

  badSerializedInfo =
      "{\n"
      "  \"rootPath\": \"abc\",\n"
      "  \"queryId\": \"query_id\",\n"
      "  \"shuffleId\": \"hey-wrong-type\"\n"
      "}";
  EXPECT_THROW(
      LocalShuffleWriteInfo::deserialize(badSerializedInfo),
      nlohmann::detail::type_error);
}

TEST_F(ShuffleTest, replicateNullsAndAny) {
  // No nulls. Expect to replicate first row.
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
  });

  testPartitionAndSerialize(
      data, makeFlatVector<bool>({true, false, false, false}));

  // Nulls. Expect to replicate rows with nulls.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, std::nullopt, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
  });

  testPartitionAndSerialize(
      data, makeFlatVector<bool>({false, false, true, true, false}));

  // Null in the first row.
  data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {std::nullopt, 2, std::nullopt, std::nullopt, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
  });

  testPartitionAndSerialize(
      data, makeFlatVector<bool>({true, false, true, true, false}));
}

TEST_F(ShuffleTest, partitionAndSerializeOutputRowLimit) {
  partitionAndSerializeWithThresholds(5, 1'000'000'000, 10, 2);
}

TEST_F(ShuffleTest, partitionAndSerializeOutputRowLimitWithSort) {
  partitionAndSerializeWithThresholds(5, 1'000'000'000, 10, 2, true);
}

TEST_F(ShuffleTest, partitionAndSerializeOutputByteLimitWithSort) {
  partitionAndSerializeWithThresholds(10'000, 100, 10, 10, true);
}

TEST_F(ShuffleTest, partitionAndSerializeNoLimit) {
  partitionAndSerializeWithThresholds(1'000, 1'000'000'000, 5, 1);
}

TEST_F(ShuffleTest, partitionAndSerializeBothLimited) {
  partitionAndSerializeWithThresholds(1, 1'000'000, 5, 5);
}

TEST_F(ShuffleTest, partitionAndSerializeOperator) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .planNode();

  testPartitionAndSerialize(plan, data);
}

TEST_F(ShuffleTest, partitionAndSerializeWithLargeInput) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(20'000, [](auto row) { return row; })});

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(1, true))
                  .planNode();

  testPartitionAndSerialize(plan, data);
}

TEST_F(ShuffleTest, partitionAndSerializeOutputByteLimit) {
  partitionAndSerializeWithThresholds(10'000, 1, 10, 10);
}

TEST_F(ShuffleTest, partitionAndSerializeWithDifferentColumnOrder) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(4, false, {"c1", "c0"}))
                  .planNode();

  auto expected = makeRowVector({data->childAt(1), data->childAt(0)});
  testPartitionAndSerialize(plan, expected);

  // Duplicate some columns.
  plan =
      exec::test::PlanBuilder()
          .values({data}, false)
          .addNode(addPartitionAndSerializeNode(4, false, {"c1", "c0", "c1"}))
          .planNode();

  expected =
      makeRowVector({data->childAt(1), data->childAt(0), data->childAt(1)});
  testPartitionAndSerialize(plan, expected);

  // Remove one column.
  plan = exec::test::PlanBuilder()
             .values({data}, false)
             .addNode(addPartitionAndSerializeNode(4, false, {"c1"}))
             .planNode();

  expected = makeRowVector({data->childAt(1)});
  testPartitionAndSerialize(plan, expected);
}

TEST_F(ShuffleTest, partitionAndSerializeOperatorWhenSinglePartition) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, false)
                  .addNode(addPartitionAndSerializeNode(1, false))
                  .planNode();

  testPartitionAndSerialize(plan, data);
}

TEST_F(ShuffleTest, partitionAndSerializeEndToEnd) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runPartitionAndSerializeSerdeTest(data, 4);

  // Clean up shuffle files between test scenarios to avoid file name collisions
  cleanupDirectory(tempDir_->getPath());

  data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40}),
      makeFlatVector<std::string>({"a", "b", "c", "d"}),
  });

  runPartitionAndSerializeSerdeTest(data, 2, {{"c2", "c0"}});
}

class DummyShuffleInterfaceFactory : public ShuffleInterfaceFactory {
 public:
  std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedShuffleInfo,
      const int32_t partition,
      velox::memory::MemoryPool* pool) override {
    return nullptr;
  }
  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedShuffleInfo,
      velox::memory::MemoryPool* pool) override {
    return nullptr;
  }
};

TEST_F(ShuffleTest, shuffleInterfaceRegistration) {
  const std::string kShuffleName = "dummy-shuffle";
  EXPECT_TRUE(
      ShuffleInterfaceFactory::registerFactory(
          kShuffleName, std::make_unique<DummyShuffleInterfaceFactory>()));
  EXPECT_NO_THROW(ShuffleInterfaceFactory::factory(kShuffleName));
  EXPECT_FALSE(
      ShuffleInterfaceFactory::registerFactory(
          kShuffleName, std::make_unique<DummyShuffleInterfaceFactory>()));
}

TEST_F(ShuffleTest, shuffleReadRuntimeStats) {
  const size_t numPartitions = 1;
  const size_t numMapDrivers = 1;

  exec::Operator::registerOperator(std::make_unique<ShuffleWriteTranslator>());
  exec::Operator::registerOperator(std::make_unique<ShuffleReadTranslator>());
  velox::exec::ExchangeSource::factories().clear();
  registerExchangeSource(std::string(shuffleName_));

  const auto dataType = ROW({
      {"c0", INTEGER()},
      {"c1", BIGINT()},
  });

  VectorFuzzer::Options opts;
  opts.vectorSize = 100;
  opts.nullRatio = 0.1;
  opts.stringLength = 50;
  VectorFuzzer fuzzer(opts, pool_.get());

  struct {
    int numBatches;

    std::string debugString() const {
      return fmt::format("numBatches: {}", numBatches);
    }
  } testSettings[] = {{1}, {3}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(testData.numBatches);
    for (int i = 0; i < testData.numBatches; ++i) {
      inputVectors.push_back(fuzzer.fuzzInputRow(dataType));
    }

    auto shuffleInfo =
        localShuffleWriteInfo(tempDir_->getPath(), numPartitions);

    auto writerPlan =
        exec::test::PlanBuilder()
            .values(inputVectors, true)
            .addNode(addPartitionAndSerializeNode(numPartitions, false))
            .localPartition(std::vector<std::string>{})
            .addNode(addShuffleWriteNode(
                numPartitions, std::string(shuffleName_), shuffleInfo))
            .planNode();

    auto writerTaskId = makeTaskId("leaf", 0);
    auto writerTask =
        makeTask(writerTaskId, writerPlan, 0, core::QueryConfig({}));
    writerTask->start(numMapDrivers);

    ASSERT_TRUE(exec::test::waitForTaskCompletion(writerTask.get(), 5'000'000));

    auto plan = exec::test::PlanBuilder()
                    .addNode(addShuffleReadNode(dataType))
                    .project(dataType->names())
                    .planNode();

    exec::CursorParameters params;
    params.planNode = plan;
    params.destination = 0;
    params.maxDrivers = 1;

    auto [taskCursor, results] = runShuffleReadTask(
        params, localShuffleReadInfo(tempDir_->getPath(), 0));
    ASSERT_TRUE(
        exec::test::waitForTaskCompletion(taskCursor->task().get(), 5'000'000));

    const auto operatorStats =
        taskCursor->task()->taskStats().pipelineStats[0].operatorStats[0];
    const auto& runtimeStats = operatorStats.runtimeStats;

    ASSERT_EQ(runtimeStats.count(ShuffleRead::kShuffleDecodeTime), 1);
    const auto& decodeTimeStat =
        runtimeStats.at(ShuffleRead::kShuffleDecodeTime);
    ASSERT_GT(decodeTimeStat.count, 0);
    ASSERT_GT(decodeTimeStat.sum, 0);
    ASSERT_EQ(velox::RuntimeCounter::Unit::kNanos, decodeTimeStat.unit);

    ASSERT_EQ(runtimeStats.count(ShuffleRead::kShufflePagesPerInputBatch), 1);
    const auto& batchesPerReadStat =
        runtimeStats.at(ShuffleRead::kShufflePagesPerInputBatch);
    ASSERT_EQ(velox::RuntimeCounter::Unit::kNone, batchesPerReadStat.unit);
    ASSERT_GT(batchesPerReadStat.count, 0);
    ASSERT_GT(batchesPerReadStat.sum, 0);

    ASSERT_EQ(runtimeStats.count(ShuffleRead::kShuffleInputBatches), 1);
    const auto& numBatchesStat =
        runtimeStats.at(ShuffleRead::kShuffleInputBatches);
    ASSERT_GT(numBatchesStat.count, 0);
    ASSERT_GT(numBatchesStat.sum, 0);
    ASSERT_EQ(velox::RuntimeCounter::Unit::kNone, numBatchesStat.unit);
  }
}

TEST_F(ShuffleTest, shuffleEndToEnd) {
  struct TestConfig {
    std::string testName;
    DataType dataType;
    size_t numPartitions;
    size_t numMapDrivers;
    bool replicateNullsAndAny;
    bool sortedShuffle;
    bool withRowLimit;
    bool useCustomTempDir;
    DataGeneratorConfig dataGenConfig;

    std::string debugString() const {
      return fmt::format(
          "test:{}, partitions:{}, drivers:{}, replicate:{}, sorted:{}, rowLimit:{}, customDir:{}, numRows:{}, numBatches:{}",
          testName,
          numPartitions,
          numMapDrivers,
          replicateNullsAndAny,
          sortedShuffle,
          withRowLimit,
          useCustomTempDir,
          dataGenConfig.numRows,
          dataGenConfig.numBatches);
    }
  };

  const TestConfig testSettings[] = {
      {.testName = "endToEnd",
       .dataType = DataType::BASIC,
       .numPartitions = 5,
       .numMapDrivers = 2,
       .replicateNullsAndAny = false,
       .sortedShuffle = false,
       .withRowLimit = false,
       .useCustomTempDir = false,
       .dataGenConfig = {.numRows = 1000, .numBatches = 2, .seed = 1}},
      {.testName = "endToEndSmallData",
       .dataType = DataType::BASIC,
       .numPartitions = 3,
       .numMapDrivers = 1,
       .replicateNullsAndAny = false,
       .sortedShuffle = false,
       .withRowLimit = false,
       .useCustomTempDir = false,
       .dataGenConfig = {.numRows = 10, .numBatches = 1, .seed = 1}},
      {.testName = "endToEndLargeData",
       .dataType = DataType::BASIC,
       .numPartitions = 8,
       .numMapDrivers = 4,
       .replicateNullsAndAny = false,
       .sortedShuffle = false,
       .withRowLimit = false,
       .useCustomTempDir = false,
       .dataGenConfig = {.numRows = 10000, .numBatches = 5, .seed = 1}},
      {.testName = "endToEndWithSortedShuffle",
       .dataType = DataType::SORTED_BIGINT,
       .numPartitions = 2,
       .numMapDrivers = 1,
       .replicateNullsAndAny = false,
       .sortedShuffle = true,
       .withRowLimit = false,
       .useCustomTempDir = false,
       .dataGenConfig = {.numRows = 500, .numBatches = 2, .seed = 1}},
      {.testName = "endToEndWithSortedShuffleSmallData",
       .dataType = DataType::SORTED_BIGINT,
       .numPartitions = 2,
       .numMapDrivers = 1,
       .replicateNullsAndAny = false,
       .sortedShuffle = true,
       .withRowLimit = false,
       .useCustomTempDir = false,
       .dataGenConfig = {.numRows = 20, .numBatches = 1, .seed = 1}},
      {.testName = "endToEndWithSortedShuffleRowLimit",
       .dataType = DataType::SORTED_VARCHAR,
       .numPartitions = 2,
       .numMapDrivers = 1,
       .replicateNullsAndAny = false,
       .sortedShuffle = true,
       .withRowLimit = true,
       .useCustomTempDir = false,
       .dataGenConfig = {.numRows = 100, .numBatches = 1, .seed = 1}},
      {.testName = "endToEndWithReplicateNullAndAny",
       .dataType = DataType::BASIC_WITH_NULLS,
       .numPartitions = 9,
       .numMapDrivers = 2,
       .replicateNullsAndAny = true,
       .sortedShuffle = false,
       .withRowLimit = false,
       .useCustomTempDir = false,
       .dataGenConfig = {.numRows = 200, .numBatches = 3, .seed = 1}},
      {.testName = "localShuffle",
       .dataType = DataType::BASIC,
       .numPartitions = 1,
       .numMapDrivers = 1,
       .replicateNullsAndAny = false,
       .sortedShuffle = false,
       .withRowLimit = false,
       .useCustomTempDir = true,
       .dataGenConfig = {.numRows = 50, .numBatches = 1, .seed = 1}},
  };

  for (const auto& config : testSettings) {
    SCOPED_TRACE(config.debugString());

    std::shared_ptr<exec::test::TempDirectoryPath> customTempDir;
    std::string rootPath;
    if (config.useCustomTempDir) {
      customTempDir = exec::test::TempDirectoryPath::create();
      rootPath = customTempDir->getPath();
    } else {
      rootPath = tempDir_->getPath();
      cleanupDirectory(rootPath);
    }

    auto testData = generateTestData(config.dataGenConfig, config.dataType);
    auto& inputData = testData.inputData;
    auto& ordering = testData.ordering;
    auto& fields = testData.fields;
    auto& expectedSortingOrder = testData.expectedSortingOrder;
    auto& queryConfig = testData.queryConfig;

    velox::exec::ExchangeSource::factories().clear();
    const std::string shuffleName = config.useCustomTempDir
        ? std::string(LocalPersistentShuffleFactory::kShuffleName)
        : std::string(shuffleName_);
    const std::string shuffleInfo = localShuffleWriteInfo(
        rootPath, config.numPartitions, config.sortedShuffle);
    registerExchangeSource(shuffleName);

    runShuffleTest(
        shuffleName,
        shuffleInfo,
        [&](auto partition) {
          return localShuffleReadInfo(
              rootPath, partition, config.sortedShuffle);
        },
        config.replicateNullsAndAny,
        config.numPartitions,
        config.numMapDrivers,
        inputData,
        0,
        ordering,
        fields,
        expectedSortingOrder,
        queryConfig.has_value() ? std::move(queryConfig.value())
                                : core::QueryConfig({}));
    cleanupDirectory(rootPath);
  }
}

TEST_F(ShuffleTest, shuffleWriterReader) {
  const uint32_t numPartitions = 1;
  const uint32_t partition = 0;

  struct TestConfig {
    bool sortedShuffle;
    size_t maxBytesPerPartition;
    size_t numRows;
    uint64_t readMaxBytes;
    size_t minDataSize;
    size_t maxDataSize;
    std::optional<int> expectedOutputCalls;
    std::string debugString() const {
      return fmt::format(
          "sorted:{}, maxBytesPerPartition:{}, rows:{}, readMax:{}, dataSize:{}-{}, expectedCalls:{}",
          sortedShuffle,
          maxBytesPerPartition,
          numRows,
          readMaxBytes,
          minDataSize,
          maxDataSize,
          expectedOutputCalls.has_value() ? std::to_string(*expectedOutputCalls)
                                          : "None");
    }
  } testSettings[] = {
      // Sorted shuffle tests: verify data comes back in sorted order
      {.sortedShuffle = true,
       .maxBytesPerPartition = 1024,
       .numRows = 1,
       .readMaxBytes = 1024,
       .minDataSize = 10,
       .maxDataSize = 50},
      {.sortedShuffle = true,
       .maxBytesPerPartition = 1024,
       .numRows = 10,
       .readMaxBytes = 1024 * 1024,
       .minDataSize = 50,
       .maxDataSize = 200},
      {.sortedShuffle = true,
       .maxBytesPerPartition = 500,
       .numRows = 20,
       .readMaxBytes = 1024 * 1024,
       .minDataSize = 50,
       .maxDataSize = 150},
      {.sortedShuffle = true,
       .maxBytesPerPartition = 1024,
       .numRows = 50,
       .readMaxBytes = 8192,
       .minDataSize = 100,
       .maxDataSize = 400},
      {.sortedShuffle = true,
       .maxBytesPerPartition = 2048,
       .numRows = 100,
       .readMaxBytes = 1024 * 1024,
       .minDataSize = 200,
       .maxDataSize = 1000},
      // Sorted shuffle batching tests: verify correct number of output batches
      {.sortedShuffle = true,
       .maxBytesPerPartition = 1,
       .numRows = 20,
       .readMaxBytes = 1,
       .minDataSize = 64,
       .maxDataSize = 64,
       .expectedOutputCalls = 20},
      {.sortedShuffle = true,
       .maxBytesPerPartition = 1,
       .numRows = 20,
       .readMaxBytes = 100,
       .minDataSize = 64,
       .maxDataSize = 64,
       .expectedOutputCalls = 20},
      {.sortedShuffle = true,
       .maxBytesPerPartition = 1,
       .numRows = 20,
       .readMaxBytes = 1 << 25,
       .minDataSize = 64,
       .maxDataSize = 64,
       .expectedOutputCalls = 1},
      // Unsorted shuffle batching tests: verify correct number of output
      // batches
      {.sortedShuffle = false,
       .maxBytesPerPartition = 1,
       .numRows = 20,
       .readMaxBytes = 1,
       .minDataSize = 64,
       .maxDataSize = 64,
       .expectedOutputCalls = 20},
      {.sortedShuffle = false,
       .maxBytesPerPartition = 1,
       .numRows = 20,
       .readMaxBytes = 100,
       .minDataSize = 64,
       .maxDataSize = 64,
       .expectedOutputCalls = 20},
      {.sortedShuffle = false,
       .maxBytesPerPartition = 1,
       .numRows = 20,
       .readMaxBytes = 1 << 25,
       .minDataSize = 64,
       .maxDataSize = 64,
       .expectedOutputCalls = 1},
  };

  for (const auto& config : testSettings) {
    SCOPED_TRACE(config.debugString());

    auto tempRootDir = velox::exec::test::TempDirectoryPath::create();
    const auto testRootPath = tempRootDir->getPath();

    LocalShuffleWriteInfo writeInfo = LocalShuffleWriteInfo::deserialize(
        localShuffleWriteInfo(testRootPath, numPartitions));

    auto writer = std::make_shared<LocalShuffleWriter>(
        writeInfo.rootPath,
        writeInfo.queryId,
        writeInfo.shuffleId,
        writeInfo.numPartitions,
        config.maxBytesPerPartition,
        config.sortedShuffle,
        pool());

    std::vector<int32_t> keys;
    std::vector<std::string> dataValues;
    keys.reserve(config.numRows);
    dataValues.reserve(config.numRows);

    if (config.sortedShuffle) {
      // For sorted shuffle, generate random keys and include index markers for
      // verification
      folly::Random::DefaultGenerator rng;
      rng.seed(1);
      for (size_t i = 0; i < config.numRows; ++i) {
        keys.push_back(static_cast<int32_t>(folly::Random::rand32(rng)));
        const size_t sizeRange = config.maxDataSize - config.minDataSize;
        const size_t dataSize = config.minDataSize +
            (sizeRange > 0 ? folly::Random::rand32(rng) % sizeRange : 0);
        std::string data(dataSize, static_cast<char>('a' + (i % 26)));
        data.append(fmt::format("_idx{:04d}", i));
        dataValues.push_back(std::move(data));
      }
    } else {
      // For unsorted shuffle, use sequential keys
      for (size_t i = 0; i < config.numRows; ++i) {
        keys.push_back(i);
        dataValues.push_back(std::string(config.minDataSize, 'a' + i % 26));
      }
    }

    for (size_t i = 0; i < keys.size(); ++i) {
      if (config.sortedShuffle) {
        int32_t keyBigEndian = folly::Endian::big(keys[i]);
        std::string_view keyBytes(
            reinterpret_cast<const char*>(&keyBigEndian), kUint32Size);
        writer->collect(partition, keyBytes, dataValues[i]);
      } else {
        writer->collect(partition, std::string_view{}, dataValues[i]);
      }
    }
    writer->noMoreData(true);

    LocalShuffleReadInfo readInfo = LocalShuffleReadInfo::deserialize(
        localShuffleReadInfo(testRootPath, partition, config.sortedShuffle));

    auto reader = std::make_shared<LocalShuffleReader>(
        readInfo.rootPath,
        readInfo.queryId,
        readInfo.partitionIds,
        config.sortedShuffle,
        pool());
    reader->initialize();

    size_t totalRows = 0;
    int numOutputCalls = 0;
    std::vector<std::string> readDataValues;

    while (true) {
      auto batches = reader->next(config.readMaxBytes)
                         .via(folly::getGlobalCPUExecutor())
                         .get();
      if (batches.empty()) {
        break;
      }

      ++numOutputCalls;
      for (const auto& batch : batches) {
        for (const auto& row : batch->rows()) {
          readDataValues.emplace_back(row.data(), row.size());
          ++totalRows;
        }
      }
    }

    EXPECT_EQ(config.numRows, totalRows);

    if (config.sortedShuffle) {
      // Verify data came back in sorted order
      std::vector<std::string> sortKeys;
      sortKeys.reserve(keys.size());
      for (const auto& key : keys) {
        int32_t keyBigEndian = folly::Endian::big(key);
        sortKeys.emplace_back(
            reinterpret_cast<const char*>(&keyBigEndian), sizeof(int32_t));
      }
      auto sortedOrder = getSortOrder(sortKeys);

      for (size_t i = 0; i < readDataValues.size(); ++i) {
        const std::string& dataValue = readDataValues[i];
        size_t idxPos = dataValue.find("_idx");
        ASSERT_NE(idxPos, std::string::npos)
            << "Data value at position " << i << " missing '_idx' marker: '"
            << dataValue << "'";
        size_t originalIdx = std::stoul(dataValue.substr(idxPos + 4));
        EXPECT_EQ(originalIdx, sortedOrder[i])
            << "Data at position " << i << " should correspond to key at index "
            << sortedOrder[i] << " but corresponds to index " << originalIdx;
      }
    }

    if (config.expectedOutputCalls.has_value()) {
      EXPECT_EQ(numOutputCalls, config.expectedOutputCalls.value())
          << "Expected " << config.expectedOutputCalls.value()
          << " output calls but got " << numOutputCalls
          << " (readMaxBytes=" << config.readMaxBytes << ")";
    }

    reader->noMoreData(true);
    cleanupDirectory(testRootPath);
  }
}

TEST_F(ShuffleTest, shuffleFuzzTest) {
  fuzzerTest(false, 1);
  fuzzerTest(false, 3);
  fuzzerTest(false, 7);

  // Test With ReplicateNullsAndAny
  fuzzerTest(true, 1);
  fuzzerTest(true, 3);
  fuzzerTest(true, 7);
}

TEST_F(ShuffleTest, shuffleWriterToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .localPartition(std::vector<std::string>{})
                  .addNode(addShuffleWriteNode(
                      4,
                      std::string(shuffleName_),
                      localShuffleWriteInfo(tempDir_->getPath(), 10)))
                  .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- ShuffleWrite[3]\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- ShuffleWrite[3][4, local]"
      " -> partition:INTEGER, key:VARBINARY, data:VARBINARY\n");
}

TEST_F(ShuffleTest, partitionAndSerializeToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addPartitionAndSerializeNode(4, false))
                  .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize[1]\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- PartitionAndSerialize[1][(c0) 4 HASH(c0) ROW<c0:INTEGER,c1:BIGINT>]"
      " -> partition:INTEGER, key:VARBINARY, data:VARBINARY\n");

  plan = exec::test::PlanBuilder()
             .values({data}, true)
             .addNode(addPartitionAndSerializeNode(4, true))
             .planNode();

  ASSERT_EQ(plan->toString(false, false), "-- PartitionAndSerialize[1]\n");
  ASSERT_EQ(
      plan->toString(true, false),
      "-- PartitionAndSerialize[1][(c0) 4 HASH(c0) ROW<c0:INTEGER,c1:BIGINT>]"
      " -> partition:INTEGER, key:VARBINARY, data:VARBINARY, replicate:BOOLEAN\n");
}
} // namespace facebook::presto::operators::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv};
  return RUN_ALL_TESTS();
}
