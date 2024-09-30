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
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/types/PrestoToVeloxConnector.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace fs = boost::filesystem;

using namespace facebook::presto;
using namespace facebook::velox;

namespace {
std::string getDataPath(const std::string& fileName) {
  std::string currentPath = fs::current_path().c_str();

  if (boost::algorithm::ends_with(currentPath, "fbcode")) {
    return currentPath +
        "/github/presto-trunk/presto-native-execution/presto_cpp/main/types/tests/data/" +
        fileName;
  }

  if (boost::algorithm::ends_with(currentPath, "fbsource")) {
    return currentPath + "/third-party/presto_cpp/main/types/tests/data/" +
        fileName;
  }

  // CLion runs the tests from cmake-build-release/ or cmake-build-debug/
  // directory. Hard-coded json files are not copied there and test fails with
  // file not found. Fixing the path so that we can trigger these tests from
  // CLion.
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");

  return currentPath + "/data/" + fileName;
}

core::PlanFragment assertToVeloxFragment(
    const std::string& fileName,
    memory::MemoryPool* pool = nullptr) {
  std::string fragment = slurp(getDataPath(fileName));

  protocol::PlanFragment prestoPlan = json::parse(fragment);
  std::shared_ptr<memory::MemoryPool> poolPtr;
  if (pool == nullptr) {
    poolPtr = memory::deprecatedAddDefaultLeafMemoryPool();
    pool = poolPtr.get();
  }

  auto queryCtx = core::QueryCtx::create();
  VeloxInteractiveQueryPlanConverter converter(queryCtx.get(), pool);
  return converter.toVeloxQueryPlan(
      prestoPlan, nullptr, "20201107_130540_00011_wrpkw.1.2.3");
}

std::shared_ptr<const core::PlanNode> assertToVeloxQueryPlan(
    const std::string& fileName,
    memory::MemoryPool* pool = nullptr) {
  return assertToVeloxFragment(fileName, pool).planNode;
}

std::shared_ptr<const core::PlanNode> assertToBatchVeloxQueryPlan(
    const std::string& fileName,
    const std::string& shuffleName,
    std::shared_ptr<std::string>&& serializedShuffleWriteInfo,
    std::shared_ptr<std::string>&& broadcastBasePath) {
  const std::string fragment = slurp(getDataPath(fileName));

  protocol::PlanFragment prestoPlan = json::parse(fragment);
  auto pool = memory::deprecatedAddDefaultLeafMemoryPool();
  auto queryCtx = core::QueryCtx::create();
  VeloxBatchQueryPlanConverter converter(
      shuffleName,
      std::move(serializedShuffleWriteInfo),
      std::move(broadcastBasePath),
      queryCtx.get(),
      pool.get());
  return converter
      .toVeloxQueryPlan(
          prestoPlan, nullptr, "20201107_130540_00011_wrpkw.1.2.3")
      .planNode;
}
} // namespace

class PlanConverterTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    registerPrestoToVeloxConnector(
        std::make_unique<HivePrestoToVeloxConnector>("hive"));
    registerPrestoToVeloxConnector(
        std::make_unique<HivePrestoToVeloxConnector>("hive-plus"));
  }

  void TearDown() override {
    unregisterPrestoToVeloxConnector("hive");
    unregisterPrestoToVeloxConnector("hive-plus");
  }
};

// Leaf stage plan for select regionkey, sum(1) from nation group by 1
// Scan + Partial Agg + Repartitioning
TEST_F(PlanConverterTest, scanAgg) {
  auto partitionedOutput = assertToVeloxQueryPlan("ScanAgg.json");
  auto* tableScan = dynamic_cast<const core::TableScanNode*>(
      partitionedOutput->sources()[0]->sources()[0]->sources()[0].get());
  ASSERT_TRUE(tableScan != nullptr);
  auto* columnHandle = dynamic_cast<const connector::hive::HiveColumnHandle*>(
      tableScan->assignments().at("complex_type").get());
  ASSERT_TRUE(columnHandle != nullptr);
  auto& requiredSubfields = columnHandle->requiredSubfields();
  ASSERT_EQ(requiredSubfields.size(), 2);
  ASSERT_EQ(requiredSubfields[0].toString(), "complex_type[1][\"foo\"].id");
  ASSERT_EQ(requiredSubfields[1].toString(), "complex_type[2][\"bar\"].id");

  auto* tableHandle = dynamic_cast<const connector::hive::HiveTableHandle*>(
      tableScan->tableHandle().get());
  ASSERT_TRUE(tableHandle);
  ASSERT_EQ(
      tableHandle->dataColumns()->toString(),
      "ROW<nationkey:BIGINT,name:VARCHAR,regionkey:BIGINT,complex_type:ARRAY<MAP<VARCHAR,ROW<id:BIGINT,description:VARCHAR>>>,comment:VARCHAR>");
  ASSERT_TRUE(tableHandle->remainingFilter());
  ASSERT_EQ(
      tableHandle->remainingFilter()->toString(),
      "presto.default.lt(presto.default.rand(),0.0001)");

  auto tableParameters = tableHandle->tableParameters();
  ASSERT_EQ(tableParameters.size(), 6);
  ASSERT_EQ(tableParameters.find("presto_version")->second, "testversion");
  ASSERT_EQ(tableParameters.find("numRows")->second, "25");
  ASSERT_EQ(tableParameters.find("totalSize")->second, "1451");
  ASSERT_EQ(tableParameters.find("foobar"), tableParameters.end());

  assertToVeloxQueryPlan("ScanAggCustomConnectorId.json");
}

// Partitioned output with partitioned scheme over const key and a variable.
TEST_F(PlanConverterTest, partitionedOutput) {
  std::shared_ptr<memory::MemoryPool> poolPtr =
      memory::deprecatedAddDefaultLeafMemoryPool();
  core::PlanFragment fragment =
      assertToVeloxFragment("PartitionedOutput.json", poolPtr.get());
  auto partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());

  // Test fragment's partitioning scheme.
  ASSERT_EQ(
      partitionedOutput->partitionFunctionSpec().toString(),
      "HASH(\"1 elements starting at 0 {cluster_label_v2}\", expr_181)");
  auto keys = partitionedOutput->keys();
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0]->toString(), "1 elements starting at 0 {cluster_label_v2}");
  ASSERT_EQ(keys[1]->toString(), "\"expr_181\"");
}

// Final Agg stage plan for select regionkey, sum(1) from nation group by 1
TEST_F(PlanConverterTest, finalAgg) {
  assertToVeloxQueryPlan("FinalAgg.json");
}

// Last stage (output) plan for select regionkey, sum(1) from nation group by 1
TEST_F(PlanConverterTest, output) {
  assertToVeloxQueryPlan("Output.json");
}

// Last stage plan for SELECT * FROM nation ORDER BY nationkey OFFSET 7 LIMIT 5.
TEST_F(PlanConverterTest, offsetLimit) {
  auto plan = assertToVeloxQueryPlan("OffsetLimit.json");

  // Look for Limit(offset = 7, count = 5) node
  bool foundLimit = false;
  auto node = plan;
  while (node) {
    node = node->sources()[0];
    if (auto limit = std::dynamic_pointer_cast<const core::LimitNode>(node)) {
      ASSERT_EQ(7, limit->offset());
      ASSERT_EQ(5, limit->count());
      foundLimit = true;
      break;
    }
  }

  ASSERT_TRUE(foundLimit);
}

TEST_F(PlanConverterTest, batchPlanConversion) {
  filesystems::registerLocalFileSystem();
  auto root = assertToBatchVeloxQueryPlan(
      "ScanAggBatch.json",
      std::string(operators::LocalPersistentShuffleFactory::kShuffleName),
      std::make_shared<std::string>(fmt::format(
          "{{\n"
          "  \"rootPath\": \"{}\",\n"
          "  \"numPartitions\": {}\n"
          "}}",
          exec::test::TempDirectoryPath::create()->getPath(),
          10)),
      std::make_shared<std::string>("/tmp"));

  auto shuffleWrite =
      std::dynamic_pointer_cast<const operators::ShuffleWriteNode>(root);
  ASSERT_NE(shuffleWrite, nullptr);
  ASSERT_EQ(shuffleWrite->sources().size(), 1);

  auto localPartition =
      std::dynamic_pointer_cast<const core::LocalPartitionNode>(
          shuffleWrite->sources().back());
  ASSERT_NE(localPartition, nullptr);
  ASSERT_EQ(localPartition->sources().size(), 1);

  auto partitionAndSerializeNode =
      std::dynamic_pointer_cast<const operators::PartitionAndSerializeNode>(
          localPartition->sources().back());
  ASSERT_NE(partitionAndSerializeNode, nullptr);
  ASSERT_EQ(partitionAndSerializeNode->numPartitions(), 3);

  auto curNode = assertToBatchVeloxQueryPlan(
      "FinalAgg.json",
      std::string(operators::LocalPersistentShuffleFactory::kShuffleName),
      nullptr,
      std::make_shared<std::string>("/tmp"));

  std::shared_ptr<const operators::ShuffleReadNode> shuffleReadNode;
  while (!curNode->sources().empty()) {
    curNode = curNode->sources().back();
  }
  shuffleReadNode =
      std::dynamic_pointer_cast<const operators::ShuffleReadNode>(curNode);
  ASSERT_NE(shuffleReadNode, nullptr);
}
