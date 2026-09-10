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
#include <gtest/gtest.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/tests/MutableConfigs.h"
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/connectors/HivePrestoToVeloxConnector.h"
#include "presto_cpp/main/operators/LocalShuffle.h"
#include "presto_cpp/main/operators/MaterializedExchange.h"
#include "presto_cpp/main/operators/MaterializedOutput.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/properties/session/SessionProperties.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/exec/DefaultOutputBufferManager.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/exec/InMemoryExchangeClient.h"
#include "velox/exec/OutputTransportRegistry.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace {

core::PlanFragment assertToVeloxFragment(
    const std::string& fileName,
    memory::MemoryPool* pool = nullptr) {
  std::string fragment = slurp(test::utils::getDataPath(fileName));

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

json parseFragmentJson(const std::string& fileName) {
  return json::parse(slurp(test::utils::getDataPath(fileName)));
}

// Converts an interactive plan fragment from already parsed JSON, so that a
// test can annotate transport types on the fragment before conversion, and with
// 'queryConfigs' as the query's config, so that it can set session properties.
core::PlanFragment toVeloxFragment(
    const json& fragmentJson,
    std::unordered_map<std::string, std::string> queryConfigs = {}) {
  const protocol::PlanFragment prestoPlan = fragmentJson;
  auto pool = memory::deprecatedAddDefaultLeafMemoryPool();
  auto queryCtx = core::QueryCtx::create(
      nullptr, core::QueryConfig{std::move(queryConfigs)});
  VeloxInteractiveQueryPlanConverter converter(queryCtx.get(), pool.get());
  return converter.toVeloxQueryPlan(
      prestoPlan, nullptr, "20201107_130540_00011_wrpkw.1.2.3");
}

// Annotates every RemoteSourceNode found in 'node' with 'transportType', the
// way the coordinator annotates the input edges of a fragment.
void annotateRemoteSources(json& node, const std::string& transportType) {
  if (node.is_object()) {
    const auto type = node.find("@type");
    if (type != node.end() && type->is_string() &&
        type->get<std::string>().find("RemoteSourceNode") !=
            std::string::npos) {
      node["transportType"] = transportType;
    }
    for (auto& element : node.items()) {
      annotateRemoteSources(element.value(), transportType);
    }
  } else if (node.is_array()) {
    for (auto& element : node) {
      annotateRemoteSources(element, transportType);
    }
  }
}

// Returns the first ExchangeNode found in the plan rooted at 'node', or nullptr
// when the plan has none.
const core::ExchangeNode* findExchangeNode(const core::PlanNodePtr& node) {
  if (const auto* exchange =
          dynamic_cast<const core::ExchangeNode*>(node.get())) {
    return exchange;
  }
  for (const auto& source : node->sources()) {
    if (const auto* found = findExchangeNode(source)) {
      return found;
    }
  }
  return nullptr;
}

// Registers a transport under the UCX id in both registries for as long as it
// is in scope, standing in for what registerCudf() does on a worker built with
// the cuDF UCX exchange. Converting a plan only asks whether the id resolves,
// never builds an operator from it, so the built-in in-memory entries stand in
// for the UCX ones and no cuDF or UCX dependency is needed here.
class ScopedUcxTransports {
 public:
  ScopedUcxTransports() {
    exec::OutputTransportRegistry::global().insert(
        kUcx_,
        exec::DefaultOutputBufferManager::makeDefaultTransportEntry(),
        /*overwrite=*/true);
    exec::ExchangeTransportRegistry::global().insert(
        kUcx_,
        exec::InMemoryExchangeClient::makeDefaultTransportEntry(),
        /*overwrite=*/true);
  }

  ~ScopedUcxTransports() {
    exec::OutputTransportRegistry::global().erase(kUcx_);
    exec::ExchangeTransportRegistry::global().erase(kUcx_);
  }

 private:
  const std::string kUcx_{core::TransportKind::kUcx};
};

std::shared_ptr<const core::PlanNode> assertToBatchVeloxQueryPlan(
    const std::string& fileName,
    const std::string& shuffleName,
    std::shared_ptr<std::string>&& serializedShuffleWriteInfo,
    std::shared_ptr<std::string>&& broadcastBasePath) {
  const std::string fragment = slurp(test::utils::getDataPath(fileName));

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
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    registerPrestoToVeloxConnector(
        std::make_unique<HivePrestoToVeloxConnector>("hive"));
    registerPrestoToVeloxConnector(
        std::make_unique<HivePrestoToVeloxConnector>("hive-plus"));
    operators::ShuffleInterfaceFactory::registerFactory(
        std::string(operators::LocalPersistentShuffleFactory::kShuffleName),
        std::make_unique<operators::LocalPersistentShuffleFactory>());
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
      "HASH(\"{cluster_label_v2}\", expr_181)");
  auto keys = partitionedOutput->keys();
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0]->toString(), "{cluster_label_v2}");
  ASSERT_EQ(keys[1]->toString(), "\"expr_181\"");
  ASSERT_EQ(partitionedOutput->serdeKind(), "CompactRow");
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

// A coordinator that annotates no transport at all leaves both ends of the
// exchange edge on the default in-memory transport.
TEST_F(PlanConverterTest, transportTypeAbsentDefaultsToInMemory) {
  auto fragmentJson = parseFragmentJson("FinalAgg.json");
  ASSERT_FALSE(fragmentJson.count("outputTransportType"));
  ASSERT_FALSE(
      fragmentJson["root"]["source"]["sources"][0].count("transportType"));

  const auto fragment = toVeloxFragment(fragmentJson);

  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kInMemory);

  const auto* exchange = findExchangeNode(fragment.planNode);
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(exchange->transportKind(), core::TransportKind::kInMemory);
}

// HTTP maps to the in-memory transport: the producer buffers its output in
// memory and the consumer drains it over HTTP.
TEST_F(PlanConverterTest, transportTypeHttp) {
  ScopedUcxTransports ucxTransports;

  auto fragmentJson = parseFragmentJson("FinalAgg.json");
  fragmentJson["outputTransportType"] = "HTTP";
  annotateRemoteSources(fragmentJson["root"], "HTTP");

  const auto fragment = toVeloxFragment(fragmentJson);

  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kInMemory);

  const auto* exchange = findExchangeNode(fragment.planNode);
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(exchange->transportKind(), core::TransportKind::kInMemory);
}

// ANY on a worker with the UCX transport registered, with the session property
// unset: the property's default is this worker's capability, so the edge uses
// UCX.
TEST_F(PlanConverterTest, transportTypeAnyWithUcxRegistered) {
  ScopedUcxTransports ucxTransports;

  auto fragmentJson = parseFragmentJson("FinalAgg.json");
  fragmentJson["outputTransportType"] = "ANY";
  annotateRemoteSources(fragmentJson["root"], "ANY");

  const auto fragment = toVeloxFragment(fragmentJson);

  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kUcx);

  const auto* exchange = findExchangeNode(fragment.planNode);
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(exchange->transportKind(), core::TransportKind::kUcx);
}

// ANY on a worker that has no UCX transport, with the session property unset:
// the same default resolves the other way, and the query runs over the
// in-memory transport instead of failing.
TEST_F(PlanConverterTest, transportTypeAnyWithoutUcxRegistered) {
  auto fragmentJson = parseFragmentJson("FinalAgg.json");
  fragmentJson["outputTransportType"] = "ANY";
  annotateRemoteSources(fragmentJson["root"], "ANY");

  const auto fragment = toVeloxFragment(fragmentJson);

  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kInMemory);

  const auto* exchange = findExchangeNode(fragment.planNode);
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(exchange->transportKind(), core::TransportKind::kInMemory);
}

// The session property set false keeps the query on the in-memory transport
// even where UCX is registered and the coordinator allows it.
TEST_F(PlanConverterTest, transportTypeAnyDisabledBySessionProperty) {
  ScopedUcxTransports ucxTransports;

  auto fragmentJson = parseFragmentJson("FinalAgg.json");
  fragmentJson["outputTransportType"] = "ANY";
  annotateRemoteSources(fragmentJson["root"], "ANY");

  const auto fragment = toVeloxFragment(
      fragmentJson, {{SessionProperties::kCudfExchangeEnabledConfig, "false"}});

  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kInMemory);

  const auto* exchange = findExchangeNode(fragment.planNode);
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(exchange->transportKind(), core::TransportKind::kInMemory);
}

// The session property set true where the transport is not registered fails the
// query, rather than silently running over the in-memory transport. Explicitly
// asking for UCX and getting HTTP performance without being told is worse than
// a failure that names the switch.
TEST_F(PlanConverterTest, transportTypeAnyEnabledWithoutUcxFails) {
  auto fragmentJson = parseFragmentJson("FinalAgg.json");
  fragmentJson["outputTransportType"] = "ANY";
  annotateRemoteSources(fragmentJson["root"], "ANY");

  VELOX_ASSERT_USER_THROW(
      toVeloxFragment(
          fragmentJson,
          {{SessionProperties::kCudfExchangeEnabledConfig, "true"}}),
      "the cuDF UCX exchange is disabled on this worker");
}

// The same property set true resolves to UCX where the transport is registered.
TEST_F(PlanConverterTest, transportTypeAnyEnabledWithUcxRegistered) {
  ScopedUcxTransports ucxTransports;

  auto fragmentJson = parseFragmentJson("FinalAgg.json");
  fragmentJson["outputTransportType"] = "ANY";
  annotateRemoteSources(fragmentJson["root"], "ANY");

  const auto fragment = toVeloxFragment(
      fragmentJson, {{SessionProperties::kCudfExchangeEnabledConfig, "true"}});

  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kUcx);

  const auto* exchange = findExchangeNode(fragment.planNode);
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(exchange->transportKind(), core::TransportKind::kUcx);
}

// A sorted remote source becomes a MergeExchangeNode, which carries the
// transport just like a plain ExchangeNode. The fragment's own output goes to
// the coordinator, which speaks only HTTP, so that edge stays in-memory even
// though the fragment is annotated ANY.
TEST_F(PlanConverterTest, transportTypeMergeExchange) {
  ScopedUcxTransports ucxTransports;

  auto fragmentJson = parseFragmentJson("OffsetLimit.json");
  fragmentJson["outputTransportType"] = "ANY";
  annotateRemoteSources(fragmentJson["root"], "ANY");

  const auto fragment = toVeloxFragment(fragmentJson);

  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(fragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kInMemory);

  const auto* exchange = findExchangeNode(fragment.planNode);
  ASSERT_NE(exchange, nullptr);
  ASSERT_NE(dynamic_cast<const core::MergeExchangeNode*>(exchange), nullptr);
  ASSERT_EQ(exchange->transportKind(), core::TransportKind::kUcx);
}

// An exchange edge spans two fragments: the producer's PartitionedOutputNode
// and the consumer's ExchangeNode. Velox resolves each end independently from
// its own node, so the two must name the same transport. Both ends reach that
// answer from the same annotation, the same session property and the same
// registries, which is what keeps them in step.
TEST_F(PlanConverterTest, transportTypeEdgeAgreement) {
  ScopedUcxTransports ucxTransports;

  // Producer fragment of the edge.
  auto producerJson = parseFragmentJson("ScanAgg.json");
  producerJson["outputTransportType"] = "ANY";
  const auto producerFragment = toVeloxFragment(producerJson);
  const auto* partitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(
          producerFragment.planNode.get());
  ASSERT_NE(partitionedOutput, nullptr);

  // Consumer fragment of the same edge, annotated with the same value.
  auto consumerJson = parseFragmentJson("FinalAgg.json");
  annotateRemoteSources(consumerJson["root"], "ANY");
  const auto consumerFragment = toVeloxFragment(consumerJson);
  const auto* exchange = findExchangeNode(consumerFragment.planNode);
  ASSERT_NE(exchange, nullptr);

  ASSERT_EQ(partitionedOutput->transportKind(), exchange->transportKind());
  ASSERT_EQ(partitionedOutput->transportKind(), core::TransportKind::kUcx);

  // The same edge with the query opted out agrees on the in-memory transport.
  const std::unordered_map<std::string, std::string> disabled{
      {SessionProperties::kCudfExchangeEnabledConfig, "false"}};
  const auto disabledProducer = toVeloxFragment(producerJson, disabled);
  const auto* disabledPartitionedOutput =
      dynamic_cast<const core::PartitionedOutputNode*>(
          disabledProducer.planNode.get());
  ASSERT_NE(disabledPartitionedOutput, nullptr);
  const auto disabledConsumer = toVeloxFragment(consumerJson, disabled);
  const auto* disabledExchange = findExchangeNode(disabledConsumer.planNode);
  ASSERT_NE(disabledExchange, nullptr);

  ASSERT_EQ(
      disabledPartitionedOutput->transportKind(),
      disabledExchange->transportKind());
  ASSERT_EQ(
      disabledPartitionedOutput->transportKind(),
      core::TransportKind::kInMemory);
}

// IndexSourceNode is converted to a TableScanNode with the same output type
// and column assignments. The default toVeloxTableHandle(IndexHandle) overload
// delegates to toVeloxTableHandle, producing a standard HiveTableHandle.
TEST_F(PlanConverterTest, indexSource) {
  auto plan = assertToVeloxQueryPlan("IndexSource.json");
  ASSERT_NE(plan, nullptr);

  // OutputNode wraps the converted IndexSourceNode.
  auto* tableScan =
      dynamic_cast<const core::TableScanNode*>(plan->sources()[0].get());
  ASSERT_NE(tableScan, nullptr);
  ASSERT_EQ(tableScan->id(), "0");

  // Verify output type has the expected columns.
  auto outputType = tableScan->outputType();
  ASSERT_EQ(outputType->size(), 2);
  ASSERT_EQ(outputType->nameOf(0), "nationkey");
  ASSERT_EQ(outputType->nameOf(1), "name");

  // Verify assignments.
  ASSERT_EQ(tableScan->assignments().size(), 2);
  ASSERT_NE(
      tableScan->assignments().find("nationkey"),
      tableScan->assignments().end());
  ASSERT_NE(
      tableScan->assignments().find("name"), tableScan->assignments().end());

  // Verify the table handle is a HiveTableHandle.
  auto* tableHandle = dynamic_cast<const connector::hive::HiveTableHandle*>(
      tableScan->tableHandle().get());
  ASSERT_NE(tableHandle, nullptr);
  ASSERT_EQ(tableHandle->tableName(), "tpch.nation");
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

TEST_F(PlanConverterTest, batchPlanConversionExchangeWrite) {
  filesystems::registerLocalFileSystem();
  facebook::presto::test::setupMutableSystemConfig();
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kExchangeMaterializationEnabled), "true");
  auto root = assertToBatchVeloxQueryPlan(
      "ScanAggBatch.json",
      std::string(operators::LocalPersistentShuffleFactory::kShuffleName),
      std::make_shared<std::string>(fmt::format(
          "{{\n"
          "  \"rootPath\": \"{}\",\n"
          "  \"numPartitions\": {},\n"
          "  \"queryId\": \"test_query\",\n"
          "  \"shuffleId\": 0\n"
          "}}",
          exec::test::TempDirectoryPath::create()->getPath(),
          10)),
      std::make_shared<std::string>("/tmp"));

  auto materializedOutput =
      std::dynamic_pointer_cast<const operators::MaterializedOutputNode>(root);
  ASSERT_NE(materializedOutput, nullptr);
  ASSERT_EQ(materializedOutput->sources().size(), 1);

  auto curNode = assertToBatchVeloxQueryPlan(
      "FinalAgg.json",
      std::string(operators::LocalPersistentShuffleFactory::kShuffleName),
      nullptr,
      std::make_shared<std::string>("/tmp"));

  std::shared_ptr<const operators::MaterializedExchangeNode>
      materializedExchangeNode;
  while (!curNode->sources().empty()) {
    curNode = curNode->sources().back();
  }
  materializedExchangeNode =
      std::dynamic_pointer_cast<const operators::MaterializedExchangeNode>(
          curNode);
  ASSERT_NE(materializedExchangeNode, nullptr);
}
