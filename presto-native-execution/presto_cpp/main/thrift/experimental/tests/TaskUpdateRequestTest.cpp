// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "presto_cpp/main/thrift/ThriftUtils.h"
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"

using namespace facebook;
using namespace facebook::presto::protocol;

class TaskUpdateRequestTest : public ::testing::Test {};

const std::string BASE_DATA_PATH = "/github/presto-trunk/presto-native-execution/presto_cpp/main/thrift/tests/data/";

TEST_F(TaskUpdateRequestTest, connectorId) {
  ConnectorId connectorId;
  cpp2::ConnectorId thriftConnectorId;
  thriftConnectorId.catalogName_ref() = "test";
  fromThrift(thriftConnectorId, connectorId);
  ASSERT_EQ(connectorId, "test");
}

TEST_F(TaskUpdateRequestTest, optionalField) {
  ResourceEstimates resourceEstimates;
  cpp2::ResourceEstimates thriftResourceEstimates;
  thriftResourceEstimates.executionTime_ref() = 100;
  thriftResourceEstimates.peakMemory_ref() = 1024 * 1024 * 1024;
  fromThrift(thriftResourceEstimates, resourceEstimates);
  ASSERT_EQ(*resourceEstimates.executionTime, Duration(100, TimeUnit::MILLISECONDS));
  ASSERT_EQ(resourceEstimates.cpuTime, nullptr);
  ASSERT_EQ(*resourceEstimates.peakMemory, DataSize(1024 * 1024 * 1024, DataUnit::BYTE));
  ASSERT_EQ(resourceEstimates.peakTaskMemory, nullptr);
}

TEST_F(TaskUpdateRequestTest, qualifiedObjectName) {
  QualifiedObjectName qualifiedObjectName;
  cpp2::QualifiedObjectName thriftQualifiedObjectName;
  thriftQualifiedObjectName.catalogName_ref() = "test_catalog";
  thriftQualifiedObjectName.schemaName_ref() = "test_schema";
  thriftQualifiedObjectName.objectName_ref() = "test_object";
  fromThrift(thriftQualifiedObjectName, qualifiedObjectName);
  ASSERT_EQ(qualifiedObjectName, "test_catalog.test_schema.test_object");
}

TEST_F(TaskUpdateRequestTest, routineCharacteristics) {
  RoutineCharacteristics routineCharacteristics;
  cpp2::RoutineCharacteristics thriftRroutineCharacteristics;
  cpp2::Language thriftLanguage;
  thriftLanguage.language_ref() = "English";
  thriftRroutineCharacteristics.language_ref() = std::move(thriftLanguage);
  thriftRroutineCharacteristics.determinism_ref() = cpp2::Determinism::NOT_DETERMINISTIC;
  thriftRroutineCharacteristics.nullCallClause_ref() = cpp2::NullCallClause::RETURNS_NULL_ON_NULL_INPUT;
  fromThrift(thriftRroutineCharacteristics, routineCharacteristics);
  ASSERT_EQ((*routineCharacteristics.language).language, "English");
  ASSERT_EQ(*routineCharacteristics.determinism, Determinism::NOT_DETERMINISTIC);
  ASSERT_EQ(*routineCharacteristics.nullCallClause, NullCallClause::RETURNS_NULL_ON_NULL_INPUT);
}

TEST_F(TaskUpdateRequestTest, mapOutputBuffers) {
  OutputBuffers outputBuffers;
  cpp2::OutputBuffers thriftOutputBuffers;
  thriftOutputBuffers.type_ref() = cpp2::BufferType::ARBITRARY;
  thriftOutputBuffers.version_ref() = 1;
  thriftOutputBuffers.noMoreBufferIds_ref() = true;
  cpp2::OutputBufferId outputBufferId1;
  cpp2::OutputBufferId outputBufferId2;
  outputBufferId1.id_ref() = 1;
  outputBufferId2.id_ref() = 2;
  thriftOutputBuffers.buffers_ref() =  {
    {outputBufferId1, 10},
    {outputBufferId2, 20}
  };

  fromThrift(thriftOutputBuffers, outputBuffers);
  ASSERT_EQ(outputBuffers.type, BufferType::ARBITRARY);
  ASSERT_EQ(outputBuffers.version, 1);
  ASSERT_EQ(outputBuffers.buffers.size(), 2);
  ASSERT_EQ(outputBuffers.buffers["1"], 10);
  ASSERT_EQ(outputBuffers.buffers["2"], 20);
}

TEST_F(TaskUpdateRequestTest, binarySplit) {
  std::string str = slurp(getDataPath(BASE_DATA_PATH, "Split.json"));
  Split split;
  
  registerPrestoToVeloxConnector(std::make_unique<facebook::presto::HivePrestoToVeloxConnector>("hive"));
  cpp2::fromThrift(str, split);
  auto hiveSplit = std::dynamic_pointer_cast<hive::HiveSplit>(split.connectorSplit);
  ASSERT_EQ(split.connectorId, "hive");
  ASSERT_EQ(hiveSplit->database, "tpch");
  ASSERT_EQ(hiveSplit->nodeSelectionStrategy, NodeSelectionStrategy::NO_PREFERENCE);
  
  presto::unregisterPrestoToVeloxConnector("hive");
}

TEST_F(TaskUpdateRequestTest, binaryTableWriteInfo) {
  std::string str = slurp(getDataPath(BASE_DATA_PATH, "TableWriteInfo.json"));
  TableWriteInfo tableWriteInfo;
  
  registerPrestoToVeloxConnector(std::make_unique<facebook::presto::HivePrestoToVeloxConnector>("hive"));
  cpp2::fromThrift(str, tableWriteInfo);
  auto hiveTableHandle = std::dynamic_pointer_cast<hive::HiveTableHandle>((*tableWriteInfo.analyzeTableHandle).connectorHandle);
  ASSERT_EQ(hiveTableHandle->tableName, "test_table");
  ASSERT_EQ(hiveTableHandle->analyzePartitionValues->size(), 2);
  
  presto::unregisterPrestoToVeloxConnector("hive");
}

TEST_F(TaskUpdateRequestTest, fragment) {
  std::string str = slurp(getDataPath(BASE_DATA_PATH, "Fragment.txt"));
  const auto strEnd = str.find_last_not_of(" \t\n\r");
  if (strEnd != std::string::npos) {
    str.erase(strEnd + 1);
  }

  registerPrestoToVeloxConnector(std::make_unique<facebook::presto::HivePrestoToVeloxConnector>("hive"));
  PlanFragment f = json::parse(velox::encoding::Base64::decode(str));

  ASSERT_EQ(f.root->_type, ".AggregationNode");

  std::shared_ptr<AggregationNode> root =
      std::static_pointer_cast<AggregationNode>(f.root);
  ASSERT_EQ(root->id, "211");
  ASSERT_NE(root->source, nullptr);
  ASSERT_EQ(root->source->_type, ".ProjectNode");

  std::shared_ptr<ProjectNode> proj =
      std::static_pointer_cast<ProjectNode>(root->source);
  ASSERT_EQ(proj->id, "233");
  ASSERT_NE(proj->source, nullptr);
  ASSERT_EQ(proj->source->_type, ".TableScanNode");

  std::shared_ptr<TableScanNode> scan =
      std::static_pointer_cast<TableScanNode>(proj->source);
  ASSERT_EQ(scan->id, "0");

  presto::unregisterPrestoToVeloxConnector("hive");
}

TEST_F(TaskUpdateRequestTest, sessionRepresentation) {
  SessionRepresentation sessionRepresentation;
  cpp2::SessionRepresentation thriftSessionRepresentation;
  std::map<std::string, std::map<std::string, std::string>> thriftMap;
  thriftMap["Person1"] = {
      {"Name", "John Doe"},
      {"Age", "30"},
      {"City", "New York"}
  };
  thriftMap["Person2"] = {
      {"Name", "Jane Doe"},
      {"Age", "25"},
      {"City", "Los Angeles"}
  };
  thriftMap["Person3"] = {
      {"Name", "Bob Smith"},
      {"Age", "40"},
      {"City", "Chicago"}
  };
  thriftSessionRepresentation.unprocessedCatalogProperties_ref() = std::move(thriftMap);

  fromThrift(thriftSessionRepresentation, sessionRepresentation);
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties.size(), 3);
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties["Person1"]["City"], "New York");
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties["Person2"]["Age"], "25");
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties["Person3"]["Name"], "Bob Smith");
}
