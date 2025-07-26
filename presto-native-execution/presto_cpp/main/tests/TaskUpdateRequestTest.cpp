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
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"

using namespace facebook::presto;

class TaskUpdateRequestTest : public ::testing::Test {
  protected:
  void SetUp() override {
    registerPrestoToVeloxConnector(
        std::make_unique<facebook::presto::HivePrestoToVeloxConnector>("hive"));
  }

  void TearDown() override {
    unregisterPrestoToVeloxConnector("hive");
  }
};

const std::string BASE_DATA_PATH = "/github/presto-trunk/presto-native-execution/presto_cpp/main/tests/data/";

TEST_F(TaskUpdateRequestTest, connectorId) {
  protocol::ConnectorId connectorId;
  thrift::ConnectorId thriftConnectorId;
  thriftConnectorId.catalogName_ref() = "test";
  thrift::fromThrift(thriftConnectorId, connectorId);
  ASSERT_EQ(connectorId, "test");
}

TEST_F(TaskUpdateRequestTest, optionalField) {
  protocol::ResourceEstimates resourceEstimates;
  thrift::ResourceEstimates thriftResourceEstimates;
  thriftResourceEstimates.executionTime_ref() = 100;
  thriftResourceEstimates.peakMemory_ref() = 1024 * 1024 * 1024;
  thrift::fromThrift(thriftResourceEstimates, resourceEstimates);
  ASSERT_EQ(*resourceEstimates.executionTime, protocol::Duration(100, protocol::TimeUnit::MILLISECONDS));
  ASSERT_EQ(resourceEstimates.cpuTime, nullptr);
  ASSERT_EQ(*resourceEstimates.peakMemory, protocol::DataSize(1024 * 1024 * 1024, protocol::DataUnit::BYTE));
  ASSERT_EQ(resourceEstimates.peakTaskMemory, nullptr);
}

TEST_F(TaskUpdateRequestTest, qualifiedObjectName) {
  protocol::QualifiedObjectName qualifiedObjectName;
  thrift::QualifiedObjectName thriftQualifiedObjectName;
  thriftQualifiedObjectName.catalogName_ref() = "test_catalog";
  thriftQualifiedObjectName.schemaName_ref() = "test_schema";
  thriftQualifiedObjectName.objectName_ref() = "test_object";
  thrift::fromThrift(thriftQualifiedObjectName, qualifiedObjectName);
  ASSERT_EQ(qualifiedObjectName, "test_catalog.test_schema.test_object");
}

TEST_F(TaskUpdateRequestTest, routineCharacteristics) {
  protocol::RoutineCharacteristics routineCharacteristics;
  thrift::RoutineCharacteristics thriftRroutineCharacteristics;
  thrift::Language thriftLanguage;
  thriftLanguage.language_ref() = "English";
  thriftRroutineCharacteristics.language_ref() = std::move(thriftLanguage);
  thriftRroutineCharacteristics.determinism_ref() = thrift::Determinism::NOT_DETERMINISTIC;
  thriftRroutineCharacteristics.nullCallClause_ref() = thrift::NullCallClause::RETURNS_NULL_ON_NULL_INPUT;
  thrift::fromThrift(thriftRroutineCharacteristics, routineCharacteristics);
  ASSERT_EQ((*routineCharacteristics.language).language, "English");
  ASSERT_EQ(*routineCharacteristics.determinism, protocol::Determinism::NOT_DETERMINISTIC);
  ASSERT_EQ(*routineCharacteristics.nullCallClause, protocol::NullCallClause::RETURNS_NULL_ON_NULL_INPUT);
}

TEST_F(TaskUpdateRequestTest, mapOutputBuffers) {
  protocol::OutputBuffers outputBuffers;
  thrift::OutputBuffers thriftOutputBuffers;
  thriftOutputBuffers.type_ref() = thrift::BufferType::ARBITRARY;
  thriftOutputBuffers.version_ref() = 1;
  thriftOutputBuffers.noMoreBufferIds_ref() = true;
  thrift::OutputBufferId outputBufferId1;
  thrift::OutputBufferId outputBufferId2;
  outputBufferId1.id_ref() = 1;
  outputBufferId2.id_ref() = 2;
  thriftOutputBuffers.buffers_ref() =  {
    {outputBufferId1, 10},
    {outputBufferId2, 20}
  };

  thrift::fromThrift(thriftOutputBuffers, outputBuffers);
  ASSERT_EQ(outputBuffers.type, protocol::BufferType::ARBITRARY);
  ASSERT_EQ(outputBuffers.version, 1);
  ASSERT_EQ(outputBuffers.buffers.size(), 2);
  ASSERT_EQ(outputBuffers.buffers["1"], 10);
  ASSERT_EQ(outputBuffers.buffers["2"], 20);
}

TEST_F(TaskUpdateRequestTest, binaryHiveSplitFromThrift) {
  thrift::Split thriftSplit;
  thriftSplit.connectorId()->catalogName_ref() = "hive";
  thriftSplit.transactionHandle()->jsonValue_ref() = R"({
    "@type": "hive",
    "uuid": "8a4d6c83-60ee-46de-9715-bc91755619fa"
  })";
  thriftSplit.connectorSplit()->jsonValue_ref() = slurp(getDataPath(BASE_DATA_PATH, "HiveSplit.json"));

  protocol::Split split;
  thrift::fromThrift(thriftSplit, split);
  
  // Verify that connector specific fields are set correctly with json codec
  auto transactionHandle =
      std::dynamic_pointer_cast<protocol::hive::HiveTransactionHandle>(
          split.transactionHandle);
  ASSERT_EQ(transactionHandle->uuid, "8a4d6c83-60ee-46de-9715-bc91755619fa");

  auto hiveSplit =
      std::dynamic_pointer_cast<protocol::hive::HiveSplit>(
          split.connectorSplit);
  ASSERT_EQ(hiveSplit->database, "tpch");
  ASSERT_EQ(
      hiveSplit->nodeSelectionStrategy,
      protocol::NodeSelectionStrategy::NO_PREFERENCE);
}

TEST_F(TaskUpdateRequestTest, binaryRemoteSplitFromThrift) {
  thrift::Split thriftSplit;
  thrift::RemoteTransactionHandle thriftTransactionHandle;
  thrift::RemoteSplit thriftRemoteSplit;

  thriftSplit.connectorId()->catalogName_ref() = "$remote";
  thriftSplit.transactionHandle()->customSerializedValue_ref() =
      thriftWrite(thriftTransactionHandle);
  
  thriftRemoteSplit.location()->location_ref() = "/test_location";
  thriftRemoteSplit.remoteSourceTaskId()->id_ref() = 100;
  thriftRemoteSplit.remoteSourceTaskId()->attemptNumber_ref() = 200;
  thriftRemoteSplit.remoteSourceTaskId()->stageExecutionId()->id_ref() = 300;
  thriftRemoteSplit.remoteSourceTaskId()->stageExecutionId()->stageId()->id_ref() = 400;
  thriftRemoteSplit.remoteSourceTaskId()->stageExecutionId()->stageId()->queryId_ref() = "test_query_id";

  thriftSplit.connectorSplit()->connectorId_ref() = "$remote";
  thriftSplit.connectorSplit()->customSerializedValue_ref() =
      thriftWrite(thriftRemoteSplit);

  protocol::Split split;
  thrift::fromThrift(thriftSplit, split);

  // Verify that connector specific fields are set correctly with thrift codec
  auto remoteSplit = std::dynamic_pointer_cast<protocol::RemoteSplit>(
      split.connectorSplit);
  ASSERT_EQ((remoteSplit->location).location, "/test_location");
  ASSERT_EQ(remoteSplit->remoteSourceTaskId, "test_query_id.400.300.100.200");
}

TEST_F(TaskUpdateRequestTest, unionExecutionWriterTargetFromThrift) {
  // Construct ExecutionWriterTarget with CreateHandle
  thrift::CreateHandle thriftCreateHandle;
  thrift::ExecutionWriterTargetUnion thriftWriterTarget;
  thriftCreateHandle.schemaTableName()->schema_ref() = "test_schema";
  thriftCreateHandle.schemaTableName()->table_ref() = "test_table";
  thriftCreateHandle.handle()->connectorId()->catalogName_ref() = "hive";
  thriftCreateHandle.handle()->transactionHandle()->jsonValue_ref() = R"({
    "@type": "hive",
    "uuid": "8a4d6c83-60ee-46de-9715-bc91755619fa"
  })";
  thriftCreateHandle.handle()->connectorHandle()->jsonValue_ref() = slurp(getDataPath(BASE_DATA_PATH, "HiveOutputTableHandle.json"));;
  thriftWriterTarget.set_createHandle(std::move(thriftCreateHandle));
  
  // Convert from thrift to protocol and verify fields
  auto writerTarget = std::make_shared<protocol::ExecutionWriterTarget>();
  thrift::fromThrift(thriftWriterTarget, writerTarget);

  ASSERT_EQ(writerTarget->_type, "CreateHandle");
  auto createHandle = std::dynamic_pointer_cast<protocol::CreateHandle>(writerTarget);
  ASSERT_NE(createHandle, nullptr);
  ASSERT_EQ(createHandle->schemaTableName.schema, "test_schema");
  ASSERT_EQ(createHandle->schemaTableName.table, "test_table");

  auto* hiveTxnHandle = dynamic_cast<protocol::hive::HiveTransactionHandle*>(createHandle->handle.transactionHandle.get());
  ASSERT_NE(hiveTxnHandle, nullptr);
  ASSERT_EQ(hiveTxnHandle->uuid, "8a4d6c83-60ee-46de-9715-bc91755619fa");

  auto* hiveOutputTableHandle = dynamic_cast<protocol::hive::HiveOutputTableHandle*>(createHandle->handle.connectorHandle.get());
  ASSERT_NE(hiveOutputTableHandle, nullptr);
  ASSERT_EQ(hiveOutputTableHandle->schemaName, "test_schema");
  ASSERT_EQ(hiveOutputTableHandle->tableName, "test_table");
  ASSERT_EQ(hiveOutputTableHandle->tableStorageFormat, protocol::hive::HiveStorageFormat::ORC);
  ASSERT_EQ(hiveOutputTableHandle->locationHandle.targetPath, "/path/to/target");
}

TEST_F(TaskUpdateRequestTest, unionExecutionWriterTargetToThrift) {
  // Construct thrift ExecutionWriterTarget with CreateHandle
  auto createHandle = std::make_shared<protocol::CreateHandle>();
  createHandle->schemaTableName.schema = "test_schema";
  createHandle->schemaTableName.table = "test_table";
  
  auto writerTarget = std::make_shared<protocol::ExecutionWriterTarget>();
  writerTarget->_type = "CreateHandle";
  writerTarget = createHandle;
  
  // Convert to thrift and verify fields. Note that toThrift functions for connector fields are not implemented.
  thrift::ExecutionWriterTargetUnion thriftWriterTarget;
  thrift::toThrift(writerTarget, thriftWriterTarget);
  ASSERT_TRUE(thriftWriterTarget.createHandle_ref().has_value());
  const auto& thriftCreateHandle = thriftWriterTarget.createHandle_ref().value();
  ASSERT_EQ(thriftCreateHandle.schemaTableName()->schema_ref().value(), "test_schema");
  ASSERT_EQ(thriftCreateHandle.schemaTableName()->table_ref().value(), "test_table");
}

TEST_F(TaskUpdateRequestTest, fragment) {
  std::string str = slurp(getDataPath(BASE_DATA_PATH, "Fragment.thrift.base64"));
  const auto strEnd = str.find_last_not_of(" \t\n\r");
  if (strEnd != std::string::npos) {
    str.erase(strEnd + 1);
  }

  protocol::PlanFragment f = json::parse(facebook::velox::encoding::Base64::decode(str));

  ASSERT_EQ(f.root->_type, ".AggregationNode");

  std::shared_ptr<protocol::AggregationNode> root =
      std::static_pointer_cast<protocol::AggregationNode>(f.root);
  ASSERT_EQ(root->id, "211");
  ASSERT_NE(root->source, nullptr);
  ASSERT_EQ(root->source->_type, ".ProjectNode");

  std::shared_ptr<protocol::ProjectNode> proj =
      std::static_pointer_cast<protocol::ProjectNode>(root->source);
  ASSERT_EQ(proj->id, "233");
  ASSERT_NE(proj->source, nullptr);
  ASSERT_EQ(proj->source->_type, ".TableScanNode");

  std::shared_ptr<protocol::TableScanNode> scan =
      std::static_pointer_cast<protocol::TableScanNode>(proj->source);
  ASSERT_EQ(scan->id, "0");
}

TEST_F(TaskUpdateRequestTest, sessionRepresentation) {
  protocol::SessionRepresentation sessionRepresentation;
  thrift::SessionRepresentation thriftSessionRepresentation;
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

  thrift::fromThrift(thriftSessionRepresentation, sessionRepresentation);
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties.size(), 3);
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties["Person1"]["City"], "New York");
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties["Person2"]["Age"], "25");
  ASSERT_EQ(sessionRepresentation.unprocessedCatalogProperties["Person3"]["Name"], "Bob Smith");
}
