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
#include "presto_cpp/main/connectors/HivePrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/IcebergPrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/presto_protocol/connector/hive/HiveConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/iceberg/IcebergConnectorProtocol.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/encode/Base64.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Filter.h"

using namespace facebook::presto;
using namespace facebook::velox;

class PrestoToVeloxConnectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    pool_ = memory::memoryManager()->addLeafPool();
    typeParser_ = std::make_unique<TypeParser>();
    exprConverter_ =
        std::make_unique<VeloxExprConverter>(pool_.get(), typeParser_.get());
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<TypeParser> typeParser_;
  std::unique_ptr<VeloxExprConverter> exprConverter_;
};

TEST_F(PrestoToVeloxConnectorTest, registerVariousConnectors) {
  std::vector<std::pair<std::string, std::unique_ptr<PrestoToVeloxConnector>>>
      connectorList;
  connectorList.emplace_back(
      std::pair("hive", std::make_unique<HivePrestoToVeloxConnector>("hive")));
  connectorList.emplace_back(
      std::pair(
          "hive-hadoop2",

          std::make_unique<HivePrestoToVeloxConnector>("hive-hadoop2")));
  connectorList.emplace_back(
      std::pair(
          "iceberg",
          std::make_unique<IcebergPrestoToVeloxConnector>("iceberg")));
  connectorList.emplace_back(
      std::pair("tpch", std::make_unique<HivePrestoToVeloxConnector>("tpch")));

  for (auto& [connectorName, connector] : connectorList) {
    registerPrestoToVeloxConnector(std::move(connector));
    EXPECT_EQ(
        connectorName,
        getPrestoToVeloxConnector(connectorName).connectorName());
    unregisterPrestoToVeloxConnector(connectorName);
  }
}

TEST_F(PrestoToVeloxConnectorTest, addDuplicates) {
  constexpr auto kConnectorName = "hive";
  registerPrestoToVeloxConnector(
      std::make_unique<HivePrestoToVeloxConnector>(kConnectorName));
  VELOX_ASSERT_THROW(
      registerPrestoToVeloxConnector(
          std::make_unique<HivePrestoToVeloxConnector>(kConnectorName)),
      fmt::format("Connector {} is already registered", kConnectorName));
}

namespace {

constexpr auto kColumnName1 = "MixedCaseCol1";
constexpr auto kColumnName2 = "UPPERCASECOL2";

protocol::List<protocol::Column> createTestDataColumns() {
  protocol::List<protocol::Column> dataColumns;
  protocol::Column col1;
  col1.name = kColumnName1;
  col1.type = "integer";
  dataColumns.push_back(col1);

  protocol::Column col2;
  col2.name = kColumnName2;
  col2.type = "varchar";
  dataColumns.push_back(col2);

  return dataColumns;
}

std::shared_ptr<protocol::ConstantExpression> createTrueConstant() {
  auto trueConstant = std::make_shared<protocol::ConstantExpression>();
  trueConstant->type = "boolean";
  // base64-encoded true value.
  trueConstant->valueBlock.data = "CgAAAEJZVEVfQVJSQVkBAAAAAAE=";
  return trueConstant;
}

template <typename LayoutType>
void setCommonLayoutProperties(
    std::shared_ptr<LayoutType> layout,
    const protocol::List<protocol::Column>& dataColumns,
    std::shared_ptr<protocol::ConstantExpression> predicate) {
  layout->domainPredicate.domains =
      std::make_shared<protocol::Map<protocol::Subfield, protocol::Domain>>();
  layout->remainingPredicate = predicate;
  layout->pushdownFilterEnabled = false;
  layout->dataColumns = dataColumns;
  layout->partitionColumns = {};
  layout->predicateColumns = {};
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, icebergPreservesColumnNameCase) {
  auto dataColumns = createTestDataColumns();
  auto trueConstant = createTrueConstant();

  auto layout = std::make_shared<protocol::iceberg::IcebergTableLayoutHandle>();
  setCommonLayoutProperties(layout, dataColumns, trueConstant);

  auto icebergHandle =
      std::make_shared<protocol::iceberg::IcebergTableHandle>();
  icebergHandle->schemaName = "test_schema";
  icebergHandle->icebergTableName.tableName = "test_table";

  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "iceberg";
  tableHandle.connectorHandle = icebergHandle;
  tableHandle.connectorTableLayout = layout;

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result = icebergConnector.toVeloxTableHandle(
      tableHandle, *exprConverter_, *typeParser_);

  ASSERT_NE(result, nullptr);
  auto* handle = dynamic_cast<connector::hive::HiveTableHandle*>(result.get());
  ASSERT_NE(handle, nullptr);

  // Verify Iceberg preserves column name case.
  auto dataColumnsType = handle->dataColumns();
  ASSERT_NE(dataColumnsType, nullptr);
  EXPECT_EQ(dataColumnsType->size(), 2);
  EXPECT_EQ(dataColumnsType->nameOf(0), kColumnName1);
  EXPECT_EQ(dataColumnsType->nameOf(1), kColumnName2);
}

TEST_F(PrestoToVeloxConnectorTest, icebergTableSchemaFieldIds) {
  auto dataColumns = createTestDataColumns();
  auto layout = std::make_shared<protocol::iceberg::IcebergTableLayoutHandle>();
  setCommonLayoutProperties(layout, dataColumns, createTrueConstant());

  auto icebergHandle =
      std::make_shared<protocol::iceberg::IcebergTableHandle>();
  icebergHandle->schemaName = "test_schema";
  icebergHandle->icebergTableName.tableName = "test_table";
  icebergHandle->tableSchemaJson = std::make_shared<std::string>(
      R"({"schema-id":7,"fields":[{"id":11,"name":"MixedCaseCol1","required":false,"type":"int"},{"id":29,"name":"UPPERCASECOL2","required":false,"type":"string"}]})");

  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "iceberg";
  tableHandle.connectorHandle = icebergHandle;
  tableHandle.connectorTableLayout = layout;

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result = icebergConnector.toVeloxTableHandle(
      tableHandle, *exprConverter_, *typeParser_);
  auto* handle = dynamic_cast<connector::hive::HiveTableHandle*>(result.get());
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(handle->dataColumnFieldIds(), (std::vector<int32_t>{11, 29}));

  icebergHandle->tableSchemaJson.reset();
  result = icebergConnector.toVeloxTableHandle(
      tableHandle, *exprConverter_, *typeParser_);
  handle = dynamic_cast<connector::hive::HiveTableHandle*>(result.get());
  ASSERT_NE(handle, nullptr);
  EXPECT_TRUE(handle->dataColumnFieldIds().empty());
}

TEST_F(PrestoToVeloxConnectorTest, hiveLowercasesColumnNames) {
  auto dataColumns = createTestDataColumns();
  auto trueConstant = createTrueConstant();

  auto layout = std::make_shared<protocol::hive::HiveTableLayoutHandle>();
  setCommonLayoutProperties(layout, dataColumns, trueConstant);
  layout->tableParameters = {};

  auto hiveHandle = std::make_shared<protocol::hive::HiveTableHandle>();
  hiveHandle->tableName = "test_table";
  hiveHandle->schemaName = "test_schema";

  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "hive";
  tableHandle.connectorHandle = hiveHandle;
  tableHandle.connectorTableLayout = layout;

  HivePrestoToVeloxConnector hiveConnector("hive");
  auto result = hiveConnector.toVeloxTableHandle(
      tableHandle, *exprConverter_, *typeParser_);

  ASSERT_NE(result, nullptr);
  auto* handle = dynamic_cast<connector::hive::HiveTableHandle*>(result.get());
  ASSERT_NE(handle, nullptr);

  // Verify Hive lowercases column names.
  auto dataColumnsType = handle->dataColumns();
  ASSERT_NE(dataColumnsType, nullptr);
  EXPECT_EQ(dataColumnsType->size(), 2);
  EXPECT_EQ(dataColumnsType->nameOf(0), "mixedcasecol1");
  EXPECT_EQ(dataColumnsType->nameOf(1), "uppercasecol2");
}

namespace {

protocol::iceberg::IcebergColumnHandle createIcebergColumnHandle(
    const std::string& name,
    int32_t fieldId,
    const std::string& type,
    protocol::iceberg::TypeCategory typeCategory =
        protocol::iceberg::TypeCategory::PRIMITIVE,
    const std::vector<protocol::iceberg::ColumnIdentity>& children = {}) {
  protocol::iceberg::IcebergColumnHandle column;
  column.columnIdentity.name = name;
  column.columnIdentity.id = fieldId;
  column.columnIdentity.typeCategory = typeCategory;
  column.columnIdentity.children = children;
  column.type = type;
  column.columnType = protocol::hive::ColumnType::REGULAR;
  return column;
}

std::shared_ptr<protocol::Block> serializeToBlock(
    const VectorPtr& vector,
    memory::MemoryPool* pool) {
  serializer::presto::PrestoVectorSerde serde;
  std::ostringstream output;
  serde.serializeSingleColumn(vector, nullptr, pool, &output);
  const auto serialized = output.str();
  auto block = std::make_shared<protocol::Block>();
  block->data = encoding::Base64::encode(serialized.c_str(), serialized.size());
  return block;
}

protocol::Domain createSingleRangeDomain(
    const std::string& typeStr,
    std::shared_ptr<protocol::Block> lowBlock,
    protocol::Bound lowBound,
    std::shared_ptr<protocol::Block> highBlock,
    protocol::Bound highBound,
    bool nullAllowed) {
  protocol::Marker lowMarker;
  lowMarker.type = typeStr;
  lowMarker.valueBlock = std::move(lowBlock);
  lowMarker.bound = lowBound;

  protocol::Marker highMarker;
  highMarker.type = typeStr;
  highMarker.valueBlock = std::move(highBlock);
  highMarker.bound = highBound;

  protocol::Range range;
  range.low = lowMarker;
  range.high = highMarker;

  auto rangeSet = std::make_shared<protocol::SortedRangeSet>();
  rangeSet->type = typeStr;
  rangeSet->ranges = {range};

  protocol::Domain domain;
  domain.values = rangeSet;
  domain.nullAllowed = nullAllowed;
  return domain;
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, icebergColumnHandleSimple) {
  auto icebergColumn = createIcebergColumnHandle("col1", 1, "integer");

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto handle =
      icebergConnector.toVeloxColumnHandle(&icebergColumn, *typeParser_);
  auto* icebergHandle =
      dynamic_cast<connector::hive::iceberg::IcebergColumnHandle*>(
          handle.get());
  ASSERT_NE(icebergHandle, nullptr);

  EXPECT_EQ(icebergHandle->name(), "col1");
  EXPECT_EQ(icebergHandle->dataType()->kind(), TypeKind::INTEGER);
  EXPECT_EQ(icebergHandle->field().fieldId, 1);
  EXPECT_TRUE(icebergHandle->field().children.empty());
}

// The protocol ColumnIdentity.typeAttributes (populated by the Java planner)
// must be converted into the Velox IcebergColumnHandle's IcebergFieldMetadata
// by toVeloxColumnHandle. Asserts a UUID column round-trips
// required/binary-type/length, and that a column without typeAttributes yields
// empty metadata.
TEST_F(PrestoToVeloxConnectorTest, icebergColumnHandleTypeAttributes) {
  auto uuidColumn = createIcebergColumnHandle("u", 5, "varbinary");
  auto typeAttributes =
      std::make_shared<protocol::iceberg::IcebergTypeAttributes>();
  typeAttributes->required = std::make_shared<bool>(true);
  typeAttributes->binaryType = std::make_shared<std::string>("UUID");
  typeAttributes->length = std::make_shared<int>(16);
  uuidColumn.columnIdentity.typeAttributes = typeAttributes;

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto handle = icebergConnector.toVeloxColumnHandle(&uuidColumn, *typeParser_);
  auto* icebergHandle =
      dynamic_cast<connector::hive::iceberg::IcebergColumnHandle*>(
          handle.get());
  ASSERT_NE(icebergHandle, nullptr);

  const auto& metadata = icebergHandle->icebergMetadata();
  EXPECT_EQ(metadata.required, true);
  EXPECT_EQ(metadata.binaryType, "UUID");
  EXPECT_EQ(metadata.length, 16);

  // A column without typeAttributes maps to empty metadata.
  auto plainColumn = createIcebergColumnHandle("a", 1, "bigint");
  auto plainHandle =
      icebergConnector.toVeloxColumnHandle(&plainColumn, *typeParser_);
  auto* plainIcebergHandle =
      dynamic_cast<connector::hive::iceberg::IcebergColumnHandle*>(
          plainHandle.get());
  ASSERT_NE(plainIcebergHandle, nullptr);
  EXPECT_TRUE(plainIcebergHandle->icebergMetadata().empty());
}

TEST_F(PrestoToVeloxConnectorTest, icebergColumnHandleNested) {
  protocol::iceberg::ColumnIdentity child1;
  child1.name = "child1";
  child1.id = 2;
  child1.typeCategory = protocol::iceberg::TypeCategory::PRIMITIVE;

  protocol::iceberg::ColumnIdentity child2;
  child2.name = "child2";
  child2.id = 3;
  child2.typeCategory = protocol::iceberg::TypeCategory::PRIMITIVE;

  auto icebergColumn = createIcebergColumnHandle(
      "struct_col",
      1,
      "row(child1 integer, child2 varchar)",
      protocol::iceberg::TypeCategory::STRUCT,
      {child1, child2});

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto handle =
      icebergConnector.toVeloxColumnHandle(&icebergColumn, *typeParser_);
  auto* icebergHandle =
      dynamic_cast<connector::hive::iceberg::IcebergColumnHandle*>(
          handle.get());
  ASSERT_NE(icebergHandle, nullptr);

  EXPECT_EQ(icebergHandle->name(), "struct_col");
  EXPECT_EQ(icebergHandle->dataType()->kind(), TypeKind::ROW);
  EXPECT_EQ(icebergHandle->field().fieldId, 1);
  ASSERT_EQ(icebergHandle->field().children.size(), 2);
  EXPECT_EQ(icebergHandle->field().children[0].fieldId, 2);
  EXPECT_EQ(icebergHandle->field().children[1].fieldId, 3);
}

TEST_F(PrestoToVeloxConnectorTest, icebergColumnHandleDeeplyNested) {
  protocol::iceberg::ColumnIdentity inner;
  inner.name = "inner";
  inner.id = 3;
  inner.typeCategory = protocol::iceberg::TypeCategory::PRIMITIVE;

  protocol::iceberg::ColumnIdentity middle;
  middle.name = "middle";
  middle.id = 2;
  middle.typeCategory = protocol::iceberg::TypeCategory::STRUCT;
  middle.children = {inner};

  auto icebergColumn = createIcebergColumnHandle(
      "outer",
      1,
      "row(middle row(inner bigint))",
      protocol::iceberg::TypeCategory::STRUCT,
      {middle});

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto handle =
      icebergConnector.toVeloxColumnHandle(&icebergColumn, *typeParser_);
  auto* icebergHandle =
      dynamic_cast<connector::hive::iceberg::IcebergColumnHandle*>(
          handle.get());
  ASSERT_NE(icebergHandle, nullptr);

  EXPECT_EQ(icebergHandle->name(), "outer");
  EXPECT_EQ(icebergHandle->field().fieldId, 1);
  ASSERT_EQ(icebergHandle->field().children.size(), 1);
  EXPECT_EQ(icebergHandle->field().children[0].fieldId, 2);
  ASSERT_EQ(icebergHandle->field().children[0].children.size(), 1);
  EXPECT_EQ(icebergHandle->field().children[0].children[0].fieldId, 3);
}

TEST_F(PrestoToVeloxConnectorTest, ctasPassesTextfileSerdeParameters) {
  auto hiveOutputTableHandle =
      std::make_shared<protocol::hive::HiveOutputTableHandle>();
  hiveOutputTableHandle->schemaName = "test_schema";
  hiveOutputTableHandle->tableName = "test_table";
  hiveOutputTableHandle->tableOwner = "owner";
  hiveOutputTableHandle->actualStorageFormat =
      protocol::hive::HiveStorageFormat::TEXTFILE;
  hiveOutputTableHandle->tableStorageFormat =
      protocol::hive::HiveStorageFormat::TEXTFILE;
  hiveOutputTableHandle->partitionStorageFormat =
      protocol::hive::HiveStorageFormat::TEXTFILE;
  hiveOutputTableHandle->compressionCodec =
      protocol::hive::HiveCompressionCodec::NONE;
  hiveOutputTableHandle->locationHandle.targetPath = "/path/to/target";
  hiveOutputTableHandle->locationHandle.writePath = "/path/to/write";
  hiveOutputTableHandle->locationHandle.tableType =
      protocol::hive::TableType::NEW;
  hiveOutputTableHandle->additionalTableParameters = {
      {"field.delim", "|"},
      {"escape.delim", "\\"},
      {"collection.delim", "$"},
      {"mapkey.delim", "#"},
      {"presto.version", "0.297"}};

  protocol::OutputTableHandle outputHandle;
  outputHandle.connectorId = "hive";
  outputHandle.connectorHandle = hiveOutputTableHandle;

  protocol::CreateHandle createHandle;
  createHandle.handle = outputHandle;

  HivePrestoToVeloxConnector hiveConnector("hive");
  auto result =
      hiveConnector.toVeloxInsertTableHandle(&createHandle, *typeParser_);
  ASSERT_NE(result, nullptr);

  auto* hiveInsert =
      dynamic_cast<connector::hive::HiveInsertTableHandle*>(result.get());
  ASSERT_NE(hiveInsert, nullptr);

  const auto& serdeParams = hiveInsert->serdeParameters();
  // Only serde keys should be extracted, not table-level keys like
  // presto.version.
  EXPECT_EQ(serdeParams.size(), 4);
  EXPECT_EQ(serdeParams.at("field.delim"), "|");
  EXPECT_EQ(serdeParams.at("escape.delim"), "\\");
  EXPECT_EQ(serdeParams.at("collection.delim"), "$");
  EXPECT_EQ(serdeParams.at("mapkey.delim"), "#");
}

TEST_F(PrestoToVeloxConnectorTest, ctasPassesNimbleSerdeParameters) {
  auto hiveOutputTableHandle =
      std::make_shared<protocol::hive::HiveOutputTableHandle>();
  hiveOutputTableHandle->schemaName = "test_schema";
  hiveOutputTableHandle->tableName = "test_table";
  hiveOutputTableHandle->tableOwner = "owner";
  hiveOutputTableHandle->actualStorageFormat =
      protocol::hive::HiveStorageFormat::ALPHA;
  hiveOutputTableHandle->tableStorageFormat =
      protocol::hive::HiveStorageFormat::ALPHA;
  hiveOutputTableHandle->partitionStorageFormat =
      protocol::hive::HiveStorageFormat::ALPHA;
  hiveOutputTableHandle->compressionCodec =
      protocol::hive::HiveCompressionCodec::NONE;
  hiveOutputTableHandle->locationHandle.targetPath = "/path/to/target";
  hiveOutputTableHandle->locationHandle.writePath = "/path/to/write";
  hiveOutputTableHandle->locationHandle.tableType =
      protocol::hive::TableType::NEW;
  hiveOutputTableHandle->additionalTableParameters = {
      {"nimble.stats.enable_vectorized", "true"},
      {"nimble.index.columns", "id"},
      {"alpha.encodingselection.read.factors",
       "Constant=1.0;Trivial=0.7;FixedBitWidth=0.7;MainlyConstant=1.0;"
       "SparseBool=1.0;Dictionary=1.0;RLE=1.0;Varint=1.0"},
      {"presto.version", "0.297"}};

  protocol::OutputTableHandle outputHandle;
  outputHandle.connectorId = "hive";
  outputHandle.connectorHandle = hiveOutputTableHandle;

  protocol::CreateHandle createHandle;
  createHandle.handle = outputHandle;

  HivePrestoToVeloxConnector hiveConnector("hive");
  auto result =
      hiveConnector.toVeloxInsertTableHandle(&createHandle, *typeParser_);
  ASSERT_NE(result, nullptr);

  auto* hiveInsert =
      dynamic_cast<connector::hive::HiveInsertTableHandle*>(result.get());
  ASSERT_NE(hiveInsert, nullptr);

  const auto& serdeParams = hiveInsert->serdeParameters();
  EXPECT_EQ(serdeParams.size(), 3);
  EXPECT_EQ(serdeParams.at("nimble.stats.enable_vectorized"), "true");
  EXPECT_EQ(serdeParams.at("nimble.index.columns"), "id");
  EXPECT_EQ(
      serdeParams.at("alpha.encodingselection.read.factors"),
      "Constant=1.0;Trivial=0.7;FixedBitWidth=0.7;MainlyConstant=1.0;"
      "SparseBool=1.0;Dictionary=1.0;RLE=1.0;Varint=1.0");
}

TEST_F(PrestoToVeloxConnectorTest, ctasEmptySerdeParameters) {
  auto hiveOutputTableHandle =
      std::make_shared<protocol::hive::HiveOutputTableHandle>();
  hiveOutputTableHandle->schemaName = "test_schema";
  hiveOutputTableHandle->tableName = "test_table";
  hiveOutputTableHandle->tableOwner = "owner";
  hiveOutputTableHandle->actualStorageFormat =
      protocol::hive::HiveStorageFormat::DWRF;
  hiveOutputTableHandle->tableStorageFormat =
      protocol::hive::HiveStorageFormat::DWRF;
  hiveOutputTableHandle->partitionStorageFormat =
      protocol::hive::HiveStorageFormat::DWRF;
  hiveOutputTableHandle->compressionCodec =
      protocol::hive::HiveCompressionCodec::NONE;
  hiveOutputTableHandle->locationHandle.targetPath = "/path/to/target";
  hiveOutputTableHandle->locationHandle.writePath = "/path/to/write";
  hiveOutputTableHandle->locationHandle.tableType =
      protocol::hive::TableType::NEW;

  protocol::OutputTableHandle outputHandle;
  outputHandle.connectorId = "hive";
  outputHandle.connectorHandle = hiveOutputTableHandle;

  protocol::CreateHandle createHandle;
  createHandle.handle = outputHandle;

  HivePrestoToVeloxConnector hiveConnector("hive");
  auto result =
      hiveConnector.toVeloxInsertTableHandle(&createHandle, *typeParser_);
  ASSERT_NE(result, nullptr);

  auto* hiveInsert =
      dynamic_cast<connector::hive::HiveInsertTableHandle*>(result.get());
  ASSERT_NE(hiveInsert, nullptr);

  EXPECT_TRUE(hiveInsert->serdeParameters().empty());
}

TEST_F(PrestoToVeloxConnectorTest, hiveInsertTableHandleTableParameters) {
  auto protoHandle = std::make_shared<protocol::hive::HiveInsertTableHandle>();
  protoHandle->_type = "hive";

  protocol::hive::HiveColumnHandle col;
  col.name = "col1";
  col.hiveType = "int";
  col.typeSignature = "integer";
  col.columnType = protocol::hive::ColumnType::REGULAR;
  protoHandle->inputColumns = {col};

  protoHandle->locationHandle.targetPath = "/target";
  protoHandle->locationHandle.writePath = "/write";
  protoHandle->locationHandle.tableType = protocol::hive::TableType::EXISTING;

  protoHandle->actualStorageFormat = protocol::hive::HiveStorageFormat::DWRF;
  protoHandle->compressionCodec = protocol::hive::HiveCompressionCodec::NONE;

  auto table = std::make_shared<protocol::hive::Table>();
  table->storage.parameters = {{"param1", "value1"}, {"param2", "value2"}};
  protoHandle->pageSinkMetadata.table = table;

  protocol::InsertHandle insertHandle;
  insertHandle.handle.connectorHandle = protoHandle;
  insertHandle.handle.connectorId = "hive";

  HivePrestoToVeloxConnector hiveConnector("hive");
  auto result =
      hiveConnector.toVeloxInsertTableHandle(&insertHandle, *typeParser_);

  auto* hiveHandle =
      dynamic_cast<connector::hive::HiveInsertTableHandle*>(result.get());
  ASSERT_NE(hiveHandle, nullptr);

  const auto& storageParams = hiveHandle->storageParameters();
  EXPECT_EQ(storageParams.size(), 2);
  EXPECT_EQ(storageParams.at("param1"), "value1");
  EXPECT_EQ(storageParams.at("param2"), "value2");
}

TEST_F(PrestoToVeloxConnectorTest, bigintOverflowLowAboveMax) {
  auto lowBlock = serializeToBlock(
      BaseVector::createConstant(
          BIGINT(),
          variant(std::numeric_limits<int64_t>::max()),
          1,
          pool_.get()),
      pool_.get());
  auto domain = createSingleRangeDomain(
      "bigint",
      lowBlock,
      protocol::Bound::ABOVE,
      nullptr,
      protocol::Bound::BELOW,
      false);

  auto filter = toFilter(domain, *exprConverter_, *typeParser_);
  EXPECT_EQ(filter->kind(), common::FilterKind::kAlwaysFalse);
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(std::numeric_limits<int64_t>::max()));
  EXPECT_FALSE(filter->testNull());
}

TEST_F(PrestoToVeloxConnectorTest, bigintOverflowHighBelowMin) {
  auto highBlock = serializeToBlock(
      BaseVector::createConstant(
          BIGINT(),
          variant(std::numeric_limits<int64_t>::min()),
          1,
          pool_.get()),
      pool_.get());
  auto domain = createSingleRangeDomain(
      "bigint",
      nullptr,
      protocol::Bound::ABOVE,
      highBlock,
      protocol::Bound::BELOW,
      false);

  auto filter = toFilter(domain, *exprConverter_, *typeParser_);
  EXPECT_EQ(filter->kind(), common::FilterKind::kAlwaysFalse);
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(std::numeric_limits<int64_t>::min()));
  EXPECT_FALSE(filter->testNull());
}

TEST_F(PrestoToVeloxConnectorTest, bigintOverflowWithNullAllowed) {
  auto lowBlock = serializeToBlock(
      BaseVector::createConstant(
          BIGINT(),
          variant(std::numeric_limits<int64_t>::max()),
          1,
          pool_.get()),
      pool_.get());
  auto domain = createSingleRangeDomain(
      "bigint",
      lowBlock,
      protocol::Bound::ABOVE,
      nullptr,
      protocol::Bound::BELOW,
      true);

  auto filter = toFilter(domain, *exprConverter_, *typeParser_);
  EXPECT_EQ(filter->kind(), common::FilterKind::kIsNull);
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(std::numeric_limits<int64_t>::max()));
  EXPECT_TRUE(filter->testNull());
}

TEST_F(PrestoToVeloxConnectorTest, dateOverflowLowAboveMax) {
  auto lowBlock = serializeToBlock(
      BaseVector::createConstant(
          DATE(), variant(std::numeric_limits<int32_t>::max()), 1, pool_.get()),
      pool_.get());
  auto domain = createSingleRangeDomain(
      "date",
      lowBlock,
      protocol::Bound::ABOVE,
      nullptr,
      protocol::Bound::BELOW,
      false);

  auto filter = toFilter(domain, *exprConverter_, *typeParser_);
  EXPECT_EQ(filter->kind(), common::FilterKind::kAlwaysFalse);
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(std::numeric_limits<int32_t>::max()));
  EXPECT_FALSE(filter->testNull());
}

TEST_F(PrestoToVeloxConnectorTest, dateOverflowHighBelowMin) {
  auto highBlock = serializeToBlock(
      BaseVector::createConstant(
          DATE(), variant(std::numeric_limits<int32_t>::min()), 1, pool_.get()),
      pool_.get());
  auto domain = createSingleRangeDomain(
      "date",
      nullptr,
      protocol::Bound::ABOVE,
      highBlock,
      protocol::Bound::BELOW,
      false);

  auto filter = toFilter(domain, *exprConverter_, *typeParser_);
  EXPECT_EQ(filter->kind(), common::FilterKind::kAlwaysFalse);
  EXPECT_FALSE(filter->testInt64(0));
  EXPECT_FALSE(filter->testInt64(std::numeric_limits<int32_t>::min()));
  EXPECT_FALSE(filter->testNull());
}

namespace {

// Builds a minimal protocol::iceberg::IcebergDeleteTableHandle wrapped in a
// protocol::DeleteHandle. The handle carries a single nullable int column and
// the requested fileContent value; other fields are intentionally minimal
// since the bridge only consults them as opaque pass-through.
protocol::DeleteHandle makeIcebergDeleteHandle(
    protocol::iceberg::FileContent fileContent) {
  auto protoHandle =
      std::make_shared<protocol::iceberg::IcebergDeleteTableHandle>();
  protoHandle->_type = "hive-iceberg";
  protoHandle->schemaName = "test_schema";
  protoHandle->tableName.tableName = "test_table";
  protoHandle->outputPath = "/path/to/iceberg/data";
  protoHandle->fileFormat = protocol::iceberg::FileFormat::PARQUET;
  protoHandle->compressionCodec = protocol::hive::HiveCompressionCodec::NONE;
  protoHandle->fileContent = fileContent;

  // Provide one input column so toIcebergColumns has something to convert.
  protocol::iceberg::IcebergColumnHandle column;
  column.columnIdentity.name = "id";
  column.columnIdentity.id = 1;
  column.columnIdentity.typeCategory =
      protocol::iceberg::TypeCategory::PRIMITIVE;
  column.type = "integer";
  column.columnType = protocol::hive::ColumnType::REGULAR;
  protoHandle->inputColumns = {column};

  protocol::DeleteHandle deleteHandle;
  deleteHandle.handle.connectorHandle = protoHandle;
  deleteHandle.handle.connectorId = "iceberg";
  return deleteHandle;
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, icebergDeleteTableHandleDeletionVector) {
  auto deleteHandle =
      makeIcebergDeleteHandle(protocol::iceberg::FileContent::DELETION_VECTOR);

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result =
      icebergConnector.toVeloxInsertTableHandle(&deleteHandle, *typeParser_);
  ASSERT_NE(result, nullptr);

  // The bridge returns a Velox IcebergInsertTableHandle (the unified write
  // handle); the WriteKind enum on it distinguishes data vs deletion-vector.
  auto* icebergInsert =
      dynamic_cast<connector::hive::iceberg::IcebergInsertTableHandle*>(
          result.get());
  ASSERT_NE(icebergInsert, nullptr);
  EXPECT_EQ(
      icebergInsert->writeKind(),
      connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::
          kDeletionVector);

  // Confirm the location handle carries the expected target path and is in
  // "existing" mode (DELETE targets an existing table).
  const auto& locationHandle = icebergInsert->locationHandle();
  EXPECT_EQ(locationHandle->targetPath(), "/path/to/iceberg/data/data");
  EXPECT_EQ(
      locationHandle->tableType(),
      connector::hive::LocationHandle::TableType::kExisting);

  // The single input column from the protocol handle round-trips through
  // toIcebergColumns.
  EXPECT_EQ(icebergInsert->inputColumns().size(), 1);
  EXPECT_EQ(icebergInsert->inputColumns()[0]->name(), "id");
}

TEST_F(
    PrestoToVeloxConnectorTest,
    icebergDeleteTableHandlePositionDeletesFallbackToData) {
  // POSITION_DELETES is the V2 content type. The V2 DELETE path runs entirely
  // on the Java side (row-id rewrite via IcebergMergeSink), so this branch
  // should not normally be exercised on the worker. The defensive default
  // path in the override falls back to kData so an unexpected protocol value
  // surfaces as a typed sink error rather than silently writing a deletion
  // vector for the wrong format.
  auto deleteHandle =
      makeIcebergDeleteHandle(protocol::iceberg::FileContent::POSITION_DELETES);

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result =
      icebergConnector.toVeloxInsertTableHandle(&deleteHandle, *typeParser_);
  ASSERT_NE(result, nullptr);

  auto* icebergInsert =
      dynamic_cast<connector::hive::iceberg::IcebergInsertTableHandle*>(
          result.get());
  ASSERT_NE(icebergInsert, nullptr);
  EXPECT_EQ(
      icebergInsert->writeKind(),
      connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::kData);
}

TEST_F(
    PrestoToVeloxConnectorTest,
    icebergDeleteTableHandleRejectsNonIcebergHandle) {
  // If a non-Iceberg connector handle is wrapped in protocol::DeleteHandle
  // (e.g., a Hive delete handle accidentally routed to the Iceberg bridge),
  // the dynamic_pointer_cast yields nullptr and the override raises a
  // VELOX_CHECK_NOT_NULL with the unexpected type name.
  auto hiveDeleteHandle = std::make_shared<protocol::hive::HiveTableHandle>();
  hiveDeleteHandle->_type = "hive";
  protocol::DeleteHandle deleteHandle;
  // protocol::DeleteHandle::connectorHandle is
  // shared_ptr<ConnectorDeleteTableHandle>; a HiveTableHandle is not such a
  // subclass, so smuggle it through by constructing a typed but mismatched
  // JSON-encoded subclass marker.
  auto bogusHandle = std::make_shared<protocol::iceberg::IcebergTableHandle>();
  bogusHandle->_type = "hive-iceberg-not-delete";
  deleteHandle.handle.connectorHandle =
      std::static_pointer_cast<protocol::ConnectorDeleteTableHandle>(
          std::shared_ptr<protocol::JsonEncodedSubclass>(bogusHandle));
  deleteHandle.handle.connectorId = "iceberg";

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  VELOX_ASSERT_THROW(
      icebergConnector.toVeloxInsertTableHandle(&deleteHandle, *typeParser_),
      "Unexpected delete table handle type");
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitTranslatesDeletionVectorDelete) {
  // A V3 DELETION_VECTOR delete file flows through toVeloxSplit and must land
  // as a velox IcebergDeleteFile with FileContent::kDeletionVector and the
  // PUFFIN content offset / length / referencedDataFile fields propagated.
  // Before the toVeloxFileContent bridge wired DELETION_VECTOR, this path
  // raised VELOX_UNSUPPORTED on the worker.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.start = 0;
  split.length = 1024;
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.dataSequenceNumber = 5;

  protocol::iceberg::DeleteFile dv;
  dv.content = protocol::iceberg::FileContent::DELETION_VECTOR;
  dv.path = "/path/to/deletes/dv.puffin";
  dv.format = protocol::iceberg::FileFormat::PUFFIN;
  dv.recordCount = 4;
  dv.fileSizeInBytes = 128;
  dv.dataSequenceNumber = 6;
  dv.contentOffset = std::make_shared<protocol::Long>(16);
  dv.contentSizeInBytes = std::make_shared<protocol::Long>(64);
  dv.referencedDataFile =
      std::make_shared<protocol::String>("/path/to/data/file.dwrf");
  split.deletes = {dv};

  protocol::SplitContext context;
  context.cacheable = false;

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  ASSERT_NE(veloxSplit, nullptr);

  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);
  ASSERT_EQ(hiveIceberg->deleteFiles.size(), 1);
  const auto& deleteFile = hiveIceberg->deleteFiles[0];
  EXPECT_EQ(
      deleteFile.content,
      connector::hive::iceberg::FileContent::kDeletionVector);
  EXPECT_EQ(deleteFile.filePath, "/path/to/deletes/dv.puffin");
  EXPECT_EQ(deleteFile.recordCount, 4);
  EXPECT_EQ(deleteFile.dataSequenceNumber, 6);
  EXPECT_EQ(deleteFile.contentOffset, 16);
  EXPECT_EQ(deleteFile.contentLength, 64);
  EXPECT_EQ(deleteFile.referencedDataFile, "/path/to/data/file.dwrf");
}

namespace {

// Builds a PartitionSpecParser.toJson() payload. Iceberg uses hyphenated key
// names ("spec-id", "source-id", "field-id"), which is what a real split
// carries.
std::string makePartitionSpecJson(
    const std::string& fieldsJson,
    int32_t specId = 0) {
  return fmt::format(R"({{"spec-id":{},"fields":[{}]}})", specId, fieldsJson);
}

protocol::hive::HivePartitionKey makePartitionKey(
    const std::string& name,
    const std::optional<std::string>& value) {
  protocol::hive::HivePartitionKey key;
  key.name = name;
  if (value.has_value()) {
    key.value = std::make_shared<protocol::String>(*value);
  }
  return key;
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitRetainsOnlyIdentityPartitions) {
  // A spec mixing identity, bucket, and void. Both transformed fields are
  // deliberately named after a real source column so that any name-based
  // classification would misidentify them: 'void' keeps the source name by
  // default, and a bucket field can be explicitly named anything.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.partitionSpecAsJson = makePartitionSpecJson(
      R"({"name":"id","transform":"identity","source-id":1,"field-id":1000},)"
      R"({"name":"name","transform":"bucket[4]","source-id":2,"field-id":1001},)"
      R"({"name":"ts","transform":"void","source-id":3,"field-id":1002})");
  // Java keys every field by its partition-field ID and duplicates identity
  // fields under the source ID.
  split.partitionKeys = {
      {1, makePartitionKey("id", "7")},
      {1000, makePartitionKey("id", "7")},
      {1001, makePartitionKey("name", "3")},
      {1002, makePartitionKey("ts", std::nullopt)},
  };

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  // Only source field ID 1 is substitutable. The bucket ordinal and the void
  // null must not be offered as source-column values.
  const std::unordered_map<int32_t, std::optional<std::string>> expected = {
      {1, std::optional<std::string>{"7"}}};
  EXPECT_EQ(hiveIceberg->identityPartitionKeys, expected);

  // The raw name-keyed map is unchanged and still carries every field.
  EXPECT_EQ(hiveIceberg->partitionKeys.size(), 3);
}

TEST_F(
    PrestoToVeloxConnectorTest,
    toVeloxSplitExcludesEveryNonIdentityTransform) {
  // Iceberg's full transform set, each paired with a partition field that
  // takes the source column's name -- the default for every transform except
  // bucket and truncate, and what makes a name-keyed lookup unsafe. Only an
  // identity field stores the source value, so every one of these must be
  // excluded no matter how the field is named.
  const std::vector<std::string> nonIdentityTransforms = {
      "bucket[16]",
      "truncate[4]",
      "year",
      "month",
      "day",
      "hour",
      "void",
      // Not an Iceberg transform string -- Transforms parses only bucket,
      // truncate, identity, void and the four time transforms. Included so an
      // unrecognized transform is proven to be skipped rather than assumed
      // identity.
      "unknownFutureTransform",
  };

  for (const auto& transform : nonIdentityTransforms) {
    protocol::iceberg::IcebergSplit split;
    split.path = "/path/to/data/file.dwrf";
    split.fileFormat = protocol::iceberg::FileFormat::ORC;
    // An identity field alongside the transformed one, so a regression that
    // simply returns an empty map cannot pass this test.
    split.partitionSpecAsJson = makePartitionSpecJson(
        fmt::format(
            R"({{"name":"id","transform":"identity","source-id":1,"field-id":1000}},)"
            R"({{"name":"ts","transform":"{}","source-id":2,"field-id":1001}})",
            transform));
    split.partitionKeys = {
        {1, makePartitionKey("id", "7")},
        {1000, makePartitionKey("id", "7")},
        {2, makePartitionKey("ts", "transformed")},
        {1001, makePartitionKey("ts", "transformed")},
    };

    protocol::SplitContext context;
    IcebergPrestoToVeloxConnector icebergConnector("iceberg");
    auto veloxSplit =
        icebergConnector.toVeloxSplit("iceberg", &split, &context);
    auto* hiveIceberg =
        dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
            veloxSplit.get());
    ASSERT_NE(hiveIceberg, nullptr);

    const std::unordered_map<int32_t, std::optional<std::string>> expected = {
        {1, std::optional<std::string>{"7"}}};
    EXPECT_EQ(hiveIceberg->identityPartitionKeys, expected)
        << "transform should not be substitutable: " << transform;
  }
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitKeepsNullIdentityPartition) {
  // A null identity value is still substitutable — the source column really
  // is null for every row in this partition.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.partitionSpecAsJson = makePartitionSpecJson(
      R"({"name":"id","transform":"identity","source-id":1,"field-id":1000})");
  split.partitionKeys = {
      {1, makePartitionKey("id", std::nullopt)},
      {1000, makePartitionKey("id", std::nullopt)},
  };

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  const std::unordered_map<int32_t, std::optional<std::string>> expected = {
      {1, std::nullopt}};
  EXPECT_EQ(hiveIceberg->identityPartitionKeys, expected);
}

TEST_F(
    PrestoToVeloxConnectorTest,
    toVeloxSplitIdentityPartitionFallsBackToFieldId) {
  // Defensive: if a producer emits only the partition-field-ID entry, the
  // value is still recoverable because the spec already proved the transform
  // is identity. The result stays keyed by the source ID.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.partitionSpecAsJson = makePartitionSpecJson(
      R"({"name":"id","transform":"identity","source-id":1,"field-id":1000})");
  split.partitionKeys = {{1000, makePartitionKey("id", "7")}};

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  const std::unordered_map<int32_t, std::optional<std::string>> expected = {
      {1, std::optional<std::string>{"7"}}};
  EXPECT_EQ(hiveIceberg->identityPartitionKeys, expected);
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitSkipsIdentityWithNoValue) {
  // An identity field whose value is absent under both its source ID and its
  // partition-field ID is skipped rather than guessed at, so the reader falls
  // back to reading the source column from the file.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.partitionSpecAsJson = makePartitionSpecJson(
      R"({"name":"id","transform":"identity","source-id":1,"field-id":1000})");
  // Only an unrelated field's value is present.
  split.partitionKeys = {{4242, makePartitionKey("other", "9")}};

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  EXPECT_TRUE(hiveIceberg->identityPartitionKeys.empty());
}

TEST_F(
    PrestoToVeloxConnectorTest,
    toVeloxSplitAcceptsPartitionSpecWithoutFieldIds) {
  // Iceberg's PartitionSpecParser treats "field-id" as optional: only "name",
  // "transform" and "source-id" are required, and a spec that omits field IDs
  // on every field gets them assigned from PARTITION_DATA_ID_START. V1 specs
  // are written this way. Rejecting such a spec would silently disable
  // identity substitution for those tables, so classify on "source-id" alone.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.partitionSpecAsJson = makePartitionSpecJson(
      R"({"name":"id","transform":"identity","source-id":1},)"
      R"({"name":"name","transform":"bucket[4]","source-id":2})");
  split.partitionKeys = {
      {1, makePartitionKey("id", "7")},
      {2, makePartitionKey("name", "3")},
  };

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  // Only the identity field is substitutable; the bucket field stores the
  // transform result, not the source value.
  const std::unordered_map<int32_t, std::optional<std::string>> expected = {
      {1, std::optional<std::string>{"7"}}};
  EXPECT_EQ(hiveIceberg->identityPartitionKeys, expected);
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitRejectsUnusablePartitionSpec) {
  // Absent, unparseable, and structurally incomplete specs must all yield an
  // empty map so the reader falls back to reading source columns from the
  // file rather than guessing identity from a name match.
  const std::vector<std::string> unusableSpecs = {
      "",
      "not json at all",
      // Valid JSON, but not an object.
      "[1, 2, 3]",
      R"({"spec-id":0})",
      // "fields" present but its entries are not objects.
      R"({"spec-id":0,"fields":[1,2]})",
      // A field missing "source-id" cannot be classified, so the whole spec
      // is rejected rather than partially trusted.
      makePartitionSpecJson(
          R"({"name":"id","transform":"identity","field-id":1000})"),
  };

  for (const auto& specJson : unusableSpecs) {
    protocol::iceberg::IcebergSplit split;
    split.path = "/path/to/data/file.dwrf";
    split.fileFormat = protocol::iceberg::FileFormat::ORC;
    split.partitionSpecAsJson = specJson;
    split.partitionKeys = {
        {1, makePartitionKey("id", "7")},
        {1000, makePartitionKey("id", "7")},
    };

    protocol::SplitContext context;
    IcebergPrestoToVeloxConnector icebergConnector("iceberg");
    auto veloxSplit =
        icebergConnector.toVeloxSplit("iceberg", &split, &context);
    auto* hiveIceberg =
        dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
            veloxSplit.get());
    ASSERT_NE(hiveIceberg, nullptr);
    EXPECT_TRUE(hiveIceberg->identityPartitionKeys.empty())
        << "spec should have been rejected: " << specJson;
  }
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitPopulatesSpecIdInfoColumn) {
  // The split's partitionSpecAsJson is produced by Iceberg's
  // PartitionSpecParser.toJson, which emits a hyphenated "spec-id" key. The
  // spec_id feeds the synthesized $target_table_row_id used by MERGE INTO, and
  // IcebergSplitReader defaults it to 0 when the info column is missing — so
  // failing to parse it silently reports spec 0 for every partition-evolved
  // table rather than erroring.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.partitionSpecAsJson = makePartitionSpecJson(
      R"({"name":"id","transform":"identity","source-id":1,"field-id":1000})",
      /*specId=*/7);

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  const auto it = hiveIceberg->infoColumns.find(
      connector::hive::iceberg::IcebergMetadataColumn::kSpecIdInfoColumn);
  ASSERT_NE(it, hiveIceberg->infoColumns.end())
      << "spec_id info column should be populated from the partition spec";
  EXPECT_EQ(it->second, "7");
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitOmitsSpecIdWhenUnparseable) {
  // Absent or malformed spec JSON leaves the info column out entirely rather
  // than inventing a spec id.
  const std::vector<std::string> unusableSpecs = {
      "", "not json at all", "[1,2,3]", R"({"fields":[]})"};

  for (const auto& specJson : unusableSpecs) {
    protocol::iceberg::IcebergSplit split;
    split.path = "/path/to/data/file.dwrf";
    split.fileFormat = protocol::iceberg::FileFormat::ORC;
    split.partitionSpecAsJson = specJson;

    protocol::SplitContext context;
    IcebergPrestoToVeloxConnector icebergConnector("iceberg");
    auto veloxSplit =
        icebergConnector.toVeloxSplit("iceberg", &split, &context);
    auto* hiveIceberg =
        dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
            veloxSplit.get());
    ASSERT_NE(hiveIceberg, nullptr);
    EXPECT_EQ(
        hiveIceberg->infoColumns.count(
            connector::hive::iceberg::IcebergMetadataColumn::kSpecIdInfoColumn),
        0)
        << "spec_id should be omitted for spec: " << specJson;
  }
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitCarriesDataSequenceNumber) {
  // The data file's sequence number gates which delete files apply to it.
  // IcebergSplitReader::shouldSkipBySequenceNumber treats a value <= 0 as
  // "unassigned" and disables filtering entirely, so leaving this at 0 lets an
  // equality delete apply to data written after it.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.dataSequenceNumber = 5;

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  EXPECT_EQ(hiveIceberg->dataSequenceNumber, 5);

  // The same value is also surfaced as an info column for row lineage; the two
  // must not drift apart.
  const auto it = hiveIceberg->infoColumns.find(
      connector::hive::iceberg::IcebergMetadataColumn::
          kDataSequenceNumberInfoColumn);
  ASSERT_NE(it, hiveIceberg->infoColumns.end());
  EXPECT_EQ(it->second, "5");
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitKeepsUnassignedSequenceNumber) {
  // V1 tables have no sequence numbers. Passing 0 through unchanged keeps
  // filtering disabled for them, which is the documented "unassigned"
  // behavior rather than an accidental skip.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.dataSequenceNumber = 0;

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  EXPECT_EQ(hiveIceberg->dataSequenceNumber, 0);
}

namespace {

// One REGULAR integer column named "id", used as both an input column and the
// source of the partition field below.
protocol::iceberg::IcebergColumnHandle makeIcebergIdColumn() {
  protocol::iceberg::IcebergColumnHandle column;
  column.columnIdentity.name = "id";
  column.columnIdentity.id = 1;
  column.columnIdentity.typeCategory =
      protocol::iceberg::TypeCategory::PRIMITIVE;
  column.type = "integer";
  column.columnType = protocol::hive::ColumnType::REGULAR;
  return column;
}

// A one-field identity partition spec over "id".
//
// toVeloxIcebergPartitionField resolves each field's Velox type by matching
// the *partition field* name against the schema's column names, so the schema
// must carry a column of the same name or the conversion throws.
protocol::iceberg::PrestoIcebergPartitionSpec makeIdentityPartitionSpec() {
  protocol::iceberg::PrestoIcebergNestedField schemaColumn;
  schemaColumn.id = 1;
  schemaColumn.name = "id";
  schemaColumn.prestoType = "integer";
  schemaColumn.optional = true;

  protocol::iceberg::PrestoIcebergSchema schema;
  schema.schemaId = 0;
  schema.columns = {schemaColumn};

  protocol::iceberg::IcebergPartitionField field;
  field.sourceId = 1;
  field.fieldId = 1000;
  field.name = "id";
  field.transform = protocol::iceberg::PartitionTransformType::IDENTITY;

  protocol::iceberg::PrestoIcebergPartitionSpec spec;
  spec.specId = 3;
  spec.schema = schema;
  spec.fields = {field};
  return spec;
}

// Asserts the parts every write handle shares: a partitioned Iceberg insert
// handle whose location points at "<outputPath>/data".
void verifyIcebergWriteHandle(
    const std::unique_ptr<connector::ConnectorInsertTableHandle>& result,
    connector::hive::LocationHandle::TableType expectedTableType,
    connector::hive::iceberg::IcebergInsertTableHandle::WriteKind
        expectedWriteKind) {
  ASSERT_NE(result, nullptr);
  auto* icebergInsert =
      dynamic_cast<connector::hive::iceberg::IcebergInsertTableHandle*>(
          result.get());
  ASSERT_NE(icebergInsert, nullptr);

  EXPECT_EQ(icebergInsert->writeKind(), expectedWriteKind);
  EXPECT_EQ(
      icebergInsert->locationHandle()->targetPath(), "/path/to/table/data");
  EXPECT_EQ(icebergInsert->locationHandle()->tableType(), expectedTableType);

  ASSERT_EQ(icebergInsert->inputColumns().size(), 1);
  EXPECT_EQ(icebergInsert->inputColumns()[0]->name(), "id");

  // The partition spec round-trips, including the identity transform and the
  // field's Velox type resolved from the schema.
  const auto& partitionSpec = icebergInsert->partitionSpec();
  ASSERT_NE(partitionSpec, nullptr);
  EXPECT_EQ(partitionSpec->specId, 3);
  ASSERT_EQ(partitionSpec->fields.size(), 1);
  EXPECT_EQ(partitionSpec->fields[0].name, "id");
  EXPECT_EQ(
      partitionSpec->fields[0].transformType,
      connector::hive::iceberg::TransformType::kIdentity);
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, icebergCreateTableHandle) {
  // CREATE TABLE AS: the target does not exist yet, so the location handle
  // must be in "new" mode.
  auto protoHandle =
      std::make_shared<protocol::iceberg::IcebergOutputTableHandle>();
  protoHandle->_type = "hive-iceberg";
  protoHandle->schemaName = "test_schema";
  protoHandle->tableName.tableName = "test_table";
  protoHandle->outputPath = "/path/to/table";
  protoHandle->fileFormat = protocol::iceberg::FileFormat::PARQUET;
  protoHandle->compressionCodec = protocol::hive::HiveCompressionCodec::NONE;
  protoHandle->inputColumns = {makeIcebergIdColumn()};
  protoHandle->partitionSpec = makeIdentityPartitionSpec();

  protocol::CreateHandle createHandle;
  createHandle.handle.connectorHandle = protoHandle;
  createHandle.handle.connectorId = "iceberg";

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result =
      icebergConnector.toVeloxInsertTableHandle(&createHandle, *typeParser_);

  verifyIcebergWriteHandle(
      result,
      connector::hive::LocationHandle::TableType::kNew,
      connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::kData);
}

TEST_F(PrestoToVeloxConnectorTest, icebergInsertTableHandle) {
  auto protoHandle =
      std::make_shared<protocol::iceberg::IcebergInsertTableHandle>();
  protoHandle->_type = "hive-iceberg";
  protoHandle->schemaName = "test_schema";
  protoHandle->tableName.tableName = "test_table";
  protoHandle->outputPath = "/path/to/table";
  protoHandle->fileFormat = protocol::iceberg::FileFormat::PARQUET;
  protoHandle->compressionCodec = protocol::hive::HiveCompressionCodec::NONE;
  protoHandle->inputColumns = {makeIcebergIdColumn()};
  protoHandle->partitionSpec = makeIdentityPartitionSpec();

  protocol::InsertHandle insertHandle;
  insertHandle.handle.connectorHandle = protoHandle;
  insertHandle.handle.connectorId = "iceberg";

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result =
      icebergConnector.toVeloxInsertTableHandle(&insertHandle, *typeParser_);

  verifyIcebergWriteHandle(
      result,
      connector::hive::LocationHandle::TableType::kExisting,
      connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::kData);
}

TEST_F(PrestoToVeloxConnectorTest, icebergMergeTableHandle) {
  // MERGE unwraps the nested insert handle and tags the result kMerge, which
  // is what routes the write to IcebergMergeSink rather than IcebergDataSink.
  auto protoHandle =
      std::make_shared<protocol::iceberg::IcebergMergeTableHandle>();
  protoHandle->_type = "hive-iceberg";
  protoHandle->insertTableHandle.schemaName = "test_schema";
  protoHandle->insertTableHandle.tableName.tableName = "test_table";
  protoHandle->insertTableHandle.outputPath = "/path/to/table";
  protoHandle->insertTableHandle.fileFormat =
      protocol::iceberg::FileFormat::PARQUET;
  protoHandle->insertTableHandle.compressionCodec =
      protocol::hive::HiveCompressionCodec::NONE;
  protoHandle->insertTableHandle.inputColumns = {makeIcebergIdColumn()};
  protoHandle->insertTableHandle.partitionSpec = makeIdentityPartitionSpec();

  protocol::MergeHandle mergeHandle;
  mergeHandle.connectorMergeTableHandle = protoHandle;

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result =
      icebergConnector.toVeloxInsertTableHandle(&mergeHandle, *typeParser_);

  verifyIcebergWriteHandle(
      result,
      connector::hive::LocationHandle::TableType::kExisting,
      connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::kMerge);
}

TEST_F(PrestoToVeloxConnectorTest, icebergDistributedProcedureHandle) {
  auto protoHandle =
      std::make_shared<protocol::iceberg::IcebergDistributedProcedureHandle>();
  protoHandle->_type = "hive-iceberg";
  protoHandle->schemaName = "test_schema";
  protoHandle->tableName.tableName = "test_table";
  protoHandle->outputPath = "/path/to/table";
  protoHandle->fileFormat = protocol::iceberg::FileFormat::PARQUET;
  protoHandle->compressionCodec = protocol::hive::HiveCompressionCodec::NONE;
  protoHandle->inputColumns = {makeIcebergIdColumn()};
  protoHandle->partitionSpec = makeIdentityPartitionSpec();

  protocol::ExecuteProcedureHandle procedureHandle;
  procedureHandle.handle.connectorHandle = protoHandle;
  procedureHandle.handle.connectorId = "iceberg";

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result =
      icebergConnector.toVeloxInsertTableHandle(&procedureHandle, *typeParser_);

  verifyIcebergWriteHandle(
      result,
      connector::hive::LocationHandle::TableType::kExisting,
      connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::kData);
}

namespace {

protocol::iceberg::DeleteFile makeProtocolDeleteFile(
    protocol::iceberg::FileContent content,
    protocol::iceberg::FileFormat format,
    const std::string& path) {
  protocol::iceberg::DeleteFile deleteFile;
  deleteFile.content = content;
  deleteFile.format = format;
  deleteFile.path = path;
  deleteFile.recordCount = 3;
  deleteFile.fileSizeInBytes = 128;
  deleteFile.dataSequenceNumber = 4;
  return deleteFile;
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitMapsEveryDeleteFileContent) {
  // Each Iceberg delete-file content type routes to a different reader, so a
  // mis-mapping here silently applies the wrong delete semantics.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.deletes = {
      makeProtocolDeleteFile(
          protocol::iceberg::FileContent::DATA,
          protocol::iceberg::FileFormat::PARQUET,
          "/deletes/data.parquet"),
      makeProtocolDeleteFile(
          protocol::iceberg::FileContent::POSITION_DELETES,
          protocol::iceberg::FileFormat::PARQUET,
          "/deletes/pos.parquet"),
      makeProtocolDeleteFile(
          protocol::iceberg::FileContent::EQUALITY_DELETES,
          protocol::iceberg::FileFormat::PARQUET,
          "/deletes/eq.parquet"),
      makeProtocolDeleteFile(
          protocol::iceberg::FileContent::DELETION_VECTOR,
          protocol::iceberg::FileFormat::PARQUET,
          "/deletes/dv.parquet"),
  };

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  using FC = connector::hive::iceberg::FileContent;
  ASSERT_EQ(hiveIceberg->deleteFiles.size(), 4);
  EXPECT_EQ(hiveIceberg->deleteFiles[0].content, FC::kData);
  EXPECT_EQ(hiveIceberg->deleteFiles[1].content, FC::kPositionalDeletes);
  EXPECT_EQ(hiveIceberg->deleteFiles[2].content, FC::kEqualityDeletes);
  EXPECT_EQ(hiveIceberg->deleteFiles[3].content, FC::kDeletionVector);

  // The delete file's own sequence number drives V2 delete applicability and
  // must survive the conversion independently of the split's.
  EXPECT_EQ(hiveIceberg->deleteFiles[0].dataSequenceNumber, 4);
}

TEST_F(
    PrestoToVeloxConnectorTest,
    toVeloxSplitTreatsPuffinDeleteAsDeletionVector) {
  // Older iceberg-api releases report deletion vectors as POSITION_DELETES in
  // Puffin format. Routing on content alone would send them to the positional
  // reader, which has no Puffin reader factory registered.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.deletes = {makeProtocolDeleteFile(
      protocol::iceberg::FileContent::POSITION_DELETES,
      protocol::iceberg::FileFormat::PUFFIN,
      "/deletes/legacy-dv.puffin")};

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  ASSERT_EQ(hiveIceberg->deleteFiles.size(), 1);
  EXPECT_EQ(
      hiveIceberg->deleteFiles[0].content,
      connector::hive::iceberg::FileContent::kDeletionVector);
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitCarriesFirstRowId) {
  // V3 row lineage: firstRowId defaults to -1 and is only surfaced when the
  // planner assigned one.
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  split.firstRowId = 900;

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  const auto it = hiveIceberg->infoColumns.find(
      connector::hive::iceberg::IcebergMetadataColumn::kFirstRowIdInfoColumn);
  ASSERT_NE(it, hiveIceberg->infoColumns.end());
  EXPECT_EQ(it->second, "900");
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitOmitsUnassignedFirstRowId) {
  protocol::iceberg::IcebergSplit split;
  split.path = "/path/to/data/file.dwrf";
  split.fileFormat = protocol::iceberg::FileFormat::ORC;
  // Left at the -1 default.

  protocol::SplitContext context;
  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto veloxSplit = icebergConnector.toVeloxSplit("iceberg", &split, &context);
  auto* hiveIceberg = dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
      veloxSplit.get());
  ASSERT_NE(hiveIceberg, nullptr);

  EXPECT_EQ(
      hiveIceberg->infoColumns.count(
          connector::hive::iceberg::IcebergMetadataColumn::
              kFirstRowIdInfoColumn),
      0);
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxSplitMapsWriteOrientedFileFormats) {
  // DWRF and NIMBLE reach the bridge directly once the protocol enum carries
  // them; before that regen they were silently coerced to ORC.
  struct TestCase {
    protocol::iceberg::FileFormat protocolFormat;
    dwio::common::FileFormat expected;
  };
  const std::vector<TestCase> cases = {
      {protocol::iceberg::FileFormat::DWRF, dwio::common::FileFormat::DWRF},
      {protocol::iceberg::FileFormat::NIMBLE, dwio::common::FileFormat::NIMBLE},
      {protocol::iceberg::FileFormat::PARQUET,
       dwio::common::FileFormat::PARQUET},
  };

  for (const auto& testCase : cases) {
    protocol::iceberg::IcebergSplit split;
    split.path = "/path/to/data/file";
    split.fileFormat = testCase.protocolFormat;

    protocol::SplitContext context;
    IcebergPrestoToVeloxConnector icebergConnector("iceberg");
    auto veloxSplit =
        icebergConnector.toVeloxSplit("iceberg", &split, &context);
    auto* hiveIceberg =
        dynamic_cast<connector::hive::iceberg::HiveIcebergSplit*>(
            veloxSplit.get());
    ASSERT_NE(hiveIceberg, nullptr);
    EXPECT_EQ(hiveIceberg->fileFormat, testCase.expected)
        << "protocol format ordinal "
        << fmt::underlying(testCase.protocolFormat);
  }
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxColumnHandleCarriesDefaultValue) {
  // Iceberg V3 column defaults: a column added after the file was written has
  // no physical data, so the reader materializes this constant instead.
  protocol::iceberg::IcebergColumnHandle column;
  column.columnIdentity.name = "added_col";
  column.columnIdentity.id = 7;
  column.columnIdentity.typeCategory =
      protocol::iceberg::TypeCategory::PRIMITIVE;
  column.type = "integer";
  column.columnType = protocol::hive::ColumnType::REGULAR;
  column.defaultValue = std::make_shared<protocol::String>("42");

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto handle = icebergConnector.toVeloxColumnHandle(&column, *typeParser_);
  ASSERT_NE(handle, nullptr);

  auto* icebergHandle =
      dynamic_cast<connector::hive::iceberg::IcebergColumnHandle*>(
          handle.get());
  ASSERT_NE(icebergHandle, nullptr);
  ASSERT_TRUE(icebergHandle->initialDefaultValue().has_value());
  EXPECT_EQ(*icebergHandle->initialDefaultValue(), "42");
}

TEST_F(PrestoToVeloxConnectorTest, icebergWriteHandleMapsOrcToDwrf) {
  // On the write path Iceberg "ORC" means Meta's DWRF, since Velox only
  // registers a DWRF writer. The read path deliberately maps ORC -> ORC
  // instead, so this branch is only reachable through a write handle.
  auto protoHandle =
      std::make_shared<protocol::iceberg::IcebergInsertTableHandle>();
  protoHandle->_type = "hive-iceberg";
  protoHandle->outputPath = "/path/to/table";
  protoHandle->fileFormat = protocol::iceberg::FileFormat::ORC;
  protoHandle->compressionCodec = protocol::hive::HiveCompressionCodec::NONE;
  protoHandle->inputColumns = {makeIcebergIdColumn()};

  protocol::InsertHandle insertHandle;
  insertHandle.handle.connectorHandle = protoHandle;
  insertHandle.handle.connectorId = "iceberg";

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto result =
      icebergConnector.toVeloxInsertTableHandle(&insertHandle, *typeParser_);
  ASSERT_NE(result, nullptr);

  auto* icebergInsert =
      dynamic_cast<connector::hive::iceberg::IcebergInsertTableHandle*>(
          result.get());
  ASSERT_NE(icebergInsert, nullptr);
  EXPECT_EQ(icebergInsert->storageFormat(), dwio::common::FileFormat::DWRF);
}

TEST_F(PrestoToVeloxConnectorTest, toVeloxColumnHandleCarriesTypeAttributes) {
  // Iceberg V3 type attributes ride alongside the field id and drive how the
  // reader interprets physical values (long vs int width, timestamp unit,
  // struct-vs-map encoding).
  protocol::iceberg::IcebergColumnHandle column;
  column.columnIdentity.name = "ts";
  column.columnIdentity.id = 5;
  column.columnIdentity.typeCategory =
      protocol::iceberg::TypeCategory::PRIMITIVE;
  column.type = "bigint";
  column.columnType = protocol::hive::ColumnType::REGULAR;

  auto attributes =
      std::make_shared<protocol::iceberg::IcebergTypeAttributes>();
  attributes->required = std::make_shared<bool>(true);
  attributes->longType = std::make_shared<protocol::String>("TIMESTAMP");
  attributes->timestampUnit =
      std::make_shared<protocol::String>("MICROSECONDS");
  attributes->structType = std::make_shared<protocol::String>("STRUCT");
  column.columnIdentity.typeAttributes = attributes;

  IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  auto handle = icebergConnector.toVeloxColumnHandle(&column, *typeParser_);
  ASSERT_NE(handle, nullptr);

  auto* icebergHandle =
      dynamic_cast<connector::hive::iceberg::IcebergColumnHandle*>(
          handle.get());
  ASSERT_NE(icebergHandle, nullptr);

  const auto& metadata = icebergHandle->icebergMetadata();
  EXPECT_EQ(metadata.longType, "TIMESTAMP");
  EXPECT_EQ(metadata.timestampUnit, "MICROSECONDS");
  EXPECT_EQ(metadata.structType, "STRUCT");
  EXPECT_TRUE(metadata.required);
}
