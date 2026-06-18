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
