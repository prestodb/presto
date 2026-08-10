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
#include "presto_cpp/main/connectors/DeltaPrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/HivePrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/IcebergPrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/presto_protocol/connector/hive/HiveConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/iceberg/IcebergConnectorProtocol.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneRegistration.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Filter.h"

using namespace facebook::presto;
using namespace facebook::velox;

class PrestoToVeloxConnectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registerTimestampWithTimeZoneType();
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    rootPool_ = memory::memoryManager()->addRootPool();
    pool_ = memory::memoryManager()->addLeafPool();
    typeParser_ = std::make_unique<TypeParser>();
    exprConverter_ =
        std::make_unique<VeloxExprConverter>(pool_.get(), typeParser_.get());
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
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

TEST_F(PrestoToVeloxConnectorTest, hiveTableHandleWithMissingLayout) {
  auto hiveHandle = std::make_shared<protocol::hive::HiveTableHandle>();
  hiveHandle->schemaName = "test_schema";
  hiveHandle->tableName = "test_table";

  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "hive";
  tableHandle.connectorHandle = hiveHandle;
  // 'connectorTableLayout' is deliberately left unset.

  const HivePrestoToVeloxConnector hiveConnector("hive");
  VELOX_ASSERT_THROW(
      hiveConnector.toVeloxTableHandle(
          tableHandle, *exprConverter_, *typeParser_),
      "Missing table layout");
}

TEST_F(PrestoToVeloxConnectorTest, icebergTableHandleWithMissingLayout) {
  auto icebergHandle =
      std::make_shared<protocol::iceberg::IcebergTableHandle>();
  icebergHandle->schemaName = "test_schema";

  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "iceberg";
  tableHandle.connectorHandle = icebergHandle;
  // 'connectorTableLayout' is deliberately left unset.

  const IcebergPrestoToVeloxConnector icebergConnector("iceberg");
  VELOX_ASSERT_THROW(
      icebergConnector.toVeloxTableHandle(
          tableHandle, *exprConverter_, *typeParser_),
      "Missing table layout");
}

namespace {

// Column mapping renames "id" to "col-1a" in the data files; "ds" and "ts" are
// stored under their logical names.
constexpr auto kDeltaPhysicalName = "col-1a";

protocol::delta::DeltaColumnHandle createDeltaColumnHandle(
    const std::string& name,
    const std::string& dataType,
    protocol::delta::ColumnType columnType,
    const std::string& physicalName = "") {
  protocol::delta::DeltaColumnHandle column;
  column.name = name;
  column.dataType = dataType;
  column.columnType = columnType;
  if (!physicalName.empty()) {
    column.physicalName = std::make_shared<std::string>(physicalName);
  }
  return column;
}

protocol::delta::DeltaColumn createDeltaColumn(
    const std::string& logicalName,
    const std::string& type,
    bool partition,
    const std::string& physicalName = "") {
  protocol::delta::DeltaColumn column;
  column.logicalName = logicalName;
  column.type = type;
  column.partition = partition;
  if (!physicalName.empty()) {
    column.physicalName = std::make_shared<std::string>(physicalName);
  }
  return column;
}

std::shared_ptr<protocol::delta::DeltaTableHandle> createDeltaTableHandle() {
  auto deltaHandle = std::make_shared<protocol::delta::DeltaTableHandle>();
  deltaHandle->connectorId = "delta";
  deltaHandle->deltaTable.schemaName = "test_schema";
  deltaHandle->deltaTable.tableName = "test_table";
  deltaHandle->deltaTable.columns = {
      createDeltaColumn("id", "bigint", false, kDeltaPhysicalName),
      createDeltaColumn("ts", "timestamp", false),
      createDeltaColumn("ds", "date", true)};
  return deltaHandle;
}

// Domain matching values greater than 'lowerBound'.
protocol::Domain createBigintRangeDomain(
    int64_t lowerBound,
    memory::MemoryPool* pool) {
  return createSingleRangeDomain(
      "bigint",
      serializeToBlock(
          BaseVector::createConstant(BIGINT(), variant(lowerBound), 1, pool),
          pool),
      protocol::Bound::ABOVE,
      nullptr,
      protocol::Bound::BELOW,
      false);
}

using DeltaDomains =
    protocol::Map<protocol::delta::DeltaColumnHandle, protocol::Domain>;

// Domain matching 'value' <= x, for any type whose values can be serialized
// into a single-element block.
protocol::Domain createAtLeastDomain(
    const std::string& typeName,
    const VectorPtr& value,
    memory::MemoryPool* pool) {
  return createSingleRangeDomain(
      typeName,
      serializeToBlock(value, pool),
      protocol::Bound::EXACTLY,
      nullptr,
      protocol::Bound::BELOW,
      false);
}

// Domain matching an IN list of bigints, i.e. several single-value ranges.
protocol::Domain createBigintValuesDomain(
    const std::vector<int64_t>& values,
    memory::MemoryPool* pool) {
  auto rangeSet = std::make_shared<protocol::SortedRangeSet>();
  rangeSet->type = "bigint";
  for (auto value : values) {
    auto block = serializeToBlock(
        BaseVector::createConstant(BIGINT(), variant(value), 1, pool), pool);

    protocol::Marker low;
    low.type = "bigint";
    low.valueBlock = block;
    low.bound = protocol::Bound::EXACTLY;

    protocol::Marker high;
    high.type = "bigint";
    high.valueBlock = block;
    high.bound = protocol::Bound::EXACTLY;

    protocol::Range range;
    range.low = low;
    range.high = high;
    rangeSet->ranges.push_back(range);
  }

  protocol::Domain domain;
  domain.values = rangeSet;
  domain.nullAllowed = false;
  return domain;
}

// 'IS NULL' arrives as an empty range set with nulls allowed.
protocol::Domain createIsNullDomain(const std::string& typeName) {
  auto rangeSet = std::make_shared<protocol::SortedRangeSet>();
  rangeSet->type = typeName;

  protocol::Domain domain;
  domain.values = rangeSet;
  domain.nullAllowed = true;
  return domain;
}

// 'IS NOT NULL' arrives as an unbounded range with nulls not allowed.
protocol::Domain createIsNotNullDomain(const std::string& typeName) {
  return createSingleRangeDomain(
      typeName,
      nullptr,
      protocol::Bound::ABOVE,
      nullptr,
      protocol::Bound::BELOW,
      false);
}

// Runs 'domains' through the Delta bridge as the layout predicate of
// 'createDeltaTableHandle()' and returns the resulting Hive table handle.
std::unique_ptr<connector::ConnectorTableHandle> convertDeltaTableHandle(
    std::shared_ptr<DeltaDomains> domains,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "delta";
  tableHandle.connectorHandle = createDeltaTableHandle();
  if (domains != nullptr) {
    auto layout = std::make_shared<protocol::delta::DeltaTableLayoutHandle>();
    layout->predicate.domains = std::move(domains);
    tableHandle.connectorTableLayout = layout;
  }

  DeltaPrestoToVeloxConnector deltaConnector("delta");
  return deltaConnector.toVeloxTableHandle(
      tableHandle, exprConverter, typeParser);
}

const connector::hive::HiveTableHandle& asHiveTableHandle(
    const connector::ConnectorTableHandle& handle) {
  return dynamic_cast<const connector::hive::HiveTableHandle&>(handle);
}

const common::Filter* findFilter(
    const common::SubfieldFilters& filters,
    const std::string& path) {
  auto it = filters.find(common::Subfield(path));
  return it == filters.end() ? nullptr : it->second.get();
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, deltaTableHandleUsesPhysicalColumnNames) {
  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "delta";
  tableHandle.connectorHandle = createDeltaTableHandle();

  DeltaPrestoToVeloxConnector deltaConnector("delta");
  auto result = deltaConnector.toVeloxTableHandle(
      tableHandle, *exprConverter_, *typeParser_);

  auto* handle = dynamic_cast<connector::hive::HiveTableHandle*>(result.get());
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(handle->tableName(), "test_schema.test_table");

  // Partition columns are not in the data files, so they are excluded from
  // dataColumns; the remaining columns use the names the files use.
  auto dataColumns = handle->dataColumns();
  ASSERT_NE(dataColumns, nullptr);
  ASSERT_EQ(dataColumns->size(), 2);
  EXPECT_EQ(dataColumns->nameOf(0), kDeltaPhysicalName);
  EXPECT_EQ(dataColumns->nameOf(1), "ts");

  // Partition columns are still passed as column handles so the reader can
  // supply them as constants.
  const auto columnHandles = handle->filterColumnHandles();
  ASSERT_EQ(columnHandles.size(), 3);
  const auto& partitionColumn = columnHandles[2];
  EXPECT_EQ(partitionColumn->name(), "ds");
  EXPECT_EQ(
      partitionColumn->columnType(),
      connector::hive::HiveColumnHandle::ColumnType::kPartitionKey);

  // Without a layout there is no predicate to push down.
  EXPECT_TRUE(handle->subfieldFilters().empty());
}

TEST_F(PrestoToVeloxConnectorTest, deltaTableHandlePushesDownDataPredicate) {
  auto layout = std::make_shared<protocol::delta::DeltaTableLayoutHandle>();
  layout->predicate.domains = std::make_shared<
      protocol::Map<protocol::delta::DeltaColumnHandle, protocol::Domain>>();
  // Pushed down keyed on the physical name.
  layout->predicate.domains->emplace(
      createDeltaColumnHandle(
          "id",
          "bigint",
          protocol::delta::ColumnType::REGULAR,
          kDeltaPhysicalName),
      createBigintRangeDomain(5, pool_.get()));
  // Enforced by split generation on the coordinator; not pushed down.
  layout->predicate.domains->emplace(
      createDeltaColumnHandle(
          "ds", "date", protocol::delta::ColumnType::PARTITION),
      createBigintRangeDomain(0, pool_.get()));
  // Timestamps are adjusted to the session timezone while decoding, so their
  // predicates stay in the filter above the scan.
  layout->predicate.domains->emplace(
      createDeltaColumnHandle(
          "ts", "timestamp", protocol::delta::ColumnType::REGULAR),
      createBigintRangeDomain(0, pool_.get()));
  // Same for the packed TIMESTAMP WITH TIME ZONE representation.
  layout->predicate.domains->emplace(
      createDeltaColumnHandle(
          "tstz",
          "timestamp with time zone",
          protocol::delta::ColumnType::REGULAR),
      createBigintRangeDomain(0, pool_.get()));

  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "delta";
  tableHandle.connectorHandle = createDeltaTableHandle();
  tableHandle.connectorTableLayout = layout;

  DeltaPrestoToVeloxConnector deltaConnector("delta");
  auto result = deltaConnector.toVeloxTableHandle(
      tableHandle, *exprConverter_, *typeParser_);

  auto* handle = dynamic_cast<connector::hive::HiveTableHandle*>(result.get());
  ASSERT_NE(handle, nullptr);

  const auto& filters = handle->subfieldFilters();
  ASSERT_EQ(filters.size(), 1);
  auto it = filters.find(common::Subfield(kDeltaPhysicalName));
  ASSERT_NE(it, filters.end());
  EXPECT_FALSE(it->second->testInt64(5));
  EXPECT_TRUE(it->second->testInt64(6));
  EXPECT_EQ(handle->remainingFilter(), nullptr);
}

TEST_F(PrestoToVeloxConnectorTest, deltaColumnHandleUsesPhysicalName) {
  auto deltaColumn = createDeltaColumnHandle(
      "id", "bigint", protocol::delta::ColumnType::REGULAR, kDeltaPhysicalName);

  DeltaPrestoToVeloxConnector deltaConnector("delta");
  auto handle = deltaConnector.toVeloxColumnHandle(&deltaColumn, *typeParser_);

  auto* hiveColumn =
      dynamic_cast<connector::hive::HiveColumnHandle*>(handle.get());
  ASSERT_NE(hiveColumn, nullptr);
  EXPECT_EQ(hiveColumn->name(), kDeltaPhysicalName);
  EXPECT_EQ(
      hiveColumn->columnType(),
      connector::hive::HiveColumnHandle::ColumnType::kRegular);
  EXPECT_EQ(hiveColumn->dataType(), BIGINT());
  EXPECT_TRUE(hiveColumn->requiredSubfields().empty());
}

TEST_F(PrestoToVeloxConnectorTest, deltaColumnHandleDatePartition) {
  auto deltaColumn = createDeltaColumnHandle(
      "ds", "date", protocol::delta::ColumnType::PARTITION);

  DeltaPrestoToVeloxConnector deltaConnector("delta");
  auto handle = deltaConnector.toVeloxColumnHandle(&deltaColumn, *typeParser_);

  auto* hiveColumn =
      dynamic_cast<connector::hive::HiveColumnHandle*>(handle.get());
  ASSERT_NE(hiveColumn, nullptr);
  EXPECT_EQ(hiveColumn->name(), "ds");
  EXPECT_EQ(
      hiveColumn->columnType(),
      connector::hive::HiveColumnHandle::ColumnType::kPartitionKey);
  // Delta writes date partition values as ISO8601 (YYYY-MM-DD), not as days
  // since epoch.
  EXPECT_FALSE(hiveColumn->isPartitionDateValueDaysSinceEpoch());
}

TEST_F(PrestoToVeloxConnectorTest, deltaPushesDownNonIntegerPredicates) {
  // toFilter() has a branch per type; cover the ones a Delta scan can reach
  // besides bigint.
  auto domains = std::make_shared<DeltaDomains>();
  domains->emplace(
      createDeltaColumnHandle(
          "name", "varchar", protocol::delta::ColumnType::REGULAR),
      createAtLeastDomain(
          "varchar",
          BaseVector::createConstant(
              VARCHAR(),
              variant::create<TypeKind::VARCHAR>(std::string("b")),
              1,
              pool_.get()),
          pool_.get()));
  domains->emplace(
      createDeltaColumnHandle(
          "day", "date", protocol::delta::ColumnType::REGULAR),
      createAtLeastDomain(
          "date",
          BaseVector::createConstant(DATE(), variant(18'000), 1, pool_.get()),
          pool_.get()));
  domains->emplace(
      createDeltaColumnHandle(
          "flag", "boolean", protocol::delta::ColumnType::REGULAR),
      createAtLeastDomain(
          "boolean",
          BaseVector::createConstant(BOOLEAN(), variant(true), 1, pool_.get()),
          pool_.get()));
  domains->emplace(
      createDeltaColumnHandle(
          "amount", "decimal(20,0)", protocol::delta::ColumnType::REGULAR),
      createAtLeastDomain(
          "decimal(20,0)",
          BaseVector::createConstant(
              DECIMAL(20, 0), variant((int128_t)100), 1, pool_.get()),
          pool_.get()));

  auto result = convertDeltaTableHandle(domains, *exprConverter_, *typeParser_);
  const auto& filters = asHiveTableHandle(*result).subfieldFilters();
  ASSERT_EQ(filters.size(), 4);

  const auto* name = findFilter(filters, "name");
  ASSERT_NE(name, nullptr);
  EXPECT_FALSE(name->testBytes("a", 1));
  EXPECT_TRUE(name->testBytes("b", 1));
  EXPECT_TRUE(name->testBytes("c", 1));

  // DATE is decoded as days since epoch, so the filter tests integers.
  const auto* day = findFilter(filters, "day");
  ASSERT_NE(day, nullptr);
  EXPECT_FALSE(day->testInt64(17'999));
  EXPECT_TRUE(day->testInt64(18'000));

  const auto* flag = findFilter(filters, "flag");
  ASSERT_NE(flag, nullptr);
  EXPECT_FALSE(flag->testBool(false));
  EXPECT_TRUE(flag->testBool(true));

  // A long decimal is decoded as HUGEINT.
  const auto* amount = findFilter(filters, "amount");
  ASSERT_NE(amount, nullptr);
  EXPECT_FALSE(amount->testInt128(99));
  EXPECT_TRUE(amount->testInt128(100));
}

TEST_F(PrestoToVeloxConnectorTest, deltaPushesDownInListPredicate) {
  // 'id IN (7, 9)' arrives as several single-value ranges, which collapse into
  // one values filter rather than a range.
  auto domains = std::make_shared<DeltaDomains>();
  domains->emplace(
      createDeltaColumnHandle(
          "id",
          "bigint",
          protocol::delta::ColumnType::REGULAR,
          kDeltaPhysicalName),
      createBigintValuesDomain({7, 9}, pool_.get()));

  auto result = convertDeltaTableHandle(domains, *exprConverter_, *typeParser_);
  const auto& filters = asHiveTableHandle(*result).subfieldFilters();
  ASSERT_EQ(filters.size(), 1);

  const auto* id = findFilter(filters, kDeltaPhysicalName);
  ASSERT_NE(id, nullptr);
  EXPECT_TRUE(id->testInt64(7));
  EXPECT_FALSE(id->testInt64(8));
  EXPECT_TRUE(id->testInt64(9));
  EXPECT_FALSE(id->testNull());
  // Row groups whose [min, max] misses both values are skipped.
  EXPECT_FALSE(id->testInt64Range(0, 6, false));
  EXPECT_TRUE(id->testInt64Range(0, 7, false));
}

TEST_F(PrestoToVeloxConnectorTest, deltaPushesDownNullPredicates) {
  auto domains = std::make_shared<DeltaDomains>();
  domains->emplace(
      createDeltaColumnHandle(
          "name", "varchar", protocol::delta::ColumnType::REGULAR),
      createIsNotNullDomain("varchar"));
  domains->emplace(
      createDeltaColumnHandle(
          "id",
          "bigint",
          protocol::delta::ColumnType::REGULAR,
          kDeltaPhysicalName),
      createIsNullDomain("bigint"));

  auto result = convertDeltaTableHandle(domains, *exprConverter_, *typeParser_);
  const auto& filters = asHiveTableHandle(*result).subfieldFilters();
  ASSERT_EQ(filters.size(), 2);

  const auto* name = findFilter(filters, "name");
  ASSERT_NE(name, nullptr);
  EXPECT_EQ(name->kind(), common::FilterKind::kIsNotNull);
  EXPECT_FALSE(name->testNull());
  EXPECT_TRUE(name->testBytes("a", 1));

  const auto* id = findFilter(filters, kDeltaPhysicalName);
  ASSERT_NE(id, nullptr);
  EXPECT_EQ(id->kind(), common::FilterKind::kIsNull);
  EXPECT_TRUE(id->testNull());
  EXPECT_FALSE(id->testInt64(1));
}

TEST_F(PrestoToVeloxConnectorTest, deltaPushesDownLogicalNameWithoutMapping) {
  // Without column mapping there is no physical name, so the file column is
  // named after the logical column.
  auto domains = std::make_shared<DeltaDomains>();
  domains->emplace(
      createDeltaColumnHandle(
          "cnt", "bigint", protocol::delta::ColumnType::REGULAR),
      createBigintRangeDomain(100, pool_.get()));

  auto result = convertDeltaTableHandle(domains, *exprConverter_, *typeParser_);
  const auto& filters = asHiveTableHandle(*result).subfieldFilters();
  ASSERT_EQ(filters.size(), 1);
  ASSERT_NE(findFilter(filters, "cnt"), nullptr);
  EXPECT_TRUE(findFilter(filters, "cnt")->testInt64(101));
}

TEST_F(PrestoToVeloxConnectorTest, deltaSubfieldPredicateIsNotPushedDown) {
  // A pushed-down subfield path is expressed in logical names, which column
  // mapping renames at every level, so it stays in the filter above the scan.
  auto domains = std::make_shared<DeltaDomains>();
  auto column = createDeltaColumnHandle(
      "a", "integer", protocol::delta::ColumnType::SUBFIELD);
  column.subfield = std::make_shared<protocol::Subfield>("a.ac.aca");
  domains->emplace(column, createBigintRangeDomain(6, pool_.get()));

  auto result = convertDeltaTableHandle(domains, *exprConverter_, *typeParser_);
  EXPECT_TRUE(asHiveTableHandle(*result).subfieldFilters().empty());
}

TEST_F(PrestoToVeloxConnectorTest, deltaNonePredicateIsNotPushedDown) {
  // TupleDomain::none() has no domain map; the coordinator turns such a plan
  // into an empty result, so there is nothing to push down.
  auto layout = std::make_shared<protocol::delta::DeltaTableLayoutHandle>();
  ASSERT_EQ(layout->predicate.domains, nullptr);

  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "delta";
  tableHandle.connectorHandle = createDeltaTableHandle();
  tableHandle.connectorTableLayout = layout;

  DeltaPrestoToVeloxConnector deltaConnector("delta");
  auto result = deltaConnector.toVeloxTableHandle(
      tableHandle, *exprConverter_, *typeParser_);
  EXPECT_TRUE(asHiveTableHandle(*result).subfieldFilters().empty());
}

TEST_F(PrestoToVeloxConnectorTest, deltaUnexpectedLayoutTypeThrows) {
  protocol::TableHandle tableHandle;
  tableHandle.connectorId = "delta";
  tableHandle.connectorHandle = createDeltaTableHandle();
  tableHandle.connectorTableLayout =
      std::make_shared<protocol::hive::HiveTableLayoutHandle>();

  DeltaPrestoToVeloxConnector deltaConnector("delta");
  VELOX_ASSERT_THROW(
      deltaConnector.toVeloxTableHandle(
          tableHandle, *exprConverter_, *typeParser_),
      "Unexpected table layout type");
}

namespace {

// Builds the scan spec the way HiveDataSource does, so a test can check that a
// pushed-down filter binds to the column the reader will actually read.
std::shared_ptr<common::ScanSpec> makeDeltaScanSpec(
    const common::SubfieldFilters& filters,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  return connector::hive::makeScanSpec(
      rowType,
      /*outputSubfields=*/{},
      filters,
      /*dataColumns=*/rowType,
      /*partitionKeys=*/{},
      /*infoColumns=*/{},
      /*specialColumns=*/{},
      /*disableStatsBasedFilterReorder=*/false,
      pool);
}

VectorPtr makeBigintRowVector(
    const RowTypePtr& rowType,
    int64_t first,
    vector_size_t size,
    memory::MemoryPool* pool) {
  auto values = BaseVector::create<FlatVector<int64_t>>(BIGINT(), size, pool);
  for (auto i = 0; i < size; ++i) {
    values->set(i, first + i);
  }
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, size, std::vector<VectorPtr>{values});
}

} // namespace

TEST_F(PrestoToVeloxConnectorTest, deltaFilterBindsToScanSpecColumn) {
  auto domains = std::make_shared<DeltaDomains>();
  domains->emplace(
      createDeltaColumnHandle(
          "id",
          "bigint",
          protocol::delta::ColumnType::REGULAR,
          kDeltaPhysicalName),
      createBigintRangeDomain(50, pool_.get()));

  auto result = convertDeltaTableHandle(domains, *exprConverter_, *typeParser_);
  auto rowType = ROW({kDeltaPhysicalName}, {BIGINT()});
  auto scanSpec = makeDeltaScanSpec(
      asHiveTableHandle(*result).subfieldFilters(), rowType, pool_.get());

  // The filter has to land on the scan spec node for the physical column;
  // keying it on the logical name would silently disable pushdown.
  auto* child = scanSpec->childByName(kDeltaPhysicalName);
  ASSERT_NE(child, nullptr);
  ASSERT_NE(child->filter(), nullptr);
  EXPECT_FALSE(child->filter()->testInt64(50));
  EXPECT_TRUE(child->filter()->testInt64(51));
  EXPECT_EQ(scanSpec->childByName("id"), nullptr);
}

TEST_F(PrestoToVeloxConnectorTest, deltaPredicateSkipsParquetRowGroups) {
  // End of the pushdown chain: the filter produced by the bridge is handed to
  // the Parquet reader, which drops the row group whose statistics cannot
  // match. Without pushdown both row groups would be read.
  auto rowType = ROW({kDeltaPhysicalName}, {BIGINT()});

  dwio::common::WriterOptions writerOptions;
  writerOptions.memoryPool = rootPool_.get();
  writerOptions.compressionKind = common::CompressionKind_NONE;
  // Start a new row group every 10 rows.
  writerOptions.flushPolicyFactory = []() {
    return std::make_unique<parquet::LambdaFlushPolicy>(
        /*rowsInRowGroup=*/10, /*bytesInRowGroup=*/1 << 20, []() {
          return false;
        });
  };

  auto sink = std::make_unique<dwio::common::MemorySink>(
      1 << 20, dwio::common::FileSink::Options{.pool = pool_.get()});
  auto* sinkPtr = sink.get();
  auto writer = std::make_unique<parquet::Writer>(
      std::move(sink), writerOptions, rowType);
  // Row group 1 holds 0..9, row group 2 holds 100..109.
  writer->write(makeBigintRowVector(rowType, 0, 10, pool_.get()));
  writer->write(makeBigintRowVector(rowType, 100, 10, pool_.get()));
  writer->close();

  const std::string fileContent(sinkPtr->data(), sinkPtr->size());

  auto readAll = [&](const common::SubfieldFilters& filters,
                     dwio::common::RuntimeStatistics& stats) {
    dwio::common::ReaderOptions readerOptions(pool_.get());
    auto reader = std::make_unique<parquet::ParquetReader>(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<InMemoryReadFile>(fileContent),
            readerOptions.memoryPool()),
        readerOptions);
    EXPECT_EQ(reader->fileMetaData().numRowGroups(), 2);

    dwio::common::RowReaderOptions rowReaderOptions;
    rowReaderOptions.select(
        std::make_shared<dwio::common::ColumnSelector>(
            rowType, rowType->names()));
    rowReaderOptions.setScanSpec(
        makeDeltaScanSpec(filters, rowType, pool_.get()));
    auto rowReader = reader->createRowReader(rowReaderOptions);

    VectorPtr result = BaseVector::create(rowType, 0, pool_.get());
    uint64_t rows = 0;
    while (rowReader->next(1'000, result)) {
      rows += result->size();
    }
    rowReader->updateRuntimeStats(stats);
    return rows;
  };

  // Control: no predicate, so both row groups are read.
  dwio::common::RuntimeStatistics withoutPushdown;
  EXPECT_EQ(readAll({}, withoutPushdown), 20);
  EXPECT_EQ(withoutPushdown.skippedStrides, 0);
  EXPECT_EQ(withoutPushdown.processedStrides, 2);

  // 'id > 50', pushed down as a filter keyed on the physical column name.
  auto domains = std::make_shared<DeltaDomains>();
  domains->emplace(
      createDeltaColumnHandle(
          "id",
          "bigint",
          protocol::delta::ColumnType::REGULAR,
          kDeltaPhysicalName),
      createBigintRangeDomain(50, pool_.get()));
  auto tableHandle =
      convertDeltaTableHandle(domains, *exprConverter_, *typeParser_);

  dwio::common::RuntimeStatistics withPushdown;
  // Only the second row group's rows survive the filter, and the first row
  // group is never read: its statistics say max(id) == 9.
  EXPECT_EQ(
      readAll(asHiveTableHandle(*tableHandle).subfieldFilters(), withPushdown),
      10);
  EXPECT_EQ(withPushdown.skippedStrides, 1);
  EXPECT_EQ(withPushdown.processedStrides, 1);
}
