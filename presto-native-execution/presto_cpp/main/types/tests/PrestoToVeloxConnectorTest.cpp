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
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/presto_protocol/connector/hive/HiveConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/iceberg/IcebergConnectorProtocol.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"

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
