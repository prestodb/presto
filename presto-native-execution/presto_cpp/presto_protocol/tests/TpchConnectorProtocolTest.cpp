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

#include <arpa/inet.h>
#include <folly/Portability.h>
#include <gtest/gtest.h>
#include <cstring>
#include <sstream>
#include "presto_cpp/presto_protocol/connector/tpch/TpchConnectorProtocol.h"

using namespace facebook::presto;
using namespace facebook::presto::protocol;
using namespace facebook::presto::protocol::tpch;

class TpchConnectorProtocolTest : public ::testing::Test {
 protected:
  TpchConnectorProtocol protocol;
};

TEST_F(TpchConnectorProtocolTest, TestTpchTableHandleDeserialization) {
  std::ostringstream oss;

  std::string tableName = "customer";
  uint16_t nameLen = htons(static_cast<uint16_t>(tableName.length()));
  oss.write(reinterpret_cast<const char*>(&nameLen), 2);
  oss.write(tableName.data(), tableName.length());

  double scaleFactor = 0.01;
  uint64_t scaleFactorBits;
  std::memcpy(&scaleFactorBits, &scaleFactor, sizeof(double));
  scaleFactorBits = folly::Endian::big(scaleFactorBits);
  oss.write(reinterpret_cast<const char*>(&scaleFactorBits), 8);

  std::string binaryData = oss.str();

  std::shared_ptr<ConnectorTableHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto tpchHandle = std::dynamic_pointer_cast<TpchTableHandle>(handle);
  ASSERT_NE(tpchHandle, nullptr);
  EXPECT_EQ(tpchHandle->tableName, "customer");
  EXPECT_DOUBLE_EQ(tpchHandle->scaleFactor, 0.01);
}

TEST_F(TpchConnectorProtocolTest, TestTpchColumnHandleDeserialization) {
  std::ostringstream oss;

  std::string columnName = "c_custkey";
  uint16_t nameLen = htons(static_cast<uint16_t>(columnName.length()));
  oss.write(reinterpret_cast<const char*>(&nameLen), 2);
  oss.write(columnName.data(), columnName.length());

  std::string type = "bigint";
  uint16_t typeLen = htons(static_cast<uint16_t>(type.length()));
  oss.write(reinterpret_cast<const char*>(&typeLen), 2);
  oss.write(type.data(), type.length());

  uint32_t subfieldCount = htonl(0);
  oss.write(reinterpret_cast<const char*>(&subfieldCount), 4);

  std::string binaryData = oss.str();

  std::shared_ptr<ColumnHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto tpchHandle = std::dynamic_pointer_cast<TpchColumnHandle>(handle);
  ASSERT_NE(tpchHandle, nullptr);
  EXPECT_EQ(tpchHandle->columnName, "c_custkey");
  EXPECT_EQ(tpchHandle->type, "bigint");
}

TEST_F(TpchConnectorProtocolTest, TestColumnHandleWithSubfields) {
  std::ostringstream oss;

  std::string columnName = "complex_col";
  uint16_t nameLen = htons(static_cast<uint16_t>(columnName.length()));
  oss.write(reinterpret_cast<const char*>(&nameLen), 2);
  oss.write(columnName.data(), columnName.length());

  std::string type = "row(field1 bigint, field2 varchar)";
  uint16_t typeLen = htons(static_cast<uint16_t>(type.length()));
  oss.write(reinterpret_cast<const char*>(&typeLen), 2);
  oss.write(type.data(), type.length());

  uint32_t subfieldCount = htonl(2);
  oss.write(reinterpret_cast<const char*>(&subfieldCount), 4);

  std::string subfield1 = "field1";
  uint16_t sub1Len = htons(static_cast<uint16_t>(subfield1.length()));
  oss.write(reinterpret_cast<const char*>(&sub1Len), 2);
  oss.write(subfield1.data(), subfield1.length());

  std::string subfield2 = "field2";
  uint16_t sub2Len = htons(static_cast<uint16_t>(subfield2.length()));
  oss.write(reinterpret_cast<const char*>(&sub2Len), 2);
  oss.write(subfield2.data(), subfield2.length());

  std::string binaryData = oss.str();

  std::shared_ptr<ColumnHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto tpchHandle = std::dynamic_pointer_cast<TpchColumnHandle>(handle);
  ASSERT_NE(tpchHandle, nullptr);
  EXPECT_EQ(tpchHandle->columnName, "complex_col");
  EXPECT_EQ(tpchHandle->type, type);
  ASSERT_EQ(tpchHandle->requiredSubfields.size(), 2);
  EXPECT_EQ(tpchHandle->requiredSubfields[0], "field1");
  EXPECT_EQ(tpchHandle->requiredSubfields[1], "field2");
}

TEST_F(TpchConnectorProtocolTest, TestAllTpchTableNames) {
  std::vector<std::string> tableNames = {
      "customer",
      "lineitem",
      "nation",
      "orders",
      "part",
      "partsupp",
      "region",
      "supplier"};

  for (const auto& tableName : tableNames) {
    std::ostringstream oss;

    uint16_t nameLen = htons(static_cast<uint16_t>(tableName.length()));
    oss.write(reinterpret_cast<const char*>(&nameLen), 2);
    oss.write(tableName.data(), tableName.length());

    double scaleFactor = 1.0;
    uint64_t scaleFactorBits;
    std::memcpy(&scaleFactorBits, &scaleFactor, sizeof(double));
    scaleFactorBits = folly::Endian::big(scaleFactorBits);
    oss.write(reinterpret_cast<const char*>(&scaleFactorBits), 8);

    std::string binaryData = oss.str();

    std::shared_ptr<ConnectorTableHandle> handle;
    protocol.deserialize(binaryData, handle);

    auto tpchHandle = std::dynamic_pointer_cast<TpchTableHandle>(handle);
    ASSERT_NE(tpchHandle, nullptr);
    EXPECT_EQ(tpchHandle->tableName, tableName);
    EXPECT_DOUBLE_EQ(tpchHandle->scaleFactor, 1.0);
  }
}

TEST_F(TpchConnectorProtocolTest, TestVariousScaleFactors) {
  double scaleFactors[] = {0.01, 0.1, 1.0, 10.0, 100.0, 1000.0};

  for (double scaleFactor : scaleFactors) {
    std::ostringstream oss;

    std::string tableName = "lineitem";
    uint16_t nameLen = htons(static_cast<uint16_t>(tableName.length()));
    oss.write(reinterpret_cast<const char*>(&nameLen), 2);
    oss.write(tableName.data(), tableName.length());

    uint64_t scaleFactorBits;
    std::memcpy(&scaleFactorBits, &scaleFactor, sizeof(double));
    scaleFactorBits = folly::Endian::big(scaleFactorBits);
    oss.write(reinterpret_cast<const char*>(&scaleFactorBits), 8);

    std::string binaryData = oss.str();

    std::shared_ptr<ConnectorTableHandle> handle;
    protocol.deserialize(binaryData, handle);

    auto tpchHandle = std::dynamic_pointer_cast<TpchTableHandle>(handle);
    ASSERT_NE(tpchHandle, nullptr);
    EXPECT_EQ(tpchHandle->tableName, "lineitem");
    EXPECT_DOUBLE_EQ(tpchHandle->scaleFactor, scaleFactor);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
