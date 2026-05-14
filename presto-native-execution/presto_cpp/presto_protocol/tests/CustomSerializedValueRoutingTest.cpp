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
#include <gtest/gtest.h>

#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/encode/Base64.h"
using namespace facebook::presto::protocol;

static void writeUTF(std::ostringstream& oss, const std::string& s) {
  uint16_t len = htons(static_cast<uint16_t>(s.length()));
  oss.write(reinterpret_cast<const char*>(&len), sizeof(len));
  oss.write(s.data(), s.length());
}

static std::string readUTF(std::istringstream& iss) {
  uint16_t len;
  iss.read(reinterpret_cast<char*>(&len), sizeof(len));
  len = ntohs(len);
  std::string s(len, '\0');
  iss.read(&s[0], len);
  return s;
}

// Dummy to_json / from_json stubs so that ConnectorProtocolTemplate compiles.
// These paths are never exercised — customSerializedValue routes via
// deserialize() instead.
#define DUMMY_JSON_SERDE(Type)                                \
  void to_json(json&, const Type&) {                          \
    VELOX_NYI("Not needed for customSerializedValueRouting test"); \
  }                                                           \
  void from_json(const json&, Type&) {                        \
    VELOX_NYI("Not needed for customSerializedValueRouting test"); \
  }

// ---------------------------------------------------------------------------
// All handle types use a simple single-string identifier to prove routing
// dispatched to deserialize().
// ---------------------------------------------------------------------------

#define DEFINE_TEST_HANDLE(Name, Base)            \
  struct Name : public Base {                     \
    std::string data;                             \
    static std::string serialize(const Name& h) { \
      std::ostringstream oss;                     \
      writeUTF(oss, h.data);                      \
      return oss.str();                           \
    }                                             \
    static std::shared_ptr<Name> deserialize(     \
        const std::string& data,                  \
        std::shared_ptr<Name>) {                  \
      auto r = std::make_shared<Name>();          \
      std::istringstream iss(data);               \
      r->data = readUTF(iss);                     \
      return r;                                   \
    }                                             \
  };                                              \
  DUMMY_JSON_SERDE(Name)

DEFINE_TEST_HANDLE(TestTableHandle, ConnectorTableHandle)
DEFINE_TEST_HANDLE(TestTableLayoutHandle, ConnectorTableLayoutHandle)
DEFINE_TEST_HANDLE(TestColumnHandle, ColumnHandle)
DEFINE_TEST_HANDLE(TestInsertTableHandle, ConnectorInsertTableHandle)
DEFINE_TEST_HANDLE(TestOutputTableHandle, ConnectorOutputTableHandle)
DEFINE_TEST_HANDLE(TestSplit, ConnectorSplit)
DEFINE_TEST_HANDLE(TestPartitioningHandle, ConnectorPartitioningHandle)
DEFINE_TEST_HANDLE(TestTransactionHandle, ConnectorTransactionHandle)
DEFINE_TEST_HANDLE(TestDeleteTableHandle, ConnectorDeleteTableHandle)
DEFINE_TEST_HANDLE(TestIndexHandle, ConnectorIndexHandle)

#undef DEFINE_TEST_HANDLE
#undef DUMMY_JSON_SERDE

// ---------------------------------------------------------------------------
// Test connector protocol using ConnectorProtocolTemplate
// ---------------------------------------------------------------------------

using TestConnectorProtocol = ConnectorProtocolTemplate<
    TestTableHandle,
    TestTableLayoutHandle,
    TestColumnHandle,
    TestInsertTableHandle,
    TestOutputTableHandle,
    TestSplit,
    TestPartitioningHandle,
    TestTransactionHandle,
    NotImplemented,
    TestDeleteTableHandle,
    TestIndexHandle>;

static const std::string kTestConnector = "test-connector";

class CustomSerializedRoutingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registerConnectorProtocol(
        kTestConnector, std::make_unique<TestConnectorProtocol>());
  }
  void TearDown() override {
    unregisterConnectorProtocol(kTestConnector);
  }
};

static json makeJson(const std::string& type, const std::string& data) {
  return json{
      {"@type", type},
      {"customSerializedValue",
       facebook::velox::encoding::Base64::encode(data)}};
}

// Routing tests for customSerializedValue in from_json().
// Each test verifies that a connector handle type is correctly routed to
// ConnectorProtocol::deserialize() when the JSON contains a
// customSerializedValue.
// ---------------------------------------------------------------------------

#define TEST_ROUTING(name, Base, Concrete)                          \
  TEST_F(CustomSerializedRoutingTest, name) {                       \
    std::ostringstream oss;                                         \
    writeUTF(oss, "test_data");                                     \
    std::shared_ptr<Base> h;                                        \
    from_json(makeJson(kTestConnector, oss.str()), h);              \
    EXPECT_EQ("test_data", dynamic_cast<Concrete*>(h.get())->data); \
  }

TEST_ROUTING(connectorTableHandle, ConnectorTableHandle, TestTableHandle)
TEST_ROUTING(
    connectorTableLayoutHandle,
    ConnectorTableLayoutHandle,
    TestTableLayoutHandle)
TEST_ROUTING(columnHandle, ColumnHandle, TestColumnHandle)
TEST_ROUTING(
    connectorInsertTableHandle,
    ConnectorInsertTableHandle,
    TestInsertTableHandle)
TEST_ROUTING(
    connectorOutputTableHandle,
    ConnectorOutputTableHandle,
    TestOutputTableHandle)
TEST_ROUTING(
    connectorPartitioningHandle,
    ConnectorPartitioningHandle,
    TestPartitioningHandle)
TEST_ROUTING(
    connectorTransactionHandle,
    ConnectorTransactionHandle,
    TestTransactionHandle)
TEST_ROUTING(
    connectorDeleteTableHandle,
    ConnectorDeleteTableHandle,
    TestDeleteTableHandle)
TEST_ROUTING(connectorIndexHandle, ConnectorIndexHandle, TestIndexHandle)
TEST_ROUTING(connectorSplit, ConnectorSplit, TestSplit)

#undef TEST_ROUTING
