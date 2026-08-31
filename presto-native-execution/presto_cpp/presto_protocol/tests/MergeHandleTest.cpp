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

#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"

using namespace facebook::presto::protocol;

namespace {

struct TestConnectorTableHandle : ConnectorTableHandle {
  std::string table;
};

struct TestConnectorTransactionHandle : ConnectorTransactionHandle {
  std::string transaction;
};

struct TestConnectorMergeTableHandle : ConnectorMergeTableHandle {
  std::string marker;
};

void to_json(json& j, const TestConnectorTableHandle& p) {
  j = json{{"@type", p._type}, {"table", p.table}};
}

void from_json(const json& j, TestConnectorTableHandle& p) {
  p._type = j.at("@type");
  p.table = j.at("table");
}

void to_json(json& j, const TestConnectorTransactionHandle& p) {
  j = json{{"@type", p._type}, {"transaction", p.transaction}};
}

void from_json(const json& j, TestConnectorTransactionHandle& p) {
  p._type = j.at("@type");
  p.transaction = j.at("transaction");
}

void to_json(json& j, const TestConnectorMergeTableHandle& p) {
  j = json{{"@type", p._type}, {"marker", p.marker}};
}

void from_json(const json& j, TestConnectorMergeTableHandle& p) {
  p._type = j.at("@type");
  p.marker = j.at("marker");
}

using TestConnectorProtocol = ConnectorProtocolTemplate<
    TestConnectorTableHandle,
    NotImplemented,
    NotImplemented,
    NotImplemented,
    NotImplemented,
    NotImplemented,
    NotImplemented,
    TestConnectorTransactionHandle,
    NotImplemented,
    NotImplemented,
    NotImplemented,
    TestConnectorMergeTableHandle>;

constexpr const char* kTestConnector = "test-merge-connector";

class MergeHandleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registerConnectorProtocol(
        kTestConnector, std::make_unique<TestConnectorProtocol>());
  }

  void TearDown() override {
    unregisterConnectorProtocol(kTestConnector);
  }
};

json makeMergeHandleJson(bool includeType) {
  // connectorTableLayout is deliberately omitted rather than set to null:
  // TableHandle::from_json deserializes the key unconditionally, so an explicit
  // null throws json type_error.305 instead of being treated as absent.
  json j = {
      {"tableHandle",
       {{"connectorId", kTestConnector},
        {"connectorHandle", {{"@type", kTestConnector}, {"table", "orders"}}},
        {"transaction",
         {{"@type", kTestConnector}, {"transaction", "txn-1"}}}}},
      {"connectorMergeTableHandle",
       {{"@type", kTestConnector}, {"marker", "merge-marker"}}}};
  if (includeType) {
    j["@type"] = "MergeHandle";
  }
  return j;
}

} // namespace

TEST_F(MergeHandleTest, missingTypeDefaultsToMergeHandle) {
  MergeHandle handle = makeMergeHandleJson(false);

  ASSERT_EQ(handle._type, "MergeHandle");
  ASSERT_EQ(handle.tableHandle.connectorId, kTestConnector);

  auto tableHandle = std::dynamic_pointer_cast<TestConnectorTableHandle>(
      handle.tableHandle.connectorHandle);
  ASSERT_NE(tableHandle, nullptr);
  ASSERT_EQ(tableHandle->table, "orders");

  auto transactionHandle =
      std::dynamic_pointer_cast<TestConnectorTransactionHandle>(
          handle.tableHandle.transaction);
  ASSERT_NE(transactionHandle, nullptr);
  ASSERT_EQ(transactionHandle->transaction, "txn-1");
}

TEST_F(
    MergeHandleTest,
    connectorMergeTableHandleRoutesThroughConnectorProtocol) {
  MergeHandle handle = makeMergeHandleJson(true);

  auto mergeHandle = std::dynamic_pointer_cast<TestConnectorMergeTableHandle>(
      handle.connectorMergeTableHandle);
  ASSERT_NE(mergeHandle, nullptr);
  ASSERT_EQ(mergeHandle->_type, kTestConnector);
  ASSERT_EQ(mergeHandle->marker, "merge-marker");
}
