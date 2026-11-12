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
#include <fstream>
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/presto_protocol/connector/arrow_federation/ArrowFederationConnectorProtocol.h"

using namespace facebook::presto::protocol;
using namespace facebook::presto::protocol::arrow_federation;

namespace {
namespace thrift = facebook::presto::thrift;

template <typename Base, typename Concrete>
json decodeHandle(
    const json& envelope,
    const std::string& bytes,
    const std::string Concrete::* member) {
  std::shared_ptr<Base> handle;
  from_json(envelope, handle);
  auto concrete = std::dynamic_pointer_cast<Concrete>(handle);
  EXPECT_NE(concrete, nullptr);
  if (!concrete) {
    return nullptr;
  }
  auto decoded = json::parse(folly::base64Decode(concrete.get()->*member));

  std::shared_ptr<Base> direct;
  getConnectorProtocol("mysql").deserialize(bytes, direct);
  auto directConcrete = std::dynamic_pointer_cast<Concrete>(direct);
  EXPECT_NE(directConcrete, nullptr);
  if (!directConcrete) {
    return nullptr;
  }
  EXPECT_EQ(concrete.get()->*member, directConcrete.get()->*member);
  auto legacyJson = decoded;
  legacyJson["@type"] = "mysql";
  std::shared_ptr<Base> legacy;
  from_json(legacyJson, legacy);
  auto legacyConcrete = std::dynamic_pointer_cast<Concrete>(legacy);
  EXPECT_NE(legacyConcrete, nullptr);
  if (legacyConcrete) {
    EXPECT_EQ(
        json::parse(folly::base64Decode(legacyConcrete.get()->*member)),
        legacyJson);
  }
  return decoded;
}

template <typename Thrift, typename Base, typename Concrete>
void checkThriftEnvelope(
    const json& fixture,
    const std::string Concrete::* member) {
  Thrift envelope;
  envelope.connectorId() = "mysql";
  envelope.customSerializedValue() =
      folly::base64Decode(fixture.at("thrift").get<std::string>());
  auto decodedEnvelope = std::make_shared<Thrift>();
  thriftRead(thriftWrite(envelope), decodedEnvelope);
  std::shared_ptr<Base> handle;
  thrift::fromThrift(*decodedEnvelope, handle);
  auto concrete = std::dynamic_pointer_cast<Concrete>(handle);
  ASSERT_NE(concrete, nullptr);
  EXPECT_EQ(
      json::parse(folly::base64Decode(concrete.get()->*member)),
      fixture.at("json"));
}
}

class JdbcThriftToJsonTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registerConnectorProtocol(
        "mysql", std::make_unique<JdbcArrowFederationConnectorProtocol>());
  }

  void TearDown() override {
    unregisterConnectorProtocol("mysql");
  }
};

TEST_F(JdbcThriftToJsonTest, javaWireFixtures) {
  std::ifstream input(PRESTO_JDBC_THRIFT_FIXTURES);
  ASSERT_TRUE(input.is_open());
  json fixtures;
  input >> fixtures;
  ASSERT_FALSE(fixtures.empty());
  for (const auto& fixture : fixtures) {
    const std::string kind = fixture.at("kind");
    SCOPED_TRACE(kind + ": " + fixture.at("json").dump());
    const std::string bytes =
        folly::base64Decode(fixture.at("thrift").get<std::string>());
    const json envelope = {
        {"@type", "mysql"}, {"customSerializedValue", fixture.at("thrift")}};
    json decoded;
    if (kind == "column") {
      decoded = decodeHandle<ColumnHandle, ArrowFederationColumnHandle>(
          envelope, bytes, &ArrowFederationColumnHandle::columnHandleBytes);
    } else if (kind == "table") {
      decoded = decodeHandle<ConnectorTableHandle, ArrowFederationTableHandle>(
          envelope, bytes, &ArrowFederationTableHandle::tableHandleBytes);
    } else if (kind == "transaction") {
      decoded = decodeHandle<
          ConnectorTransactionHandle,
          ArrowFederationTransactionHandle>(
          envelope,
          bytes,
          &ArrowFederationTransactionHandle::transactionHandleBytes);
    } else if (kind == "layout") {
      decoded = decodeHandle<
          ConnectorTableLayoutHandle,
          ArrowFederationTableLayoutHandle>(
          envelope,
          bytes,
          &ArrowFederationTableLayoutHandle::tableLayoutHandleBytes);
    } else if (kind == "split") {
      decoded = decodeHandle<ConnectorSplit, ArrowFederationSplit>(
          envelope, bytes, &ArrowFederationSplit::splitBytes);
    } else {
      FAIL() << "Unknown fixture kind: " << kind;
    }
    EXPECT_EQ(decoded, fixture.at("json"));
    if (kind == "split") {
      checkThriftEnvelope<
          thrift::ConnectorSplit,
          ConnectorSplit,
          ArrowFederationSplit>(fixture, &ArrowFederationSplit::splitBytes);
    } else if (kind == "transaction") {
      checkThriftEnvelope<
          thrift::ConnectorTransactionHandle,
          ConnectorTransactionHandle,
          ArrowFederationTransactionHandle>(
          fixture, &ArrowFederationTransactionHandle::transactionHandleBytes);
    } else if (kind == "table") {
      checkThriftEnvelope<
          thrift::ConnectorTableHandle,
          ConnectorTableHandle,
          ArrowFederationTableHandle>(
          fixture, &ArrowFederationTableHandle::tableHandleBytes);
    } else if (kind == "layout") {
      checkThriftEnvelope<
          thrift::ConnectorTableLayoutHandle,
          ConnectorTableLayoutHandle,
          ArrowFederationTableLayoutHandle>(
          fixture, &ArrowFederationTableLayoutHandle::tableLayoutHandleBytes);
    }
  }
}

TEST_F(JdbcThriftToJsonTest, rejectsMalformedPayloads) {
  EXPECT_THROW(jdbcSplitFromThrift("bad thrift"), std::exception);
  EXPECT_THROW(
      jdbcTransactionHandleFromThrift(std::string(1, '\0')),
      std::invalid_argument);
}
