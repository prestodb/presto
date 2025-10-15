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

#include "presto_cpp/main/connectors/Registration.h"
#include <gtest/gtest.h>
#include <memory>
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/Connector.h"

using namespace facebook::presto;

namespace {

class TestConnectorFactory : public ConnectorFactory {
 public:
  TestConnectorFactory() : ConnectorFactory("test") {}

  std::shared_ptr<facebook::velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config,
      folly::Executor* /*ioExecutor*/ = nullptr,
      folly::Executor* /*cpuExecutor*/ = nullptr) override {
    // Return nullptr for testing purposes - we're just testing registration
    return nullptr;
  }
};

class AnotherTestConnectorFactory : public ConnectorFactory {
 public:
  AnotherTestConnectorFactory() : ConnectorFactory("another-test") {}

  std::shared_ptr<facebook::velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config,
      folly::Executor* /*ioExecutor*/ = nullptr,
      folly::Executor* /*cpuExecutor*/ = nullptr) override {
    // Return nullptr for testing purposes - we're just testing registration
    return nullptr;
  }
};

class ConnectorTest : public testing::Test {
 protected:
  void SetUp() override {
    auto& factories = facebook::presto::detail::connectorFactories();
    factories.clear();
  }

  void TearDown() override {
    auto& factories = facebook::presto::detail::connectorFactories();
    factories.clear();
  }
};

TEST_F(ConnectorTest, registerConnectorFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  EXPECT_TRUE(facebook::presto::registerConnectorFactory(factory));
  EXPECT_TRUE(facebook::presto::hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, registerDuplicateConnectorFactory) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  auto factory2 = std::make_shared<TestConnectorFactory>();

  EXPECT_TRUE(facebook::presto::registerConnectorFactory(factory1));
  VELOX_ASSERT_THROW(
      facebook::presto::registerConnectorFactory(factory2),
      "ConnectorFactory with name 'test' is already registered");
}

TEST_F(ConnectorTest, hasConnectorFactory) {
  EXPECT_FALSE(facebook::presto::hasConnectorFactory("test"));

  auto factory = std::make_shared<TestConnectorFactory>();
  facebook::presto::registerConnectorFactory(factory);
  EXPECT_TRUE(facebook::presto::hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, unregisterConnectorFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  facebook::presto::registerConnectorFactory(factory);

  EXPECT_TRUE(facebook::presto::hasConnectorFactory("test"));
  EXPECT_TRUE(facebook::presto::unregisterConnectorFactory("test"));
  EXPECT_FALSE(facebook::presto::hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, unregisterNonExistentConnectorFactory) {
  EXPECT_FALSE(facebook::presto::unregisterConnectorFactory("non-existent"));
}

TEST_F(ConnectorTest, getConnectorFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  facebook::presto::registerConnectorFactory(factory);

  auto retrieved = facebook::presto::getConnectorFactory("test");
  EXPECT_EQ(retrieved->connectorName(), "test");
}

TEST_F(ConnectorTest, getConnectorFactoryNotRegistered) {
  VELOX_ASSERT_THROW(
      facebook::presto::getConnectorFactory("non-existent"),
      "ConnectorFactory with name 'non-existent' not registered");
}

TEST_F(ConnectorTest, connectorFactoryName) {
  auto factory = std::make_shared<TestConnectorFactory>();
  EXPECT_EQ(factory->connectorName(), "test");
}

TEST_F(ConnectorTest, multipleConnectorFactories) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  auto factory2 = std::make_shared<AnotherTestConnectorFactory>();

  EXPECT_TRUE(facebook::presto::registerConnectorFactory(factory1));
  EXPECT_TRUE(facebook::presto::registerConnectorFactory(factory2));

  EXPECT_TRUE(facebook::presto::hasConnectorFactory("test"));
  EXPECT_TRUE(facebook::presto::hasConnectorFactory("another-test"));

  auto retrieved1 = facebook::presto::getConnectorFactory("test");
  auto retrieved2 = facebook::presto::getConnectorFactory("another-test");

  EXPECT_EQ(retrieved1->connectorName(), "test");
  EXPECT_EQ(retrieved2->connectorName(), "another-test");
}

TEST_F(ConnectorTest, listConnectorFactories) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  auto factory2 = std::make_shared<AnotherTestConnectorFactory>();

  facebook::presto::registerConnectorFactory(factory1);
  facebook::presto::registerConnectorFactory(factory2);

  auto connectorNames = facebook::presto::listConnectorFactories();
  EXPECT_EQ(connectorNames.size(), 2);

  bool hasTest = false;
  bool hasAnotherTest = false;
  for (const auto& name : connectorNames) {
    if (name == "test") {
      hasTest = true;
    }
    if (name == "another-test") {
      hasAnotherTest = true;
    }
  }
  EXPECT_TRUE(hasTest);
  EXPECT_TRUE(hasAnotherTest);
}

TEST_F(ConnectorTest, listConnectorFactoriesEmpty) {
  auto connectorNames = facebook::presto::listConnectorFactories();
  EXPECT_EQ(connectorNames.size(), 0);
}

TEST_F(ConnectorTest, newConnectorFromFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  facebook::presto::registerConnectorFactory(factory);

  auto config = std::make_shared<facebook::velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  auto connector = factory->newConnector("test-id", config);
  EXPECT_EQ(connector, nullptr);
}

TEST_F(ConnectorTest, unregisterAndReregisterFactory) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  facebook::presto::registerConnectorFactory(factory1);
  EXPECT_TRUE(facebook::presto::hasConnectorFactory("test"));

  EXPECT_TRUE(facebook::presto::unregisterConnectorFactory("test"));
  EXPECT_FALSE(facebook::presto::hasConnectorFactory("test"));

  auto factory2 = std::make_shared<TestConnectorFactory>();
  EXPECT_TRUE(facebook::presto::registerConnectorFactory(factory2));
  EXPECT_TRUE(facebook::presto::hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, connectorFactorySingleton) {
  auto& factories1 = facebook::presto::detail::connectorFactories();
  auto& factories2 = facebook::presto::detail::connectorFactories();
  EXPECT_EQ(&factories1, &factories2);
}

} // namespace

