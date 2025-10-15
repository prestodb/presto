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

#include "presto_cpp/main/connectors/Connector.h"
#include <gtest/gtest.h>
#include <memory>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/Connector.h"

using namespace facebook::presto;
using namespace facebook::velox;
using facebook::velox::connectors::ConnectorFactory;

namespace {

class TestConnector : public connector::Connector {
 public:
  TestConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config)
      : id_(id), config_(config) {}

  std::shared_ptr<connector::ConnectorTransactionHandle> beginTransaction(
      connector::IsolationLevel isolationLevel,
      bool readOnly) override {
    return nullptr;
  }

  std::unique_ptr<connector::ConnectorSplit> createConnectorSplit(
      const std::string& splitInfo) override {
    return nullptr;
  }

 private:
  std::string id_;
  std::shared_ptr<const config::ConfigBase> config_;
};

class TestConnectorFactory : public ConnectorFactory {
 public:
  TestConnectorFactory() : ConnectorFactory("test") {}

  std::shared_ptr<connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /*ioExecutor*/ = nullptr,
      folly::Executor* /*cpuExecutor*/ = nullptr) override {
    return std::make_shared<TestConnector>(id, config);
  }
};

class AnotherTestConnectorFactory : public ConnectorFactory {
 public:
  AnotherTestConnectorFactory() : ConnectorFactory("another-test") {}

  std::shared_ptr<connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* /*ioExecutor*/ = nullptr,
      folly::Executor* /*cpuExecutor*/ = nullptr) override {
    return std::make_shared<TestConnector>(id, config);
  }
};

class ConnectorTest : public testing::Test {
 protected:
  void SetUp() override {
    auto& factories = detail::connectorFactories();
    factories.clear();
  }

  void TearDown() override {
    auto& factories = detail::connectorFactories();
    factories.clear();
  }
};

TEST_F(ConnectorTest, registerConnectorFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  EXPECT_TRUE(registerConnectorFactory(factory));
  EXPECT_TRUE(hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, registerDuplicateConnectorFactory) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  auto factory2 = std::make_shared<TestConnectorFactory>();

  EXPECT_TRUE(registerConnectorFactory(factory1));
  VELOX_ASSERT_THROW(
      registerConnectorFactory(factory2),
      "ConnectorFactory with name 'test' is already registered");
}

TEST_F(ConnectorTest, hasConnectorFactory) {
  EXPECT_FALSE(hasConnectorFactory("test"));

  auto factory = std::make_shared<TestConnectorFactory>();
  registerConnectorFactory(factory);
  EXPECT_TRUE(hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, unregisterConnectorFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  registerConnectorFactory(factory);

  EXPECT_TRUE(hasConnectorFactory("test"));
  EXPECT_TRUE(unregisterConnectorFactory("test"));
  EXPECT_FALSE(hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, unregisterNonExistentConnectorFactory) {
  EXPECT_FALSE(unregisterConnectorFactory("non-existent"));
}

TEST_F(ConnectorTest, getConnectorFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  registerConnectorFactory(factory);

  auto retrieved = getConnectorFactory("test");
  EXPECT_EQ(retrieved->connectorName(), "test");
}

TEST_F(ConnectorTest, getConnectorFactoryNotRegistered) {
  VELOX_ASSERT_THROW(
      getConnectorFactory("non-existent"),
      "ConnectorFactory with name 'non-existent' not registered");
}

TEST_F(ConnectorTest, connectorFactoryName) {
  auto factory = std::make_shared<TestConnectorFactory>();
  EXPECT_EQ(factory->connectorName(), "test");
}

TEST_F(ConnectorTest, multipleConnectorFactories) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  auto factory2 = std::make_shared<AnotherTestConnectorFactory>();

  EXPECT_TRUE(registerConnectorFactory(factory1));
  EXPECT_TRUE(registerConnectorFactory(factory2));

  EXPECT_TRUE(hasConnectorFactory("test"));
  EXPECT_TRUE(hasConnectorFactory("another-test"));

  auto retrieved1 = getConnectorFactory("test");
  auto retrieved2 = getConnectorFactory("another-test");

  EXPECT_EQ(retrieved1->connectorName(), "test");
  EXPECT_EQ(retrieved2->connectorName(), "another-test");
}

TEST_F(ConnectorTest, listConnectorFactories) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  auto factory2 = std::make_shared<AnotherTestConnectorFactory>();

  registerConnectorFactory(factory1);
  registerConnectorFactory(factory2);

  auto connectorNames = listConnectorFactories();
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
  auto connectorNames = listConnectorFactories();
  EXPECT_EQ(connectorNames.size(), 0);
}

TEST_F(ConnectorTest, newConnectorFromFactory) {
  auto factory = std::make_shared<TestConnectorFactory>();
  registerConnectorFactory(factory);

  auto config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  auto connector = factory->newConnector("test-id", config);
  EXPECT_NE(connector, nullptr);
}

TEST_F(ConnectorTest, unregisterAndReregisterFactory) {
  auto factory1 = std::make_shared<TestConnectorFactory>();
  registerConnectorFactory(factory1);
  EXPECT_TRUE(hasConnectorFactory("test"));

  EXPECT_TRUE(unregisterConnectorFactory("test"));
  EXPECT_FALSE(hasConnectorFactory("test"));

  auto factory2 = std::make_shared<TestConnectorFactory>();
  EXPECT_TRUE(registerConnectorFactory(factory2));
  EXPECT_TRUE(hasConnectorFactory("test"));
}

TEST_F(ConnectorTest, connectorFactorySingleton) {
  auto& factories1 = detail::connectorFactories();
  auto& factories2 = detail::connectorFactories();
  EXPECT_EQ(&factories1, &factories2);
}

} // namespace
