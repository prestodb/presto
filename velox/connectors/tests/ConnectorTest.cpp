/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "velox/connectors/Connector.h"
#include <gtest/gtest.h>

namespace facebook::velox::connector {

class ConnectorTest : public testing::Test {};

namespace {

class TestConnector : public connector::Connector {
 public:
  TestConnector(const std::string& id) : connector::Connector(id, nullptr) {}

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr& /* outputType */,
      const std::shared_ptr<ConnectorTableHandle>& /* tableHandle */,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /* columnHandles */,
      connector::ConnectorQueryCtx* connectorQueryCtx) override {
    VELOX_NYI();
  }

  std::unique_ptr<connector::DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI();
  }
};

} // namespace

TEST_F(ConnectorTest, getAllConnectors) {
  const int32_t numConnectors = 10;
  for (int32_t i = 0; i < numConnectors; i++) {
    registerConnector(
        std::make_shared<TestConnector>(fmt::format("connector-{}", i)));
  }
  const auto& connectors = getAllConnectors();
  EXPECT_EQ(connectors.size(), numConnectors);
  for (int32_t i = 0; i < numConnectors; i++) {
    EXPECT_EQ(connectors.count(fmt::format("connector-{}", i)), 1);
  }
  for (int32_t i = 0; i < numConnectors; i++) {
    unregisterConnector(fmt::format("connector-{}", i));
  }
  EXPECT_EQ(getAllConnectors().size(), 0);
}
} // namespace facebook::velox::connector
