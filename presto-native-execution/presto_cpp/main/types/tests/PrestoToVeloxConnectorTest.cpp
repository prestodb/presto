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
#include "presto_cpp/main/types/PrestoToVeloxConnector.h"
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::presto;
using namespace facebook::velox;

class PrestoToVeloxConnectorTest : public ::testing::Test {};

TEST_F(PrestoToVeloxConnectorTest, registerVariousConnectors) {
  std::vector<std::pair<std::string, std::unique_ptr<PrestoToVeloxConnector>>>
      connectorList;
  connectorList.emplace_back(std::move(std::pair(
      "hive",
      std::move(std::make_unique<HivePrestoToVeloxConnector>("hive")))));
  connectorList.emplace_back(std::move(std::pair(
      "hive-hadoop2",
      std::move(
          std::make_unique<HivePrestoToVeloxConnector>("hive-hadoop2")))));
  connectorList.emplace_back(std::move(std::pair(
      "iceberg",
      std::move(std::make_unique<IcebergPrestoToVeloxConnector>("iceberg")))));
  connectorList.emplace_back(std::move(std::pair(
      "tpch",
      std::move(std::make_unique<HivePrestoToVeloxConnector>("tpch")))));

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
