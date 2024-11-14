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
#pragma once

#include <arrow/flight/api.h>
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/TestingArrowFlightServer.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::presto::test {

static const std::string kFlightConnectorId = "test-flight";

/// Creates and registers an Arrow Flight connector and
/// spawns a Flight server for testing.
/// Initially there is no data in the Flight server,
/// tests should call ArrowFlightConnectorTestBase::updateTables to populate it.
class ArrowFlightConnectorTestBase
    : public velox::exec::test::OperatorTestBase {
 public:
  static constexpr const char* kBindHost = "127.0.0.1";
  static constexpr const char* kConnectHost = "localhost";

  void SetUp() override;

  void TearDown() override;

  /// Create splits for this test flight server.
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> makeSplits(
      const std::initializer_list<std::string>& tokens);

  /// Convenience function for creating splits with endpoint locations.
  static std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>
  makeSplits(
      const std::initializer_list<std::string>& tokens,
      const std::vector<arrow::flight::Location>& locations);

  /// Add (or update) a table in the test flight server.
  void updateTable(
      const std::string& name,
      const std::shared_ptr<arrow::Table>& table) {
    server_->updateTable(name, table);
  }

  void setBatchSize(int64_t batchSize) {
    server_->setBatchSize(batchSize);
  }

  virtual void setFlightServerOptions(
      arrow::flight::FlightServerOptions* serverOptions) {}

 protected:
  explicit ArrowFlightConnectorTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : config_{std::move(config)} {}

  ArrowFlightConnectorTestBase()
      : ArrowFlightConnectorTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>())) {}

  uint32_t port_;
  std::unique_ptr<TestingArrowFlightServer> server_;
  std::shared_ptr<velox::config::ConfigBase> config_;
};

} // namespace facebook::presto::test
