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

class ArrowFlightConnectorTestBase
    : public velox::exec::test::OperatorTestBase {
 public:
  void SetUp() override;

  void TearDown() override;

 protected:
  explicit ArrowFlightConnectorTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : config_{std::move(config)} {}

  ArrowFlightConnectorTestBase()
      : config_{std::make_shared<velox::config::ConfigBase>(
            std::move(std::unordered_map<std::string, std::string>{}))} {}

 protected:
  std::shared_ptr<velox::config::ConfigBase> config_;
};

/// Creates and registers an Arrow Flight connector and
/// spawns a Flight server for testing.
/// Initially there is no data in the Flight server,
/// tests should call FlightWithServerTestBase::updateTables to populate it.
class FlightWithServerTestBase : public ArrowFlightConnectorTestBase {
 public:
  static constexpr const char* BIND_HOST = "127.0.0.1";
  static constexpr const char* CONNECT_HOST = "localhost";
  constexpr static int LISTEN_PORT = 5000;

  void SetUp() override;

  void TearDown() override;

  /// Convenience method which creates splits for the test flight server
  static std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>
  makeSplits(
      const std::initializer_list<std::string>& tokens,
      const std::vector<arrow::flight::Location>& locations =
          std::vector<arrow::flight::Location>{
              *arrow::flight::Location::ForGrpcTcp(CONNECT_HOST, LISTEN_PORT)});

  /// Add (or update) a table in the test flight server
  void updateTable(std::string name, std::shared_ptr<arrow::Table> table) {
    server_->updateTable(std::move(name), std::move(table));
  }

  virtual arrow::flight::Location getServerLocation();

  virtual void setFlightServerOptions(
      arrow::flight::FlightServerOptions* serverOptions) {}

 protected:
  explicit FlightWithServerTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : ArrowFlightConnectorTestBase{std::move(config)} {}

  FlightWithServerTestBase() : ArrowFlightConnectorTestBase() {}

 private:
  std::unique_ptr<TestingArrowFlightServer> server_;
};

} // namespace facebook::presto::test
