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

#include "arrow/flight/api.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/StaticFlightServer.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::presto::connector::arrow_flight::test {

static const std::string kFlightConnectorId = "test-flight";

class FlightConnectorTestBase : public velox::exec::test::OperatorTestBase {
 public:
  void SetUp() override;

  void TearDown() override;

 protected:
  explicit FlightConnectorTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : config_{config} {}

  FlightConnectorTestBase()
      : config_{std::make_shared<velox::config::ConfigBase>(
            std::move(std::unordered_map<std::string, std::string>{}))} {}

 private:
  std::shared_ptr<velox::config::ConfigBase> config_;
};

/// Creates and registers an arrow flight connector and
/// spawns a Flight server for testing.
/// Initially there is no data in the Flight server,
/// tests should call FlightWithServerTestBase::updateTables to populate it.
class FlightWithServerTestBase : public FlightConnectorTestBase {
 public:
  static constexpr const char* BIND_HOST = "127.0.0.1";
  static constexpr const char* CONNECT_HOST = "localhost";
  constexpr static int LISTEN_PORT = 5000;

  void SetUp() override;

  void TearDown() override;

  /// Convenience method which creates splits for the test flight server
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> makeSplits(
      const std::initializer_list<std::string>& tokens,
      const std::vector<std::string>& location = std::vector<std::string>{
          fmt::format("grpc://{}:{}", CONNECT_HOST, LISTEN_PORT)});

  /// Add (or update) a table in the test flight server
  void updateTable(std::string name, std::shared_ptr<arrow::Table> table) {
    server_->updateTable(std::move(name), std::move(table));
  }

 protected:
  explicit FlightWithServerTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : FlightConnectorTestBase{std::move(config)},
        options_{createFlightServerOptions()} {}

  FlightWithServerTestBase()
      : FlightConnectorTestBase(), options_{createFlightServerOptions()} {}

  explicit FlightWithServerTestBase(
      std::shared_ptr<velox::config::ConfigBase> config,
      std::shared_ptr<arrow::flight::FlightServerOptions> options)
      : FlightConnectorTestBase{std::move(config)}, options_{options} {}

  std::shared_ptr<arrow::flight::FlightServerOptions> createFlightServerOptions(
      bool isSecure = false,
      const std::string& certPath = "",
      const std::string& keyPath = "");

 private:
  std::unique_ptr<StaticFlightServer> server_;
  std::shared_ptr<arrow::flight::FlightServerOptions> options_;
};

} // namespace facebook::presto::connector::arrow_flight::test
