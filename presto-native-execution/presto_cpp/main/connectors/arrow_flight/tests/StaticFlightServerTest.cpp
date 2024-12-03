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
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/StaticFlightServer.h"
#include "arrow/api.h"
#include "arrow/flight/api.h"
#include "arrow/testing/gtest_util.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"

namespace {

using namespace facebook::presto::connector::arrow_flight::test;
using namespace arrow::flight;

class StaticFlightServerTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    server = std::make_unique<StaticFlightServer>();
    ASSERT_OK_AND_ASSIGN(auto loc, Location::ForGrpcTcp("127.0.0.1", 0));
    ASSERT_OK(server->Init(FlightServerOptions(loc)));
  }

  static void TearDownTestSuite() {
    ASSERT_OK(server->Shutdown());
  }

  static void updateTable(
      std::string name,
      std::shared_ptr<arrow::Table> table) {
    server->updateTable(std::move(name), std::move(table));
  }

  void SetUp() {
    ASSERT_OK_AND_ASSIGN(
        auto loc, Location::ForGrpcTcp("localhost", server->port()));
    ASSERT_OK_AND_ASSIGN(client, FlightClient::Connect(loc));
  }

  std::unique_ptr<FlightClient> client;
  static std::unique_ptr<StaticFlightServer> server;
};

std::unique_ptr<StaticFlightServer> StaticFlightServerTest::server;

TEST_F(StaticFlightServerTest, basicTest) {
  auto sampleTable = makeArrowTable(
      {"id", "value"},
      {makeNumericArray<arrow::UInt32Type>({1, 2}),
       makeNumericArray<arrow::Int64Type>({41, 42})});
  updateTable("sample-data", sampleTable);

  ASSERT_RAISES(KeyError, client->DoGet(Ticket{"empty"}));

  auto emptyTable = makeArrowTable({}, {});
  updateTable("empty", emptyTable);

  ASSERT_RAISES(KeyError, client->DoGet(Ticket{"non-existent-table"}));

  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"empty"}));
  ASSERT_OK_AND_ASSIGN(auto actual, reader->ToTable());
  EXPECT_TRUE(actual->Equals(*emptyTable));

  ASSERT_OK_AND_ASSIGN(reader, client->DoGet(Ticket{"sample-data"}));
  ASSERT_OK_AND_ASSIGN(actual, reader->ToTable());
  EXPECT_TRUE(actual->Equals(*sampleTable));

  server->removeTable("sample-data");
  ASSERT_RAISES(KeyError, client->DoGet(Ticket{"sample-data"}));
}

} // namespace
