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
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/TestingArrowFlightServer.h"
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/testing/gtest_util.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"

using namespace arrow;

namespace facebook::presto::test {

class TestingArrowFlightServerTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    server = std::make_unique<TestingArrowFlightServer>();
    ASSERT_OK_AND_ASSIGN(
        auto loc, flight::Location::ForGrpcTcp("127.0.0.1", 0));
    ASSERT_OK(server->Init(flight::FlightServerOptions(loc)));
  }

  static void TearDownTestSuite() {
    ASSERT_OK(server->Shutdown());
  }

  static void updateTable(
      const std::string& name,
      const std::shared_ptr<arrow::Table>& table) {
    server->updateTable(name, table);
  }

  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(
        auto loc, flight::Location::ForGrpcTcp("localhost", server->port()));
    ASSERT_OK_AND_ASSIGN(client_, flight::FlightClient::Connect(loc));
  }

  std::unique_ptr<flight::FlightClient> client_;
  static std::unique_ptr<TestingArrowFlightServer> server;
};

std::unique_ptr<TestingArrowFlightServer> TestingArrowFlightServerTest::server;

TEST_F(TestingArrowFlightServerTest, basicClientConnection) {
  auto sampleTable = makeArrowTable(
      {"id", "value"},
      {makeNumericArray<arrow::UInt32Type>({1, 2}),
       makeNumericArray<arrow::Int64Type>({41, 42})});
  updateTable("sample-data", sampleTable);

  ASSERT_RAISES(KeyError, client_->DoGet(flight::Ticket{"empty"}));

  auto emptyTable = makeArrowTable({}, {});
  updateTable("empty", emptyTable);

  ASSERT_RAISES(KeyError, client_->DoGet(flight::Ticket{"non-existent-table"}));

  ASSERT_OK_AND_ASSIGN(auto reader, client_->DoGet(flight::Ticket{"empty"}));
  ASSERT_OK_AND_ASSIGN(auto actual, reader->ToTable());
  EXPECT_TRUE(actual->Equals(*emptyTable));

  ASSERT_OK_AND_ASSIGN(reader, client_->DoGet(flight::Ticket{"sample-data"}));
  ASSERT_OK_AND_ASSIGN(actual, reader->ToTable());
  EXPECT_TRUE(actual->Equals(*sampleTable));

  server->removeTable("sample-data");
  ASSERT_RAISES(KeyError, client_->DoGet(flight::Ticket{"sample-data"}));
}

} // namespace facebook::presto::test
