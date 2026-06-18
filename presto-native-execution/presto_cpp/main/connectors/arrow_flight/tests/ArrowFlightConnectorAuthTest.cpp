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
#include <arrow/testing/gtest_util.h>
#include <folly/init/Init.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightConnectorTestBase.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightPlanBuilder.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"

using namespace arrow;
using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::presto::test {

class TestServerMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  static constexpr const char* kAuthHeader = "authorization";
  static constexpr const char* kAuthToken = "Bearer 1234";
  static constexpr const char* kAuthTokenUnauthorized = "Bearer 2112";

  arrow::Status StartCall(
      const flight::CallInfo& info,
      const flight::ServerCallContext& context,
      std::shared_ptr<flight::ServerMiddleware>* middleware) override {
    auto iter = context.incoming_headers().find(kAuthHeader);

    if (iter == context.incoming_headers().end()) {
      return flight::MakeFlightError(
          flight::FlightStatusCode::Unauthenticated,
          "Authorization token not provided");
    } else {
      std::lock_guard<std::mutex> l(mutex_);
      checkedTokens_.emplace_back(iter->second);
    }

    if (kAuthToken != iter->second) {
      return flight::MakeFlightError(
          flight::FlightStatusCode::Unauthorized,
          "Authorization token is invalid");
    }

    return arrow::Status::OK();
  }

  bool isTokenChecked(const std::string& authToken) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      return std::find(
                 checkedTokens_.begin(), checkedTokens_.end(), authToken) !=
          checkedTokens_.end();
    }
  }

 private:
  std::string validToken_;
  std::vector<std::string> checkedTokens_;
  std::mutex mutex_;
};

class TestAuthenticator : public Authenticator {
 public:
  explicit TestAuthenticator(const std::string& authToken)
      : authToken_(authToken) {}

  void authenticateClient(
      std::unique_ptr<arrow::flight::FlightClient>& client,
      const velox::config::ConfigBase* sessionProperties,
      arrow::flight::AddCallHeaders& headerWriter) override {
    if (!authToken_.empty()) {
      headerWriter.AddHeader(
          TestServerMiddlewareFactory::kAuthHeader, authToken_);
    }
  }

 private:
  std::string authToken_;
};

class TestAuthenticatorFactory : public AuthenticatorFactory {
 public:
  TestAuthenticatorFactory(
      const std::string& name,
      const std::string& authToken)
      : AuthenticatorFactory(name),
        testAuthenticator_{std::make_shared<TestAuthenticator>(authToken)} {}

  std::shared_ptr<Authenticator> newAuthenticator(
      std::shared_ptr<const velox::config::ConfigBase> config) override {
    return testAuthenticator_;
  }

 private:
  std::shared_ptr<TestAuthenticator> testAuthenticator_;
};

namespace {
constexpr const char* kAuthFactoryName = "testing-auth-valid";
constexpr const char* kAuthFactoryUnauthorizedName =
    "testing-auth-unauthorized";
constexpr const char* kAuthFactoryNoTokenName = "testing-auth-no-token";

bool registerTestAuthFactories() {
  static bool once = [] {
    auto authFactory = std::make_shared<TestAuthenticatorFactory>(
        kAuthFactoryName, TestServerMiddlewareFactory::kAuthToken);
    registerAuthenticatorFactory(authFactory);
    auto authFactoryUnauthorized = std::make_shared<TestAuthenticatorFactory>(
        kAuthFactoryUnauthorizedName,
        TestServerMiddlewareFactory::kAuthTokenUnauthorized);
    registerAuthenticatorFactory(authFactoryUnauthorized);
    auto authFactoryNoToken =
        std::make_shared<TestAuthenticatorFactory>(kAuthFactoryNoTokenName, "");
    registerAuthenticatorFactory(authFactoryNoToken);
    return true;
  }();
  return once;
}
} // namespace

class ArrowFlightConnectorAuthTestBase : public ArrowFlightConnectorTestBase {
 public:
  explicit ArrowFlightConnectorAuthTestBase(const std::string& authFactoryName)
      : ArrowFlightConnectorTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kAuthenticatorName, authFactoryName}})),
        testMiddlewareFactory_(
            std::make_shared<TestServerMiddlewareFactory>()) {}

  void SetUp() override {
    registerTestAuthFactories();
    ArrowFlightConnectorTestBase::SetUp();
  }

  void setFlightServerOptions(
      flight::FlightServerOptions* serverOptions) override {
    serverOptions->middleware.push_back(
        {"bearer-auth-server", testMiddlewareFactory_});
  }

  core::PlanNodePtr addSampleDataAndRunQuery() {
    updateTable(
        "sample-data",
        makeArrowTable(
            {"id", "value"},
            {makeNumericArray<arrow::Int64Type>(
                 {1, 12, 2, std::numeric_limits<int64_t>::max()}),
             makeNumericArray<arrow::Int32Type>(
                 {41, 42, 43, std::numeric_limits<int32_t>::min()})}));

    return ArrowFlightPlanBuilder()
        .flightTableScan(
            velox::ROW({"id", "value"}, {velox::BIGINT(), velox::INTEGER()}))
        .planNode();
  }

 protected:
  std::shared_ptr<TestServerMiddlewareFactory> testMiddlewareFactory_;
};

class ArrowFlightConnectorAuthTest : public ArrowFlightConnectorAuthTestBase {
 public:
  ArrowFlightConnectorAuthTest()
      : ArrowFlightConnectorAuthTestBase(kAuthFactoryName) {}
};

TEST_F(ArrowFlightConnectorAuthTest, customAuthenticator) {
  core::PlanNodePtr plan = addSampleDataAndRunQuery();

  auto idVec =
      makeFlatVector<int64_t>({1, 12, 2, std::numeric_limits<int64_t>::max()});
  auto valueVec = makeFlatVector<int32_t>(
      {41, 42, 43, std::numeric_limits<int32_t>::min()});

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, valueVec}));

  ASSERT_TRUE(testMiddlewareFactory_->isTokenChecked(
      TestServerMiddlewareFactory::kAuthToken));
}

class ArrowFlightConnectorUnauthorizedTest
    : public ArrowFlightConnectorAuthTestBase {
 public:
  ArrowFlightConnectorUnauthorizedTest()
      : ArrowFlightConnectorAuthTestBase(kAuthFactoryUnauthorizedName) {}
};

TEST_F(ArrowFlightConnectorUnauthorizedTest, unauthorizedToken) {
  core::PlanNodePtr plan = addSampleDataAndRunQuery();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .assertEmptyResults(),
      "Unauthorized");
}

class ArrowFlightConnectorUnauthenticatedTest
    : public ArrowFlightConnectorAuthTestBase {
 public:
  ArrowFlightConnectorUnauthenticatedTest()
      : ArrowFlightConnectorAuthTestBase(kAuthFactoryNoTokenName) {}
};

TEST_F(ArrowFlightConnectorUnauthenticatedTest, unauthenticatedNoToken) {
  core::PlanNodePtr plan = addSampleDataAndRunQuery();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .assertEmptyResults(),
      "Unauthenticated");
}

} // namespace facebook::presto::test
