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
#include <folly/init/Init.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/ibm/CpdAuthenticator.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/ibm/SaasAuthenticator.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightConnectorTestBase.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/ArrowFlightPlanBuilder.h"
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/Utils.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"
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
  static constexpr const char* kAuthToken = "1234";
  static constexpr const char* kBearerAuthToken = "Bearer 1234";

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

    if (kBearerAuthToken != iter->second) {
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

namespace {
bool registerTestAuthFactories() {
  static bool once = [] {
    // Ensure the static macro registers factory
    std::make_shared<ibm::CpdAuthenticatorFactory>();
    std::make_shared<ibm::SaasAuthenticatorFactory>();
    return true;
  }();
  return once;
}
} // namespace

class ArrowFlightConnectorAuthTestBase : public ArrowFlightConnectorTestBase {
 public:
  explicit ArrowFlightConnectorAuthTestBase(
      const std::string_view authFactoryName)
      : ArrowFlightConnectorAuthTestBase(
            std::make_shared<velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>{
                    {ArrowFlightConfig::kAuthenticatorName,
                     std::string(authFactoryName)}})) {}

  explicit ArrowFlightConnectorAuthTestBase(
      std::shared_ptr<velox::config::ConfigBase> config)
      : ArrowFlightConnectorTestBase(config),
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

class IbmCpdAuthTest : public ArrowFlightConnectorAuthTestBase {
 public:
  IbmCpdAuthTest()
      : ArrowFlightConnectorAuthTestBase(
            ibm::CpdAuthenticatorFactory::kCpdAuthenticatorName) {}
};

TEST_F(IbmCpdAuthTest, cpdAuthenticator) {
  core::PlanNodePtr plan = addSampleDataAndRunQuery();

  auto idVec =
      makeFlatVector<int64_t>({1, 12, 2, std::numeric_limits<int64_t>::max()});
  auto valueVec = makeFlatVector<int32_t>(
      {41, 42, 43, std::numeric_limits<int32_t>::min()});

  AssertQueryBuilder(plan)
      .connectorSessionProperty(
          kFlightConnectorId,
          ibm::CpdAuthenticator::kCpdTokenKey,
          TestServerMiddlewareFactory::kAuthToken)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, valueVec}));

  ASSERT_TRUE(testMiddlewareFactory_->isTokenChecked(
      TestServerMiddlewareFactory::kBearerAuthToken));
}

class IbmSaasAuthTest : public ArrowFlightConnectorAuthTestBase {
 public:
  static constexpr const char* kTokenApiKey = "ABC123";

  IbmSaasAuthTest()
      : ArrowFlightConnectorAuthTestBase(std::string(
            ibm::SaasAuthenticatorFactory::kSaasAuthenticatorName)) {}

  void SetUp() override {
    httpIOExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(
        4, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));

    auto server = getHttpServer(false, httpIOExecutor_);
    server->registerPost(R"(/saas_token.*)", mockTokenResponse);

    serverWrapper_ = std::make_shared<HttpServerWrapper>(std::move(server));
    auto serverAddress = serverWrapper_->start().get();
    auto tokenUrl = fmt::format(
        "{}:{}/saas_token",
        serverAddress.getAddressStr(),
        serverAddress.getPort());

    config_ = std::make_shared<velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>{
            {ibm::SaasAuthenticatorConfig::kTokenUrl, tokenUrl},
            {ibm::SaasAuthenticatorConfig::kApiKey, getConnectorApiKey()},
            {ArrowFlightConfig::kAuthenticatorName,
             std::string(
                 ibm::SaasAuthenticatorFactory::kSaasAuthenticatorName)}});

    ArrowFlightConnectorAuthTestBase::SetUp();
  }

  void TearDown() override {
    ArrowFlightConnectorAuthTestBase::TearDown();
    serverWrapper_->stop();
  }

  virtual std::string getConnectorApiKey() {
    return kTokenApiKey;
  }

  static void mockTokenResponse(
      proxygen::HTTPMessage* message,
      std::vector<std::unique_ptr<folly::IOBuf>>& body,
      proxygen::ResponseHandler* downstream) {
    auto requestBody = facebook::presto::util::extractMessageBody(body);
    if (requestBody.find(kTokenApiKey) == std::string::npos) {
      proxygen::ResponseBuilder(downstream)
          .status(facebook::presto::http::kHttpUnauthorized, "Error")
          .body("Invalid API key")
          .sendWithEOM();
      return;
    }
    nlohmann::json j;
    j["access_token"] = TestServerMiddlewareFactory::kAuthToken;
    std::string responseBody(j.dump());
    proxygen::ResponseBuilder(downstream)
        .status(facebook::presto::http::kHttpOk, "")
        .header(proxygen::HTTP_HEADER_CONTENT_TYPE, "text/json")
        .body(responseBody)
        .sendWithEOM();
  }

 private:
  std::shared_ptr<folly::IOThreadPoolExecutor> httpIOExecutor_;
  std::shared_ptr<HttpServerWrapper> serverWrapper_;
};

TEST_F(IbmSaasAuthTest, mockTokenResponse) {
  core::PlanNodePtr plan = addSampleDataAndRunQuery();

  auto idVec =
      makeFlatVector<int64_t>({1, 12, 2, std::numeric_limits<int64_t>::max()});
  auto valueVec = makeFlatVector<int32_t>(
      {41, 42, 43, std::numeric_limits<int32_t>::min()});

  AssertQueryBuilder(plan)
      .splits(makeSplits({"sample-data"}))
      .assertResults(makeRowVector({idVec, valueVec}));

  ASSERT_TRUE(testMiddlewareFactory_->isTokenChecked(
      TestServerMiddlewareFactory::kBearerAuthToken));
}

class IbmSaasUnauthTest : public IbmSaasAuthTest {
 public:
  std::string getConnectorApiKey() override {
    return "BADKEY999";
  }
};

TEST_F(IbmSaasUnauthTest, unauthorizedApiKey) {
  core::PlanNodePtr plan = addSampleDataAndRunQuery();

  auto idVec =
      makeFlatVector<int64_t>({1, 12, 2, std::numeric_limits<int64_t>::max()});
  auto valueVec = makeFlatVector<int32_t>(
      {41, 42, 43, std::numeric_limits<int32_t>::min()});

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .splits(makeSplits({"sample-data"}))
          .assertResults(makeRowVector({idVec, valueVec})),
      "Arrow flight server token fetching failed");

  ASSERT_FALSE(testMiddlewareFactory_->isTokenChecked(
      TestServerMiddlewareFactory::kAuthToken));
}

} // namespace facebook::presto::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv};
  return RUN_ALL_TESTS();
}
