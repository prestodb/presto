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
#include "presto_cpp/main/http/filters/InternalAuthenticationFilter.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"

namespace fs = boost::filesystem;

using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::memory;

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

class HttpJwtTestSuite : public ::testing::TestWithParam<bool> {
 public:
  explicit HttpJwtTestSuite() {
    setupJwtNodeConfig();
  }

 protected:
  std::unique_ptr<Config> jwtSystemConfig(
      const std::unordered_map<std::string, std::string> configOverride = {})
      const {
    std::unordered_map<std::string, std::string> systemConfig{
        {std::string(SystemConfig::kMutableConfig), std::string("true")},
        {std::string(SystemConfig::kInternalCommunicationJwtEnabled),
         std::string("true")},
        {std::string(SystemConfig::kInternalCommunicationSharedSecret),
         "mysecret"}};

    // Update the default config map with the supplied configOverride map
    for (const auto& [configName, configValue] : configOverride) {
      systemConfig[configName] = configValue;
    }

    return std::make_unique<core::MemConfigMutable>(systemConfig);
  }

  void setupJwtNodeConfig() const {
    std::unordered_map<std::string, std::string> nodeConfigValues{
        {std::string(NodeConfig::kMutableConfig), std::string("true")},
        {std::string(NodeConfig::kNodeId), std::string("testnode")}};
    std::unique_ptr<Config> rawNodeConfig =
        std::make_unique<core::MemConfig>(nodeConfigValues);
    NodeConfig::instance()->initialize(std::move(rawNodeConfig));
  }

  std::unique_ptr<http::HttpResponse> produceHttpResponse(
      const bool useHttps,
      std::unordered_map<std::string, std::string> clientSystemConfigOverride =
          {},
      std::unordered_map<std::string, std::string> serverSystemConfigOverride =
          {},
      const uint64_t sendDelayMs = 500) {
    auto memoryPool = defaultMemoryManager().addLeafPool("HttpJwtTestSuite");
    auto clientConfig = jwtSystemConfig(clientSystemConfigOverride);
    auto systemConfig = SystemConfig::instance();
    systemConfig->initialize(std::move(clientConfig));

    auto server = getServer(useHttps);

    auto request = std::make_shared<AsyncMsgRequestState>();

    // Set the delay to 1s to have enough time to update the config.
    server->registerGet("/async/msg", asyncMsg(request));

    std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters;
    filters.push_back(
        std::make_unique<http::filters::InternalAuthenticationFilterFactory>());
    HttpServerWrapper wrapper(std::move(server));
    wrapper.setFilters(filters);
    auto serverAddress = wrapper.start().get();

    HttpClientFactory clientFactory;
    // Make sure client doesn't timeout for a delayed request.
    auto client = clientFactory.newClient(
        serverAddress, std::chrono::milliseconds(1'000), useHttps, memoryPool);

    auto [reqPromise, reqFuture] = folly::makePromiseContract<bool>();
    request->requestPromise = std::move(reqPromise);

    auto responseFuture =
        sendGet(client.get(), "/async/msg", sendDelayMs, "TestBody");

    auto serverConfig = jwtSystemConfig(serverSystemConfigOverride);
    auto valuesMap = serverConfig->valuesCopy();
    /// The request is delayed. Meanwhile update the config so the server
    /// might use a different configuration, if provided.
    for (auto key : valuesMap) {
      systemConfig->setValue(key.first, key.second);
    }

    /// Wait and wake up after 1s, if needed, because the server should have
    /// rejected the request or processed the request already.
    std::move(reqFuture).wait(std::chrono::milliseconds(1'000));

    if (auto msgPromise = request->msgPromise.lock()) {
      msgPromise->promise.setValue("Success");
    }

    auto response = std::move(responseFuture).get();
    wrapper.stop();

    return response;
  }
};

TEST_P(HttpJwtTestSuite, basicJwtTest) {
  const bool useHttps = GetParam();

  auto response = std::move(produceHttpResponse(useHttps));

  EXPECT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
}

TEST_P(HttpJwtTestSuite, jwtSecretMismatch) {
  std::unordered_map<std::string, std::string> serverConfigOverride{
      {std::string(SystemConfig::kInternalCommunicationSharedSecret),
       "falseSecret"}};

  const bool useHttps = GetParam();

  auto response =
      std::move(produceHttpResponse(useHttps, {}, serverConfigOverride));

  EXPECT_EQ(response->headers()->getStatusCode(), http::kHttpUnauthorized);
}

/// This test tests when a token arrives, is processed, and is rejected
/// if past the expiration time.
TEST_P(HttpJwtTestSuite, jwtExpiredToken) {
  const uint64_t kSendDelay{1'500};

  std::unordered_map<std::string, std::string> clientConfigOverride{
      {std::string(SystemConfig::kInternalCommunicationJwtExpirationSeconds),
       std::string("1")}}; // expire after 1s, delay is 1.5s.

  const bool useHttps = GetParam();

  auto response = std::move(
      produceHttpResponse(useHttps, clientConfigOverride, {}, kSendDelay));

  EXPECT_EQ(response->headers()->getStatusCode(), http::kHttpUnauthorized);
}

// Test catching a misconfiguration when a request with a JWT is received.
TEST_P(HttpJwtTestSuite, jwtServerVerificationDisabled) {
  std::unordered_map<std::string, std::string> serverConfigOverride{
      {std::string(SystemConfig::kInternalCommunicationJwtEnabled),
       std::string("false")}};

  const bool useHttps = GetParam();

  auto response =
      std::move(produceHttpResponse(useHttps, {}, serverConfigOverride));

  EXPECT_EQ(response->headers()->getStatusCode(), http::kHttpUnauthorized);
}

// Test missing client JWT.
TEST_P(HttpJwtTestSuite, jwtClientMissingJwt) {
  std::unordered_map<std::string, std::string> clientConfigOverride{
      {std::string(SystemConfig::kInternalCommunicationJwtEnabled),
       std::string("false")}};

  const bool useHttps = GetParam();

  auto response =
      std::move(produceHttpResponse(useHttps, clientConfigOverride));

  EXPECT_EQ(response->headers()->getStatusCode(), http::kHttpUnauthorized);
}

INSTANTIATE_TEST_CASE_P(
    HTTPJwtTest,
    HttpJwtTestSuite,
    ::testing::Values(true, false));
