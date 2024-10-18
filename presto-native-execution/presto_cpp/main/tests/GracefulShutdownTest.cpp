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
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "presto_cpp/main/tests/PrestoServerWrapper.h"

using namespace facebook::presto;
using namespace facebook::velox;

static std::unique_ptr<facebook::presto::PrestoServer> getPrestoServer() {
  auto server = std::make_unique<PrestoServer>("../../../etc");
  return server;
}

class GracefulShutdownTest : public ::testing::Test {
 protected:
  static facebook::presto::test::PrestoServerWrapper* wrapper;
  static folly::SocketAddress* socketAddress;
  static void SetUpTestSuite() {
    folly::SingletonVault::singleton()->registrationComplete();
#ifndef PRESTO_STATS_REPORTER_TYPE
    // Initialize singleton for the reporter.
    folly::Singleton<facebook::velox::BaseStatsReporter> reporter(
        []() { return new facebook::velox::DummyStatsReporter(); });
#endif
    static auto prestoServer = getPrestoServer();
    wrapper = new facebook::presto::test::PrestoServerWrapper(
        std::move(prestoServer));
    socketAddress = new folly::SocketAddress(wrapper->start().get());
  }

  static void TearDownTestSuite() {
    wrapper->stop();
  }
};

facebook::presto::test::PrestoServerWrapper* GracefulShutdownTest::wrapper =
    nullptr;
folly::SocketAddress* GracefulShutdownTest::socketAddress = nullptr;
TEST_F(GracefulShutdownTest, TestGetState) {
  auto memoryPool = memory::MemoryManager::getInstance()->addLeafPool("");

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      *socketAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      false,
      memoryPool);

  {
    auto response = sendGet(client.get(), "/v1/info/state").get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  }
}

TEST_F(GracefulShutdownTest, TestSendPutShuttingDown) {
  auto memoryPool = memory::MemoryManager::getInstance()->addLeafPool("");

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      *socketAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      false,
      memoryPool);

  {
    std::string emptyBody = "";
    auto response = sendPut(client.get(), "/v1/info/state", 0, emptyBody).get();
    ASSERT_EQ(
        response->headers()->getStatusCode(),
        http::kHttpBadRequest); // Assuming empty request is bad
    ASSERT_EQ(bodyAsString(*response, memoryPool.get()), "Bad Request");
  }

  {
    std::string invalidBody = "\"SHUTTING_DWN\"";
    auto response =
        sendPut(client.get(), "/v1/info/state", 0, invalidBody).get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpBadRequest);
    ASSERT_EQ(bodyAsString(*response, memoryPool.get()), "Bad Request");
  }

  {
    std::string body = "\"SHUTTING_DOWN\"";
    auto response = sendPut(client.get(), "/v1/info/state", 0, body).get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  }
}
