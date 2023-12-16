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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

class HttpsBasicTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(HttpsBasicTest, ssl) {
  auto memoryPool = memory::MemoryManager::getInstance()->addLeafPool("ssl");

  std::string certPath = getCertsPath("test_cert1.pem");
  std::string keyPath = getCertsPath("test_key1.pem");
  std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";

  auto httpsConfig = std::make_unique<http::HttpsConfig>(
      folly::SocketAddress("127.0.0.1", 0), certPath, keyPath, ciphers);
  auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(
      8, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));
  auto server = std::make_unique<http::HttpServer>(
      ioPool, nullptr, std::move(httpsConfig));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  folly::EventBase evb;
  auto ctx = std::make_shared<folly::SSLContext>();
  folly::AsyncSSLSocket::UniquePtr sock(new folly::AsyncSSLSocket(ctx, &evb));
  AsyncSSLSockAutoCloseCallback cb(sock.get());
  sock->connect(&cb, serverAddress, 1000);
  evb.loop();
  EXPECT_TRUE(cb.succeeded());
}

class HttpTestSuite : public ::testing::TestWithParam<bool> {
 public:
  void SetUp() override {
    httpIOExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(
        8, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));
  }

 protected:
  std::unique_ptr<http::HttpServer> getServer(
      bool useHttps,
      const std::shared_ptr<folly::IOThreadPoolExecutor>& ioPool = nullptr) {
    return getHttpServer(
        useHttps, ioPool == nullptr ? httpIOExecutor_ : ioPool);
  }

 private:
  std::shared_ptr<folly::IOThreadPoolExecutor> httpIOExecutor_;
};

TEST_P(HttpTestSuite, basic) {
  auto memoryPool = memory::MemoryManager::getInstance()->addLeafPool("basic");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  server->registerGet("/ping", ping);
  server->registerGet("/blackhole", blackhole);
  server->registerGet(R"(/echo.*)", echo);
  server->registerPost(R"(/echo.*)", echo);

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);

  {
    auto response = sendGet(client.get(), "/ping").get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);

    response = sendGet(client.get(), "/echo/good-morning").get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
    ASSERT_EQ(bodyAsString(*response, memoryPool.get()), "/echo/good-morning");

    response = http::RequestBuilder()
                   .method(proxygen::HTTPMethod::POST)
                   .url("/echo")
                   .send(client.get(), "Good morning!")
                   .get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
    ASSERT_EQ(bodyAsString(*response, memoryPool.get()), "Good morning!");

    response = sendGet(client.get(), "/wrong/path").get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpNotFound);

    auto tryResponse = sendGet(client.get(), "/blackhole").getTry();
    ASSERT_TRUE(tryResponse.hasException());
    auto httpException = dynamic_cast<proxygen::HTTPException*>(
        tryResponse.tryGetExceptionObject());
    ASSERT_EQ(httpException->getProxygenError(), proxygen::kErrorTimeout);

    response = sendGet(client.get(), "/ping").get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  }
  wrapper.stop();

  auto tryResponse = sendGet(client.get(), "/ping").getTry();
  ASSERT_TRUE(tryResponse.hasException());

  auto socketException = dynamic_cast<folly::AsyncSocketException*>(
      tryResponse.tryGetExceptionObject());
  ASSERT_EQ(socketException->getType(), folly::AsyncSocketException::NOT_OPEN);
}

TEST_P(HttpTestSuite, httpResponseAllocationFailure) {
  const int64_t memoryCapBytes = 1 << 10;
  auto rootPool =
      memory::MemoryManager::getInstance()->addRootPool("", memoryCapBytes);
  auto leafPool = rootPool->addLeafChild("httpResponseAllocationFailure");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  server->registerGet(R"(/echo.*)", echo);
  server->registerPost(R"(/echo.*)", echo);

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      leafPool);

  {
    const std::string echoMessage(memoryCapBytes * 4, 'C');
    auto response =
        sendGet(client.get(), fmt::format("/echo/{}", echoMessage)).get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
    ASSERT_TRUE(response->hasError());
    VELOX_ASSERT_THROW(response->consumeBody(), "");
  }
  wrapper.stop();
}

TEST_P(HttpTestSuite, serverRestart) {
  auto memoryPool =
      memory::MemoryManager::getInstance()->addLeafPool("serverRestart");

  const bool useHttps = GetParam();
  auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(
      8, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));
  auto server = getServer(useHttps, ioPool);

  server->registerGet("/ping", ping);

  auto wrapper = std::make_unique<HttpServerWrapper>(std::move(server));
  auto serverAddress = wrapper->start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);

  auto response = sendGet(client.get(), "/ping").get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);

  wrapper->stop();

  auto ioPool2 = std::make_shared<folly::IOThreadPoolExecutor>(
      8, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO2"));
  auto server2 = getServer(useHttps);

  server2->registerGet("/ping", ping);

  wrapper = std::make_unique<HttpServerWrapper>(std::move(server2));

  serverAddress = wrapper->start().get();
  client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);
  response = sendGet(client.get(), "/ping").get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  wrapper->stop();
}

TEST_P(HttpTestSuite, asyncRequests) {
  auto memoryPool =
      memory::MemoryManager::getInstance()->addLeafPool("asyncRequests");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  auto request = std::make_shared<AsyncMsgRequestState>();
  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);

  auto [reqPromise, reqFuture] = folly::makePromiseContract<bool>();
  request->requestPromise = std::move(reqPromise);

  auto responseFuture = sendGet(client.get(), "/async/msg");

  // Wait until the request reaches to the server.
  std::move(reqFuture).wait();
  if (auto msgPromise = request->msgPromise.lock()) {
    msgPromise->promise.setValue("Success");
  }
  auto response = std::move(responseFuture).get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  ASSERT_EQ(bodyAsString(*response, memoryPool.get()), "Success");

  ASSERT_EQ(request->requestStatus, kStatusValid);
  wrapper.stop();
}

TEST_P(HttpTestSuite, timedOutRequests) {
  auto memoryPool =
      memory::MemoryManager::getInstance()->addLeafPool("timedOutRequests");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  auto request = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);

  request->maxWaitMillis = 100;
  auto [reqPromise, reqFuture] = folly::makePromiseContract<bool>();
  request->requestPromise = std::move(reqPromise);

  auto responseFuture = sendGet(client.get(), "/async/msg");

  // Wait until the request reaches to the server.
  std::move(reqFuture).wait();

  auto response = std::move(responseFuture).get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  ASSERT_EQ(bodyAsString(*response, memoryPool.get()), "Timedout");

  ASSERT_EQ(request->requestStatus, kStatusValid);
  wrapper.stop();
}

TEST_P(HttpTestSuite, httpConnectTimeout) {
  const bool useHttps = GetParam();
  if (!useHttps) {
    // Cannot test connect timeout for HTTP as HTTP connection establishment is
    // handled in kernel.
    return;
  }
  auto memoryPool =
      memory::MemoryManager::getInstance()->addLeafPool("httpTimeouts");

  auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(
      1, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));
  auto server = getServer(useHttps, ioPool);

  auto requestState1 = std::make_shared<AsyncMsgRequestState>();
  auto requestState2 = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg1", asyncMsg(requestState1));
  server->registerGet("/async/msg2", asyncMsg(requestState2));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client1 = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(10'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);
  auto client2 = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(10'000),
      std::chrono::milliseconds(2'000),
      useHttps,
      memoryPool);
  requestState1->customFunc = []() {
    // Holds the thread for 3s to fully occupy the server io pool (size 1), so
    // that the second request shall wait in io pool queue for at least 3s
    std::this_thread::sleep_for(std::chrono::milliseconds(3'000));
  };

  auto promisePair1 = folly::makePromiseContract<bool>();
  requestState1->requestPromise = std::move(promisePair1.first);
  auto promisePair2 = folly::makePromiseContract<bool>();
  requestState2->requestPromise = std::move(promisePair2.first);

  std::thread thread1([&]() {
    auto responseFuture = sendGet(client1.get(), "/async/msg1");
    try {
      std::move(promisePair1.second).wait();
      requestState1->msgPromise.lock()->promise.setValue("Success");
      auto response = std::move(responseFuture).get();
      ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
      ASSERT_EQ(bodyAsString(*response, memoryPool.get()), "Success");
      ASSERT_EQ(requestState1->requestStatus, kStatusValid);
    } catch (...) {
      FAIL();
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread thread2([&]() {
    // Time 00:00.300
    auto responseFuture = sendGet(client2.get(), "/async/msg2");
    try {
      auto response = std::move(responseFuture).get();
    } catch (std::exception& ex) {
      // Connect TIMEOUT happens here.
      ASSERT_TRUE(
          std::string_view(ex.what()).find(
              "AsyncSocketException: SSL connect timed out after") !=
          std::string::npos);
      return;
    }
    FAIL();
  });

  thread1.join();
  thread2.join();

  // Wait until the both reaches to the server.
  wrapper.stop();
}

TEST_P(HttpTestSuite, httpRequestTimeout) {
  auto memoryPool =
      memory::MemoryManager::getInstance()->addLeafPool("httpRequestTimeout");

  const bool useHttps = GetParam();
  auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(
      1, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));
  auto server = getServer(useHttps, ioPool);

  auto requestState = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg", asyncMsg(requestState));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);

  requestState->maxWaitMillis = 1500;

  auto promisePair = folly::makePromiseContract<bool>();
  requestState->requestPromise = std::move(promisePair.first);

  auto responseFuture = sendGet(client.get(), "/async/msg");
  try {
    std::move(promisePair.second).wait();
    auto response = std::move(responseFuture).get();
  } catch (std::exception& ex) {
    // Request TIMEOUT happens here.
    ASSERT_TRUE(
        std::string_view(ex.what()).find("ingress timeout") !=
        std::string::npos);
    wrapper.stop();
    return;
  }
  FAIL();
}

// TODO: Enabled it when fixed.
// Disabled it, while we are investigating and fixing this test failure.
TEST_P(HttpTestSuite, DISABLED_outstandingRequests) {
  auto memoryPool = memory::MemoryManager::getInstance()->addLeafPool(
      "DISABLED_outstandingRequests");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  auto request = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(10'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool);

  request->maxWaitMillis = 0;
  auto [reqPromise, reqFuture] = folly::makePromiseContract<bool>();
  request->requestPromise = std::move(reqPromise);

  auto responseFuture = sendGet(client.get(), "/async/msg");

  // Wait until the request reaches to the server.
  std::move(reqFuture).wait();

  // Stop the server now with the outstanding request.
  wrapper.stop();

  // Verify that Future's thenValue/thenError invoked.
  ASSERT_EQ(request->requestStatus, kStatusInvalid);
}

TEST_P(HttpTestSuite, testReportOnBodyStatsFunc) {
  std::atomic<int> reportedCount = 0;
  auto memoryPool =
      memory::MemoryManager::getInstance()->addLeafPool("asyncRequests");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  auto request = std::make_shared<AsyncMsgRequestState>();
  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress,
      std::chrono::milliseconds(1'000),
      std::chrono::milliseconds(0),
      useHttps,
      memoryPool,
      [&](size_t bufferBytes) { reportedCount.fetch_add(bufferBytes); });

  auto [reqPromise, reqFuture] = folly::makePromiseContract<bool>();
  request->requestPromise = std::move(reqPromise);

  auto responseFuture = sendGet(client.get(), "/async/msg");

  // Wait until the request reaches to the server.
  std::string responseData = "Success";
  std::move(reqFuture).wait();
  if (auto msgPromise = request->msgPromise.lock()) {
    msgPromise->promise.setValue(responseData);
  }
  auto response = std::move(responseFuture).get();

  ASSERT_EQ(reportedCount, responseData.size());
  wrapper.stop();
}

INSTANTIATE_TEST_CASE_P(
    HTTPTest,
    HttpTestSuite,
    ::testing::Values(true, false));

// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new facebook::velox::DummyStatsReporter();
});
