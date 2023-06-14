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
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <velox/common/base/VeloxException.h>
#include <velox/common/base/tests/GTestUtils.h>
#include <velox/common/memory/Memory.h>
#include "presto_cpp/main/http/HttpClient.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "velox/common/base/StatsReporter.h"

namespace fs = boost::filesystem;

using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::memory;

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

namespace {

std::string getCertsPath(const std::string& fileName) {
  std::string currentPath = fs::current_path().c_str();
  if (boost::algorithm::ends_with(currentPath, "fbcode")) {
    return currentPath +
        "/github/presto-trunk/presto-native-execution/presto_cpp/main/http/tests/certs/" +
        fileName;
  }

  // CLion runs the tests from cmake-build-release/ or cmake-build-debug/
  // directory. Hard-coded json files are not copied there and test fails with
  // file not found. Fixing the path so that we can trigger these tests from
  // CLion.
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");

  return currentPath + "/certs/" + fileName;
}

class HttpServerWrapper {
 public:
  explicit HttpServerWrapper(std::unique_ptr<http::HttpServer> server)
      : server_(std::move(server)) {}

  ~HttpServerWrapper() {
    stop();
  }

  folly::SemiFuture<folly::SocketAddress> start() {
    auto [promise, future] = folly::makePromiseContract<folly::SocketAddress>();
    promise_ = std::move(promise);
    serverThread_ = std::make_unique<std::thread>([this]() {
      server_->start({}, [&](proxygen::HTTPServer* httpServer) {
        ASSERT_EQ(httpServer->addresses().size(), 1);
        promise_.setValue(httpServer->addresses()[0].address);
      });
    });

    return std::move(future);
  }

  void stop() {
    if (serverThread_) {
      server_->stop();
      serverThread_->join();
      serverThread_.reset();
    }
  }

 private:
  std::unique_ptr<http::HttpServer> server_;
  std::unique_ptr<std::thread> serverThread_;
  folly::Promise<folly::SocketAddress> promise_;
};

// Async SSL connection callback which auto close the socket on success for
// test.
class AsyncSSLSockAutoCloseCallback
    : public folly::AsyncSocket::ConnectCallback {
 public:
  explicit AsyncSSLSockAutoCloseCallback(folly::AsyncSSLSocket* sock)
      : sock_(sock) {}

  void connectSuccess() noexcept override {
    succeeded_ = true;
    sock_->close();
  }

  void connectErr(const folly::AsyncSocketException&) noexcept override {
    succeeded_ = false;
  }

  bool succeeded() const {
    return succeeded_;
  }

 private:
  folly::AsyncSSLSocket* const sock_{nullptr};
  bool succeeded_{false};
};

void ping(
    proxygen::HTTPMessage* /*message*/,
    std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
    proxygen::ResponseHandler* downstream) {
  proxygen::ResponseBuilder(downstream).status(http::kHttpOk, "").sendWithEOM();
}

void blackhole(
    proxygen::HTTPMessage* /*message*/,
    std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
    proxygen::ResponseHandler* downstream) {}

std::string bodyAsString(http::HttpResponse& response, MemoryPool* pool) {
  EXPECT_FALSE(response.hasError());
  std::ostringstream oss;
  auto iobufs = response.consumeBody();
  for (auto& body : iobufs) {
    oss << std::string((const char*)body->data(), body->length());
    pool->free(body->writableData(), body->capacity());
  }
  EXPECT_EQ(pool->currentBytes(), 0);
  return oss.str();
}

std::string toString(std::vector<std::unique_ptr<folly::IOBuf>>& bufs) {
  std::ostringstream oss;
  for (auto& buf : bufs) {
    oss << std::string((const char*)buf->data(), buf->length());
  }
  return oss.str();
}

void echo(
    proxygen::HTTPMessage* message,
    std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream) {
  if (body.empty()) {
    proxygen::ResponseBuilder(downstream)
        .status(http::kHttpOk, "")
        .body(folly::IOBuf::wrapBuffer(
            message->getURL().c_str(), message->getURL().size()))
        .sendWithEOM();
    return;
  }

  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "")
      .header(proxygen::HTTP_HEADER_CONTENT_TYPE, "text/plain")
      .body(toString(body))
      .sendWithEOM();
}

class HttpClientFactory {
 public:
  HttpClientFactory() : eventBase_(std::make_unique<folly::EventBase>()) {
    eventBaseThread_ =
        std::make_unique<std::thread>([&]() { eventBase_->loopForever(); });
  }

  ~HttpClientFactory() {
    eventBase_->terminateLoopSoon();
    eventBaseThread_->join();
  }

  std::shared_ptr<http::HttpClient> newClient(
      const folly::SocketAddress& address,
      const std::chrono::milliseconds& timeout,
      bool useHttps,
      std::shared_ptr<MemoryPool> pool,
      std::function<void(int)>&& reportOnBodyStatsFunc = nullptr) {
    if (useHttps) {
      std::string clientCaPath = getCertsPath("client_ca.pem");
      std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
      return std::make_shared<http::HttpClient>(
          eventBase_.get(),
          address,
          timeout,
          pool,
          clientCaPath,
          ciphers,
          std::move(reportOnBodyStatsFunc));
    } else {
      return std::make_shared<http::HttpClient>(
          eventBase_.get(),
          address,
          timeout,
          pool,
          "",
          "",
          std::move(reportOnBodyStatsFunc));
    }
  }

 private:
  std::unique_ptr<folly::EventBase> eventBase_;
  std::unique_ptr<std::thread> eventBaseThread_;
};

folly::SemiFuture<std::unique_ptr<http::HttpResponse>> sendGet(
    http::HttpClient* client,
    const std::string& url) {
  return http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(url)
      .send(client);
}

static std::unique_ptr<http::HttpServer> getServer(bool useHttps) {
  if (useHttps) {
    std::string certPath = getCertsPath("test_cert1.pem");
    std::string keyPath = getCertsPath("test_key1.pem");
    std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
    auto httpsConfig = std::make_unique<http::HttpsConfig>(
        folly::SocketAddress("127.0.0.1", 0), certPath, keyPath, ciphers);
    return std::make_unique<http::HttpServer>(nullptr, std::move(httpsConfig));
  } else {
    return std::make_unique<http::HttpServer>(
        std::make_unique<http::HttpConfig>(
            folly::SocketAddress("127.0.0.1", 0)));
  }
}
} // namespace

class HttpsBasicTest : public ::testing::Test {};

TEST_F(HttpsBasicTest, ssl) {
  auto memoryPool = defaultMemoryManager().addLeafPool("ssl");

  std::string certPath = getCertsPath("test_cert1.pem");
  std::string keyPath = getCertsPath("test_key1.pem");
  std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";

  auto httpsConfig = std::make_unique<http::HttpsConfig>(
      folly::SocketAddress("127.0.0.1", 0), certPath, keyPath, ciphers);

  auto server =
      std::make_unique<http::HttpServer>(nullptr, std::move(httpsConfig));

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

class HttpTestSuite : public ::testing::TestWithParam<bool> {};

TEST_P(HttpTestSuite, basic) {
  auto memoryPool = defaultMemoryManager().addLeafPool("basic");

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
      serverAddress, std::chrono::milliseconds(1'000), useHttps, memoryPool);

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
  auto rootPool = defaultMemoryManager().addRootPool("", memoryCapBytes);
  auto leafPool = rootPool->addLeafChild("httpResponseAllocationFailure");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  server->registerGet(R"(/echo.*)", echo);
  server->registerPost(R"(/echo.*)", echo);

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress, std::chrono::milliseconds(1'000), useHttps, leafPool);

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
  auto memoryPool = defaultMemoryManager().addLeafPool("serverRestart");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  server->registerGet("/ping", ping);

  auto wrapper = std::make_unique<HttpServerWrapper>(std::move(server));
  auto serverAddress = wrapper->start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress, std::chrono::milliseconds(1'000), useHttps, memoryPool);

  auto response = sendGet(client.get(), "/ping").get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);

  wrapper->stop();

  auto server2 = getServer(useHttps);

  server2->registerGet("/ping", ping);

  wrapper = std::make_unique<HttpServerWrapper>(std::move(server2));

  serverAddress = wrapper->start().get();
  client = clientFactory.newClient(
      serverAddress, std::chrono::milliseconds(1'000), useHttps, memoryPool);
  response = sendGet(client.get(), "/ping").get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  wrapper->stop();
}

namespace {
struct StringPromise {
  explicit StringPromise(folly::Promise<std::string> p)
      : promise(std::move(p)) {}
  folly::Promise<std::string> promise;
};

enum RequestStatus { kStatusUnknown, kStatusInvalid, kStatusValid };

struct AsyncMsgRequestState {
  folly::Promise<bool> requestPromise;
  uint64_t maxWaitMillis{0};
  std::weak_ptr<StringPromise> msgPromise;
  RequestStatus requestStatus{kStatusUnknown};
};

http::EndpointRequestHandlerFactory asyncMsg(
    std::shared_ptr<AsyncMsgRequestState> request) {
  return [request](
             proxygen::HTTPMessage* /* message */,
             const std::vector<std::string>& /* args */) {
    return new http::CallbackRequestHandler(
        [request](
            proxygen::HTTPMessage* /*message*/,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream,
            std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
          auto [promise, future] = folly::makePromiseContract<std::string>();
          auto eventBase = folly::EventBaseManager::get()->getEventBase();
          auto maxWaitMillis = request->maxWaitMillis;
          if (maxWaitMillis == 0) {
            maxWaitMillis = 1'000'000'000;
          }

          std::move(future)
              .via(eventBase)
              .onTimeout(
                  std::chrono::milliseconds(maxWaitMillis),
                  []() { return std::string("Timedout"); })
              .thenValue([downstream, handlerState, request](std::string msg) {
                if (!handlerState->requestExpired()) {
                  request->requestStatus = kStatusValid;
                  proxygen::ResponseBuilder(downstream)
                      .status(http::kHttpOk, "")
                      .header(proxygen::HTTP_HEADER_CONTENT_TYPE, "text/plain")
                      .body(msg)
                      .sendWithEOM();
                } else {
                  request->requestStatus = kStatusInvalid;
                }
              })
              .thenError(
                  folly::tag_t<std::exception>{},
                  [downstream, handlerState, request](std::exception const& e) {
                    if (!handlerState->requestExpired()) {
                      request->requestStatus = kStatusValid;
                      proxygen::ResponseBuilder(downstream)
                          .status(http::kHttpInternalServerError, "")
                          .header(
                              proxygen::HTTP_HEADER_CONTENT_TYPE, "text/plain")
                          .body(e.what())
                          .sendWithEOM();
                    } else {
                      request->requestStatus = kStatusInvalid;
                    }
                  });
          auto promiseHolder =
              std::make_shared<StringPromise>(std::move(promise));
          handlerState->runOnFinalization(
              [promiseHolder]() mutable { promiseHolder.reset(); });
          request->msgPromise = folly::to_weak_ptr(promiseHolder);
          request->requestPromise.setValue(true);
        });
  };
}
} // namespace

TEST_P(HttpTestSuite, asyncRequests) {
  auto memoryPool = defaultMemoryManager().addLeafPool("asyncRequests");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  auto request = std::make_shared<AsyncMsgRequestState>();
  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress, std::chrono::milliseconds(1'000), useHttps, memoryPool);

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
  auto memoryPool = defaultMemoryManager().addLeafPool("timedOutRequests");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  auto request = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress, std::chrono::milliseconds(1'000), useHttps, memoryPool);

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

// TODO: Enabled it when fixed.
// Disabled it, while we are investigating and fixing this test failure.
TEST_P(HttpTestSuite, DISABLED_outstandingRequests) {
  auto memoryPool =
      defaultMemoryManager().addLeafPool("DISABLED_outstandingRequests");

  const bool useHttps = GetParam();
  auto server = getServer(useHttps);

  auto request = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client = clientFactory.newClient(
      serverAddress, std::chrono::milliseconds(10'000), useHttps, memoryPool);

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
  auto memoryPool = defaultMemoryManager().addLeafPool("asyncRequests");

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
