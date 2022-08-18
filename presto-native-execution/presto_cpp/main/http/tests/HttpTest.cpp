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
#include <gtest/gtest.h>
#include "presto_cpp/main/http/HttpClient.h"
#include "presto_cpp/main/http/HttpServer.h"

using namespace facebook::presto;

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

class HttpServerWrapper {
 public:
  HttpServerWrapper(std::unique_ptr<http::HttpServer> server)
      : server_(std::move(server)) {}

  ~HttpServerWrapper() {
    stop();
  }

  folly::SemiFuture<folly::SocketAddress> start() {
    auto [promise, future] = folly::makePromiseContract<folly::SocketAddress>();
    promise_ = std::move(promise);
    serverThread_ = std::make_unique<std::thread>([this]() {
      server_->start([&](proxygen::HTTPServer* httpServer) {
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

std::string bodyAsString(http::HttpResponse& response) {
  std::ostringstream oss;
  auto iobufs = response.consumeBody();
  for (auto& body : iobufs) {
    oss << std::string((const char*)body->data(), body->length());
    response.mappedMemory()->freeBytes(body->writableData(), body->length());
  }
  EXPECT_EQ(response.mappedMemory()->numAllocated(), 0);
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

  std::unique_ptr<http::HttpClient> newClient(
      const folly::SocketAddress& address,
      const std::chrono::milliseconds& timeout) {
    return std::make_unique<http::HttpClient>(
        eventBase_.get(), address, timeout);
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

TEST(HttpTest, basic) {
  auto server =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));
  server->registerGet("/ping", ping);
  server->registerGet("/blackhole", blackhole);
  server->registerGet(R"(/echo.*)", echo);
  server->registerPost(R"(/echo.*)", echo);

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client =
      clientFactory.newClient(serverAddress, std::chrono::milliseconds(1'000));

  {
    auto response = sendGet(client.get(), "/ping").get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);

    response = sendGet(client.get(), "/echo/good-morning").get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
    ASSERT_EQ(bodyAsString(*response), "/echo/good-morning");

    response = http::RequestBuilder()
                   .method(proxygen::HTTPMethod::POST)
                   .url("/echo")
                   .send(client.get(), "Good morning!")
                   .get();
    ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
    ASSERT_EQ(bodyAsString(*response), "Good morning!");

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

TEST(HttpTest, serverRestart) {
  auto server =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));
  server->registerGet("/ping", ping);

  auto wrapper = std::make_unique<HttpServerWrapper>(std::move(server));
  auto serverAddress = wrapper->start().get();

  HttpClientFactory clientFactory;
  auto client =
      clientFactory.newClient(serverAddress, std::chrono::milliseconds(1'000));

  auto response = sendGet(client.get(), "/ping").get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);

  wrapper->stop();

  server =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));
  server->registerGet("/ping", ping);

  wrapper = std::make_unique<HttpServerWrapper>(std::move(server));

  serverAddress = wrapper->start().get();
  client =
      clientFactory.newClient(serverAddress, std::chrono::milliseconds(1'000));
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

TEST(HttpTest, asyncRequests) {
  auto server =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));

  auto request = std::make_shared<AsyncMsgRequestState>();
  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client =
      clientFactory.newClient(serverAddress, std::chrono::milliseconds(1'000));

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
  ASSERT_EQ(bodyAsString(*response), "Success");

  ASSERT_EQ(request->requestStatus, kStatusValid);
  wrapper.stop();
}

TEST(HttpTest, timedOutRequests) {
  auto server =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));

  auto request = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client =
      clientFactory.newClient(serverAddress, std::chrono::milliseconds(1'000));

  request->maxWaitMillis = 100;
  auto [reqPromise, reqFuture] = folly::makePromiseContract<bool>();
  request->requestPromise = std::move(reqPromise);

  auto responseFuture = sendGet(client.get(), "/async/msg");

  // Wait until the request reaches to the server.
  std::move(reqFuture).wait();
  auto response = std::move(responseFuture).get();
  ASSERT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  ASSERT_EQ(bodyAsString(*response), "Timedout");

  ASSERT_EQ(request->requestStatus, kStatusValid);
  wrapper.stop();
}

TEST(HttpTest, outstandingRequests) {
  auto server =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));
  auto request = std::make_shared<AsyncMsgRequestState>();

  server->registerGet("/async/msg", asyncMsg(request));

  HttpServerWrapper wrapper(std::move(server));
  auto serverAddress = wrapper.start().get();

  HttpClientFactory clientFactory;
  auto client =
      clientFactory.newClient(serverAddress, std::chrono::milliseconds(10'000));

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

// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new facebook::velox::DummyStatsReporter();
});
