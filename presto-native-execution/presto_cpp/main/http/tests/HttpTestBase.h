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
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "velox/common/base/StatsReporter.h"

namespace fs = boost::filesystem;

using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::memory;

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

  // As with building/testing using CLion when using a manual build and
  // running the test from the build path the certs are not found and
  // their path must be updated.
  // The path is used by CMake.
  boost::algorithm::replace_all(currentPath, "_build/debug/", "");
  boost::algorithm::replace_all(currentPath, "_build/release/", "");

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
      server_->start(
          std::move(filters_), [&](proxygen::HTTPServer* httpServer) {
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

  void setFilters(
      std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>& filters) {
    filters_ = std::move(filters);
  }

 private:
  std::unique_ptr<http::HttpServer> server_;
  std::unique_ptr<std::thread> serverThread_;
  folly::Promise<folly::SocketAddress> promise_;
  std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters_ = {};
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
  EXPECT_EQ(pool->usedBytes(), 0);
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
    eventBase_->runInEventBaseThread([pools = std::move(sessionPools_)] {});
    eventBase_->terminateLoopSoon();
    eventBaseThread_->join();
  }

  std::shared_ptr<http::HttpClient> newClient(
      const folly::SocketAddress& address,
      const std::chrono::milliseconds& transactionTimeout,
      const std::chrono::milliseconds& connectTimeout,
      bool useHttps,
      std::shared_ptr<MemoryPool> pool,
      std::function<void(int)>&& reportOnBodyStatsFunc = nullptr) {
    sessionPools_.push_back(
        std::make_unique<proxygen::SessionPool>(nullptr, 10));
    if (useHttps) {
      const std::string keyPath = getCertsPath("client_ca.pem");
      const std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
      auto sslContext = std::make_shared<folly::SSLContext>();
      sslContext->loadCertKeyPairFromFiles(keyPath.c_str(), keyPath.c_str());
      sslContext->setCiphersOrThrow(ciphers);
      return std::make_shared<http::HttpClient>(
          eventBase_.get(),
          sessionPools_.back().get(),
          address,
          transactionTimeout,
          connectTimeout,
          pool,
          std::move(sslContext),
          std::move(reportOnBodyStatsFunc));
    } else {
      return std::make_shared<http::HttpClient>(
          eventBase_.get(),
          sessionPools_.back().get(),
          address,
          transactionTimeout,
          connectTimeout,
          pool,
          nullptr,
          std::move(reportOnBodyStatsFunc));
    }
  }

 private:
  std::unique_ptr<folly::EventBase> eventBase_;
  std::unique_ptr<std::thread> eventBaseThread_;
  std::vector<std::unique_ptr<proxygen::SessionPool>> sessionPools_;
};

folly::SemiFuture<std::unique_ptr<http::HttpResponse>> sendGet(
    http::HttpClient* client,
    const std::string& url,
    const uint64_t sendDelay = 0,
    const std::string body = "") {
  return http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(url)
      .send(client, body, sendDelay);
}

static std::unique_ptr<http::HttpServer> getHttpServer(
    bool useHttps,
    const std::shared_ptr<folly::IOThreadPoolExecutor>& httpIOExecutor) {
  if (useHttps) {
    const std::string certPath = getCertsPath("test_cert1.pem");
    const std::string keyPath = getCertsPath("test_key1.pem");
    const std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
    auto httpsConfig = std::make_unique<http::HttpsConfig>(
        folly::SocketAddress("127.0.0.1", 0), certPath, keyPath, ciphers);
    return std::make_unique<http::HttpServer>(
        httpIOExecutor, nullptr, std::move(httpsConfig));
  } else {
    return std::make_unique<http::HttpServer>(
        httpIOExecutor,
        std::make_unique<http::HttpConfig>(
            folly::SocketAddress("127.0.0.1", 0)),
        nullptr);
  }
}

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
  std::function<void()> customFunc;
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
          if (request->customFunc != nullptr) {
            request->customFunc();
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
