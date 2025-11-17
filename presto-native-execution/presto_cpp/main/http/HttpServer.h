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
#pragma once
#include <fmt/core.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <re2/re2.h>
#include <wangle/ssl/SSLContextConfig.h>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/http/HttpConstants.h"

namespace facebook::presto::http {

using json = nlohmann::json;

void sendOkResponse(proxygen::ResponseHandler* downstream);

void sendOkResponse(proxygen::ResponseHandler* downstream, const json& body);

void sendOkResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& body);

void sendOkTextResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& body);

void sendOkThriftResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& body);

void sendErrorResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& error = "",
    uint16_t status = http::kHttpInternalServerError);

void sendResponse(
    proxygen::ResponseHandler* downstream,
    const json& body,
    uint16_t status);

class AbstractRequestHandler : public proxygen::RequestHandler {
 public:
  void onRequest(
      std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override {
    headers_ = std::move(headers);
    body_.clear();
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
    body_.emplace_back(std::move(body));
  }

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override {}

  void requestComplete() noexcept override {
    delete this;
  }

  void onError(proxygen::ProxygenError err) noexcept override {
    delete this;
  }

 protected:
  std::unique_ptr<proxygen::HTTPMessage> headers_;
  std::vector<std::unique_ptr<folly::IOBuf>> body_;
};

// Some request handlers delay responses until some asynchronous work
// completes. When they delay responses asynchronously with
// promise/future, they also must handle the case that request may get
// expired in the meantime.
// They should
// 1. Check the state->requestExpired() before responding
//    and must do that on the thread that invoked 'onEOM()' to avoid
//    race.
// 2. Release any resources (eg. promise) using the
//    onFinalizationCallback, which will also run on the same thread
//    that invoked 'onEOM()'
//    See 'keepPromiseAlive()' in TaskManager.cpp
class CallbackRequestHandlerState {
 public:
  ~CallbackRequestHandlerState() {
    finalize();
  }

  void finalize() {
    requestExpired_ = true;
    if (onFinalizationCallback_) {
      onFinalizationCallback_();
      onFinalizationCallback_ = std::function<void(void)>();
    }
  }

  // The function 'fn' will run on the thread that invoked onEOM()
  void runOnFinalization(std::function<void(void)> callback) {
    onFinalizationCallback_ = callback;
  }

  bool requestExpired() const {
    return requestExpired_;
  }

  static std::shared_ptr<CallbackRequestHandlerState> create() {
    return std::make_shared<CallbackRequestHandlerState>();
  }

 private:
  std::function<void(void)> onFinalizationCallback_;
  bool requestExpired_{false};
};

using RequestHandlerCallback = std::function<void(
    proxygen::HTTPMessage*,
    std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream)>;

using AsyncRequestHandlerCallback = std::function<void(
    proxygen::HTTPMessage*,
    std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream,
    std::shared_ptr<CallbackRequestHandlerState> state)>;

class CallbackRequestHandler : public AbstractRequestHandler {
 public:
  explicit CallbackRequestHandler(RequestHandlerCallback callback)
      : callback_(wrap(callback)) {}

  explicit CallbackRequestHandler(AsyncRequestHandlerCallback callback)
      : callback_(callback), state_{CallbackRequestHandlerState::create()} {}

  ~CallbackRequestHandler() override {
    if (state_) {
      state_->finalize();
    }
  }

  void onEOM() noexcept override {
    callback_(headers_.get(), body_, downstream_, state_);
  }

 private:
  const AsyncRequestHandlerCallback callback_;
  std::shared_ptr<CallbackRequestHandlerState> state_;

  static AsyncRequestHandlerCallback wrap(RequestHandlerCallback callback) {
    return [callback](
               proxygen::HTTPMessage* headers,
               std::vector<std::unique_ptr<folly::IOBuf>>& body,
               proxygen::ResponseHandler* downstream,
               std::shared_ptr<CallbackRequestHandlerState> /* state */) {
      callback(headers, body, downstream);
    };
  }
};

class ErrorRequestHandler : public AbstractRequestHandler {
 public:
  ErrorRequestHandler(uint16_t errorCode, const std::string& errorMessage)
      : errorCode_(errorCode), errorMessage_(errorMessage) {}

  void onEOM() noexcept override {
    proxygen::ResponseBuilder(downstream_)
        .status(errorCode_, errorMessage_)
        .sendWithEOM();
  }

 private:
  const uint16_t errorCode_;
  const std::string errorMessage_;
};

using EndpointRequestHandlerFactory = std::function<proxygen::RequestHandler*(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& args)>;

class EndPoint {
 public:
  EndPoint(
      const std::string& pattern,
      const EndpointRequestHandlerFactory& factory)
      : re_(pattern), factory_(factory) {}

  bool check(
      const std::string& path,
      std::vector<std::string>& matches,
      std::vector<RE2::Arg>& args,
      std::vector<RE2::Arg*>& argPtrs) const;

  proxygen::RequestHandler* checkAndApply(
      const std::string& path,
      proxygen::HTTPMessage* message,
      std::vector<std::string>& matches,
      std::vector<RE2::Arg>& args,
      std::vector<RE2::Arg*>& argPtrs) const;

  const std::string& pattern() const {
    return re_.pattern();
  }

 private:
  const RE2 re_;
  EndpointRequestHandlerFactory factory_;
};

class DispatchingRequestHandlerFactory
    : public proxygen::RequestHandlerFactory {
 public:
  void onServerStart(folly::EventBase* /*evb*/) noexcept override {}

  void onServerStop() noexcept override {}

  proxygen::RequestHandler* onRequest(
      proxygen::RequestHandler*,
      proxygen::HTTPMessage* message) noexcept override;

  void registerEndPoint(
      proxygen::HTTPMethod method,
      const std::string& pattern,
      const EndpointRequestHandlerFactory& endpoint);

  const std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>&
  endpoints() const;

 private:
  std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>
      endpoints_;
};

class HttpConfig {
 public:
  explicit HttpConfig(
      const folly::SocketAddress& address,
      bool reusePort = false);

  proxygen::HTTPServer::IPConfig ipConfig() const;

 private:
  const folly::SocketAddress address_;
  const bool reusePort_{false};
};

class HttpsConfig {
 public:
  HttpsConfig(
      const folly::SocketAddress& address,
      const std::string& certPath,
      const std::string& keyPath,
      const std::string& supportedCiphers,
      bool reusePort = false,
      bool http2Enabled = true,
      const std::string& clientCaFile = "");

  proxygen::HTTPServer::IPConfig ipConfig() const;

 private:
  const folly::SocketAddress address_;
  const std::string certPath_;
  const std::string keyPath_;
  std::string supportedCiphers_;
  const std::string clientCaFile_;
  const bool reusePort_;
  const bool http2Enabled_;
};

class HttpServer {
 public:
  explicit HttpServer(
      const std::shared_ptr<folly::IOThreadPoolExecutor>& httpIOExecutor,
      std::unique_ptr<HttpConfig> httpConfig,
      std::unique_ptr<HttpsConfig> httpsConfig = nullptr);

  void start(
      std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters =
          {},
      std::function<void(proxygen::HTTPServer* /*server*/)> onSuccess = nullptr,
      std::function<void(std::exception_ptr)> onError = nullptr);

  folly::IOThreadPoolExecutor* getExecutor() {
    return httpIOExecutor_.get();
  }

  void stop() {
    server_->stop();
  }

  void registerGet(
      const std::string& pattern,
      const EndpointRequestHandlerFactory& endpoint) {
    handlerFactory_->registerEndPoint(
        proxygen::HTTPMethod::GET, pattern, endpoint);
  }

  void registerGet(
      const std::string& pattern,
      const RequestHandlerCallback& callback) {
    registerGet(pattern, endPointWrapper(callback));
  }

  void registerHead(
      const std::string& pattern,
      const EndpointRequestHandlerFactory& endpoint) {
    handlerFactory_->registerEndPoint(
        proxygen::HTTPMethod::HEAD, pattern, endpoint);
  }

  void registerHead(
      const std::string& pattern,
      const RequestHandlerCallback& callback) {
    registerHead(pattern, endPointWrapper(callback));
  }

  void registerPost(
      const std::string& pattern,
      const EndpointRequestHandlerFactory& endpoint) {
    handlerFactory_->registerEndPoint(
        proxygen::HTTPMethod::POST, pattern, endpoint);
  }

  void registerPost(
      const std::string& pattern,
      const RequestHandlerCallback& callback) {
    registerPost(pattern, endPointWrapper(callback));
  }

  void registerPut(
      const std::string& pattern,
      const EndpointRequestHandlerFactory& endpoint) {
    handlerFactory_->registerEndPoint(
        proxygen::HTTPMethod::PUT, pattern, endpoint);
  }

  void registerPut(
      const std::string& pattern,
      const RequestHandlerCallback& callback) {
    registerPut(pattern, endPointWrapper(callback));
  }

  void registerDelete(
      const std::string& pattern,
      const EndpointRequestHandlerFactory& endpoint) {
    handlerFactory_->registerEndPoint(
        proxygen::HTTPMethod::DELETE, pattern, endpoint);
  }

  void registerDelete(
      const std::string& pattern,
      const RequestHandlerCallback& callback) {
    registerDelete(pattern, endPointWrapper(callback));
  }

  std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>
  endpoints() const;

 private:
  const std::unique_ptr<HttpConfig> httpConfig_;
  const std::unique_ptr<HttpsConfig> httpsConfig_;
  std::unique_ptr<DispatchingRequestHandlerFactory> handlerFactory_;
  std::unique_ptr<proxygen::HTTPServer> server_;
  std::shared_ptr<folly::IOThreadPoolExecutor> httpIOExecutor_;

  static EndpointRequestHandlerFactory endPointWrapper(
      const RequestHandlerCallback& callback) {
    return [callback](
               proxygen::HTTPMessage* /* headers */,
               const std::vector<std::string>& /* args */) {
      return new CallbackRequestHandler(callback);
    };
  }
};

} // namespace facebook::presto::http
