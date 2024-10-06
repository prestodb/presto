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

#include <algorithm>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/http/HttpServer.h"

namespace facebook::presto::http {

void sendOkResponse(proxygen::ResponseHandler* downstream) {
  proxygen::ResponseBuilder(downstream).status(http::kHttpOk, "").sendWithEOM();
}

void sendOkResponse(proxygen::ResponseHandler* downstream, const json& body) {
  // nlohmann::json throws when it finds invalid UTF-8 characters. In that case
  // the server will crash. We handle such situation here and generate body
  // replacing the faulty UTF-8 sequences.
  std::string messageBody;
  try {
    messageBody = body.dump();
  } catch (const std::exception& e) {
    messageBody =
        body.dump(-1, ' ', false, nlohmann::detail::error_handler_t::replace);
    LOG(WARNING) << "Failed to serialize json to string. "
                    "Will retry with 'replace' option. "
                    "Json Dump:\n"
                 << messageBody;
  }

  sendOkResponse(downstream, messageBody);
}

void sendOkResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& body) {
  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
      .body(body)
      .sendWithEOM();
}

void sendOkThriftResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& body) {
  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationThrift)
      .body(body)
      .sendWithEOM();
}

void sendErrorResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& error,
    uint16_t status) {
  proxygen::ResponseBuilder(downstream)
      .status(status, "")
      .body(error)
      .sendWithEOM();
}

HttpConfig::HttpConfig(const folly::SocketAddress& address, bool reusePort)
    : address_(address), reusePort_(reusePort) {}

proxygen::HTTPServer::IPConfig HttpConfig::ipConfig() const {
  proxygen::HTTPServer::IPConfig ipConfig{
      address_, proxygen::HTTPServer::Protocol::HTTP};
  if (reusePort_) {
    folly::SocketOptionKey portReuseOpt = {SOL_SOCKET, SO_REUSEPORT};
    ipConfig.acceptorSocketOptions.emplace();
    ipConfig.acceptorSocketOptions->insert({portReuseOpt, 1});
  }
  return ipConfig;
}

HttpsConfig::HttpsConfig(
    const folly::SocketAddress& address,
    const std::string& certPath,
    const std::string& keyPath,
    const std::string& supportedCiphers,
    bool reusePort)
    : address_(address),
      certPath_(certPath),
      keyPath_(keyPath),
      supportedCiphers_(supportedCiphers),
      reusePort_(reusePort) {
  // Wangle separates ciphers by ":" where in the config it's separated with ","
  std::replace(supportedCiphers_.begin(), supportedCiphers_.end(), ',', ':');
}

proxygen::HTTPServer::IPConfig HttpsConfig::ipConfig() const {
  proxygen::HTTPServer::IPConfig ipConfig{
      address_, proxygen::HTTPServer::Protocol::HTTP};

  wangle::SSLContextConfig sslCfg;
  sslCfg.isDefault = true;
  sslCfg.clientVerification =
      folly::SSLContext::VerifyClientCertificate::DO_NOT_REQUEST;
  sslCfg.setCertificate(certPath_, keyPath_, "");
  sslCfg.sslCiphers = supportedCiphers_;

  ipConfig.sslConfigs.push_back(sslCfg);

  if (reusePort_) {
    folly::SocketOptionKey portReuseOpt = {SOL_SOCKET, SO_REUSEPORT};
    ipConfig.acceptorSocketOptions.emplace();
    ipConfig.acceptorSocketOptions->insert({portReuseOpt, 1});
  }
  return ipConfig;
}

HttpServer::HttpServer(
    const std::shared_ptr<folly::IOThreadPoolExecutor>& httpIOExecutor,
    std::unique_ptr<HttpConfig> httpConfig,
    std::unique_ptr<HttpsConfig> httpsConfig)
    : httpConfig_(std::move(httpConfig)),
      httpsConfig_(std::move(httpsConfig)),
      handlerFactory_(std::make_unique<DispatchingRequestHandlerFactory>()),
      httpIOExecutor_(httpIOExecutor) {
  VELOX_CHECK((httpConfig_ != nullptr) || (httpsConfig_ != nullptr));
  VELOX_CHECK(httpIOExecutor_ != nullptr);
}

bool EndPoint::check(
    const std::string& path,
    std::vector<std::string>& matches,
    std::vector<RE2::Arg>& args,
    std::vector<RE2::Arg*>& argPtrs) const {
  auto numArgs = re_.NumberOfCapturingGroups();
  matches.resize(numArgs + 1);
  args.resize(numArgs);
  argPtrs.resize(numArgs);

  for (auto i = 0; i < numArgs; ++i) {
    args[i] = &matches[i + 1];
    argPtrs[i] = &args[i];
  }
  if (RE2::FullMatchN(path, re_, argPtrs.data(), numArgs)) {
    matches[0] = path;
    return true;
  }
  return false;
}

proxygen::RequestHandler* EndPoint::checkAndApply(
    const std::string& path,
    proxygen::HTTPMessage* message,
    std::vector<std::string>& matches,
    std::vector<RE2::Arg>& args,
    std::vector<RE2::Arg*>& argPtrs) const {
  if (check(path, matches, args, argPtrs)) {
    return factory_(message, matches);
  }
  return nullptr;
}

proxygen::RequestHandler* DispatchingRequestHandlerFactory::onRequest(
    proxygen::RequestHandler*,
    proxygen::HTTPMessage* message) noexcept {
  auto it = endpoints_.find(message->getMethod().value());
  if (it == endpoints_.end()) {
    return new ErrorRequestHandler(
        http::kHttpInternalServerError,
        fmt::format(
            "Unsupported HTTP method: {} {}",
            message->getMethodString(),
            message->getURL()));
  }

  auto path = message->getPath();

  // Allocate vector outside of loop to avoid repeated alloc/free.
  std::vector<std::string> matches(4);
  std::vector<RE2::Arg> args(4);
  std::vector<RE2::Arg*> argPtrs(4);

  for (const auto& endpoint : it->second) {
    if (auto handler =
            endpoint->checkAndApply(path, message, matches, args, argPtrs)) {
      return handler;
    }
  }

  return new ErrorRequestHandler(
      http::kHttpNotFound,
      fmt::format(
          "Unsupported HTTP request: {} {}",
          message->getMethodString(),
          message->getURL()));
}

void DispatchingRequestHandlerFactory::registerEndPoint(
    proxygen::HTTPMethod method,
    const std::string& pattern,
    const EndpointRequestHandlerFactory& endpoint) {
  auto it = endpoints_.find(method);
  if (it == endpoints_.end()) {
    endpoints_[method].emplace_back(
        std::make_unique<EndPoint>(pattern, endpoint));
  } else {
    it->second.emplace_back(std::make_unique<EndPoint>(pattern, endpoint));
  }
}

const std::
    unordered_map<proxygen::HTTPMethod, std::vector<std::unique_ptr<EndPoint>>>&
    DispatchingRequestHandlerFactory::endpoints() const {
  return endpoints_;
}

std::unordered_map<proxygen::HTTPMethod, std::vector<std::unique_ptr<EndPoint>>>
HttpServer::endpoints() const {
  const auto& endpoints = handlerFactory_->endpoints();
  std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>
      copy;
  for (const auto& methodPair : endpoints) {
    const auto& method = methodPair.first;
    copy.emplace(method, std::vector<std::unique_ptr<EndPoint>>());
    auto& endpoints = copy.at(method);
    for (const auto& endpoint : methodPair.second) {
      endpoints.emplace_back(
          std::make_unique<EndPoint>(endpoint->pattern(), nullptr));
    }
  }
  return copy;
}

void HttpServer::start(
    std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters,
    std::function<void(proxygen::HTTPServer* /*server*/)> onSuccess,
    std::function<void(std::exception_ptr)> onError) {
  proxygen::HTTPServerOptions options;
  options.idleTimeout = std::chrono::milliseconds(60'000);
  options.enableContentCompression = false;

  proxygen::RequestHandlerChain handlerFactories;

  // Register all filters passed to the http server.
  for (size_t i = 0; i < filters.size(); ++i) {
    if (filters[i] != nullptr) {
      handlerFactories.addThen(std::move(filters[i]));
    }
  }

  handlerFactories.addThen(std::move(handlerFactory_));
  options.handlerFactories = handlerFactories.build();

  // Increase the default flow control to 1MB/10MB
  options.initialReceiveWindow = uint32_t(1 << 20);
  options.receiveStreamWindowSize = uint32_t(1 << 20);
  options.receiveSessionWindowSize = 10 * (1 << 20);
  options.h2cEnabled = true;

  server_ = std::make_unique<proxygen::HTTPServer>(std::move(options));

  std::vector<proxygen::HTTPServer::IPConfig> ipConfigs;

  if (httpConfig_ != nullptr) {
    ipConfigs.push_back(httpConfig_->ipConfig());
  }

  if (httpsConfig_ != nullptr) {
    ipConfigs.push_back(httpsConfig_->ipConfig());
  }

  server_->bind(ipConfigs);

  PRESTO_STARTUP_LOG(INFO) << "proxygen::HTTPServer::start()";
  server_->start(
      [&]() {
        if (onSuccess) {
          onSuccess(server_.get());
        }
      },
      onError,
      nullptr,
      httpIOExecutor_);
}
} // namespace facebook::presto::http
