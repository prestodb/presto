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
#include "presto_cpp/main/http/HttpServer.h"

namespace facebook::presto::http {

HttpServer::HttpServer(
    const folly::SocketAddress& httpAddress,
    int httpExecThreads)
    : httpAddress_(httpAddress),
      httpExecThreads_(httpExecThreads),
      httpExecutor_{std::make_shared<folly::IOThreadPoolExecutor>(
          httpExecThreads,
          std::make_shared<folly::NamedThreadFactory>("HTTPSrvExec"))} {}

proxygen::RequestHandler*
DispatchingRequestHandlerFactory::EndPoint::checkAndApply(
    const std::string& path,
    proxygen::HTTPMessage* message,
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

void HttpServer::start(
    std::function<void(proxygen::HTTPServer* /*server*/)> onSuccess,
    std::function<void(std::exception_ptr)> onError) {
  proxygen::HTTPServer::IPConfig cfg{
      httpAddress_, proxygen::HTTPServer::Protocol::HTTP};

  proxygen::HTTPServerOptions options;
  // The 'threads' field is not used when we provide our own executor (see us
  // passing httpExecutor_ below) to the start() method. In that case we create
  // executor ourselves with exactly that number of threads.
  options.threads = httpExecThreads_;
  options.idleTimeout = std::chrono::milliseconds(60'000);
  options.enableContentCompression = false;
  options.handlerFactories = proxygen::RequestHandlerChain()
                                 .addThen(std::move(handlerFactory_))
                                 .build();
  // Increase the default flow control to 1MB/10MB
  options.initialReceiveWindow = uint32_t(1 << 20);
  options.receiveStreamWindowSize = uint32_t(1 << 20);
  options.receiveSessionWindowSize = 10 * (1 << 20);
  options.h2cEnabled = true;

  server_ = std::make_unique<proxygen::HTTPServer>(std::move(options));

  std::vector<proxygen::HTTPServer::IPConfig> ips{cfg};
  server_->bind(ips);

  LOG(INFO) << "STARTUP: proxygen::HTTPServer::start()";
  server_->start(
      [&]() {
        if (onSuccess) {
          onSuccess(server_.get());
        }
      },
      onError,
      nullptr,
      httpExecutor_);
}
} // namespace facebook::presto::http
