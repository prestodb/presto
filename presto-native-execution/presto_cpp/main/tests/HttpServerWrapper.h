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

#include "presto_cpp/main/http/HttpServer.h"

namespace facebook::presto::test {
class HttpServerWrapper {
 public:
  explicit HttpServerWrapper(std::unique_ptr<http::HttpServer> server)
      : server_(std::move(server)) {}

  ~HttpServerWrapper() {
    stop();
  }

  folly::SemiFuture<folly::SocketAddress> start();

  void stop();

 private:
  std::unique_ptr<http::HttpServer> server_;
  std::unique_ptr<std::thread> serverThread_;
  folly::Promise<folly::SocketAddress> promise_;
};
} // namespace facebook::presto::test
