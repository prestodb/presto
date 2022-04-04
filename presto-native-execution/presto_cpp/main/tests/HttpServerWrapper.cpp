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
#include "presto_cpp/main/tests/HttpServerWrapper.h"
#include <gtest/gtest.h>

namespace facebook::presto::test {

folly::SemiFuture<folly::SocketAddress> HttpServerWrapper::start() {
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

void HttpServerWrapper::stop() {
  if (serverThread_) {
    server_->stop();
    serverThread_->join();
    serverThread_.reset();
  }
}
} // namespace facebook::presto::test
