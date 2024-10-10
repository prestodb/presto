#include "presto_cpp/main/tests/PrestoServerWrapper.h"
#include <gtest/gtest.h>
#include "velox/common/base/StatsReporter.h"

namespace facebook::presto::test {

folly::SemiFuture<folly::SocketAddress> PrestoServerWrapper::start() {
  auto [promise, future] = folly::makePromiseContract<folly::SocketAddress>();
  promise_ = std::move(promise);

  // Start the server in a separate thread
  serverThread_ = std::make_unique<std::thread>([this]() {
  server_->run([&](proxygen::HTTPServer* httpServer) {
    ASSERT_EQ(httpServer->addresses().size(), 1);
    promise_.setValue(httpServer->addresses()[0].address);
    }); // PrestoServer's start function
  });

  return std::move(future);
}

void PrestoServerWrapper::stop() {
  if (serverThread_) {
    server_->stop();  // Gracefully stop PrestoServer
    serverThread_->join();
    serverThread_.reset();
  }
}

} // namespace facebook::presto::test
