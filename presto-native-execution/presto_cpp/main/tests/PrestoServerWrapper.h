#pragma once

#include <gtest/gtest.h>
#include <folly/futures/Future.h>
#include "presto_cpp/main/PrestoServer.h"

namespace facebook::presto::test {

class PrestoServerWrapper {
public:
  // PrestoServerWrapper();
  PrestoServerWrapper(std::unique_ptr<PrestoServer> server)
      : server_(std::move(server)) {}

  folly::SemiFuture<folly::SocketAddress> start();

  void stop();

private:
  std::unique_ptr<PrestoServer> server_;
  std::unique_ptr<std::thread> serverThread_;
  folly::Promise<folly::SocketAddress> promise_;
};

} // namespace facebook::presto::test
