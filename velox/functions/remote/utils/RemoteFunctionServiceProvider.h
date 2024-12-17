/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <memory>
#include <string>
#include <thread>

#include <folly/SocketAddress.h>
#include <folly/synchronization/CallOnce.h>

namespace apache::thrift {
class ThriftServer;
} // namespace apache::thrift

namespace facebook::velox::functions {

constexpr std::string_view kRemoteFunctionPrefix = "remote";

struct RemoteFunctionServiceParams {
  // Function prefix to be used for registering the actual functions.
  // This is needed when server is running in the same process.
  std::string functionPrefix;

  // The socket address that the thrift server is running on.
  folly::SocketAddress serverAddress;
};

class IRemoteFunctionServiceProvider {
 public:
  virtual ~IRemoteFunctionServiceProvider() = default;
  virtual RemoteFunctionServiceParams getRemoteFunctionServiceParams() = 0;
};

class RemoteFunctionServiceProviderForLocalThrift
    : public IRemoteFunctionServiceProvider {
 public:
  // Creates a thrift server that runs in a separate thread
  // and returns the parameters of the service.
  RemoteFunctionServiceParams getRemoteFunctionServiceParams() override;

  ~RemoteFunctionServiceProviderForLocalThrift() override;

 private:
  void initializeServer();

  // Loop until the server is up and running.
  bool waitForRunning();

  std::shared_ptr<apache::thrift::ThriftServer> server_;
  std::unique_ptr<std::thread> thread_;

  // Creates a random temporary file name to use to communicate as a unix domain
  // socket.
  folly::SocketAddress location_ = []() {
    char name[] = "/tmp/socketXXXXXX";
    int fd = mkstemp(name);
    if (fd < 0) {
      throw std::runtime_error("Failed to create temporary file for socket");
    }
    close(fd);
    std::string socketPath(name);
    // Cleanup existing socket file if it exists.
    unlink(socketPath.c_str());
    return folly::SocketAddress::makeFromPath(socketPath);
  }();

  const std::string remotePrefix_{kRemoteFunctionPrefix};
  folly::once_flag initializeServiceFlag_;
};

// If no thrift server is currently running, creates a thrift server
// that runs in a separate thread and returns the parameters of the service.
// If a thrift server is already running, returns the parameters of the service.
RemoteFunctionServiceParams startLocalThriftServiceAndGetParams();

} // namespace facebook::velox::functions
