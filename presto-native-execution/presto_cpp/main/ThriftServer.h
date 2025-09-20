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
#include <folly/SocketAddress.h>
#include <memory>

namespace apache {
namespace thrift {
class ThriftServer;
}
} // namespace apache

namespace facebook {
namespace velox {
namespace memory {
class MemoryPool;
}
} // namespace velox
} // namespace facebook

namespace facebook::presto {

class TaskManager;
class VeloxPlanValidator;

namespace thrift {

/// Configuration for Thrift server
struct ThriftConfig {
  folly::SocketAddress address;
  int numCpuThreads{8};
  int numIoThreads{4};
  bool reusePort{false};
  std::string certPath;
  std::string keyPath;
  std::string ciphers;
  bool http2Enabled;

  int taskExpireTimeMs;
  int streamExpireTimeMs;
  int maxRequest;
  int sslVerification;

  ThriftConfig(const folly::SocketAddress& addr) : address(addr) {}
};

/// Thrift server wrapper - follows the same pattern as HttpServer
class ThriftServer {
 public:
  explicit ThriftServer(
      std::unique_ptr<ThriftConfig> config,
      facebook::velox::memory::MemoryPool* pool,
      VeloxPlanValidator* planValidator,
      TaskManager* taskManager);

  ~ThriftServer();

  /// Start the Thrift server
  void start();

  /// Stop the Thrift server
  void stop();

  /// Get the server address (after binding)
  folly::SocketAddress address() const;

 private:
  std::unique_ptr<ThriftConfig> config_;
  std::unique_ptr<apache::thrift::ThriftServer> server_;
  facebook::velox::memory::MemoryPool* pool_;
  VeloxPlanValidator* planValidator_;
  TaskManager* taskManager_;
  bool started_{false};
};

} // namespace thrift
} // namespace facebook::presto
