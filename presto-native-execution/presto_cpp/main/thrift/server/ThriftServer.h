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
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <memory>
#include <string>

#include "PrestoThriftServiceHandler.h"
#include "presto_cpp/main/common/Utils.h"

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
class ThriftConfig {
 public:
  ThriftConfig(
      const folly::SocketAddress& address,
      const std::string& certPath,
      const std::string& keyPath,
      const std::string& supportedCiphers);

  const folly::SocketAddress& getAddress() const {
    return address_;
  }
  const std::string& getCertPath() const {
    return certPath_;
  }
  const std::string& getKeyPath() const {
    return keyPath_;
  }
  const std::string& getSupportedCiphers() const {
    return supportedCiphers_;
  }
  int getTaskExpireTimeMs() const {
    return taskExpireTimeMs_;
  }
  int getStreamExpireTimeMs() const {
    return streamExpireTimeMs_;
  }
  int getMaxRequest() const {
    return maxRequest_;
  }
  int getMaxConnections() const {
    return maxConnections_;
  }
  int getIdleTimeout() const {
    return idleTimeout_;
  }

 private:
  const folly::SocketAddress address_;
  const std::string certPath_;
  const std::string keyPath_;
  const std::string ciphers_;
  const std::string supportedCiphers_;

  int taskExpireTimeMs_;
  int streamExpireTimeMs_;
  int maxRequest_;
  int maxConnections_;
  int idleTimeout_;
};

class ThriftServer {
 public:
  explicit ThriftServer(
      std::unique_ptr<ThriftConfig> config,
      std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
      std::shared_ptr<velox::memory::MemoryPool> pool,
      std::shared_ptr<VeloxPlanValidator> planValidator,
      std::shared_ptr<TaskManager> taskManager);

  void start();

  void stop() {
    server_->stop();
    PRESTO_SHUTDOWN_LOG(INFO) << "Stopping Thrift server...";
  }

  folly::SocketAddress address() const;

 private:
  std::unique_ptr<ThriftConfig> config_;
  std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::unique_ptr<apache::thrift::ThriftServer> server_;
  std::shared_ptr<PrestoThriftServiceHandler> handler_;
};

} // namespace thrift
} // namespace facebook::presto
