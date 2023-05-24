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
#include <folly/Synchronized.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <velox/exec/Task.h>
#include <velox/expression/Expr.h>
#include "presto_cpp/main/CPUMon.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/memory/MemoryAllocator.h"
#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::velox::connector {
class Connector;
}

namespace facebook::velox {
class Config;
}

namespace facebook::presto::http {
class HttpServer;
}

namespace proxygen {
class ResponseHandler;
} // namespace proxygen

namespace facebook::presto::protocol {
struct MemoryInfo;
}

namespace facebook::presto {

// Three states our server can be in.
enum class NodeState { ACTIVE, INACTIVE, SHUTTING_DOWN };

class SignalHandler;
class TaskManager;
class TaskResource;
class PeriodicTaskManager;

class PrestoServer {
 public:
  explicit PrestoServer(const std::string& configDirectoryPath);
  virtual ~PrestoServer();

  void run();

  /// Called from signal handler on signals that should stop the server.
  void stop();

  NodeState nodeState() const {
    return nodeState_;
  }

  void setNodeState(NodeState nodeState) {
    nodeState_ = nodeState;
  }

 protected:
  /// Hook for derived PrestoServer implementations to add additional periodic
  /// tasks.
  virtual void addAdditionalPeriodicTasks();

  virtual std::function<folly::SocketAddress()> discoveryAddressLookup();

  virtual std::shared_ptr<velox::exec::TaskListener> getTaskListener();

  virtual std::shared_ptr<velox::exec::ExprSetListener> getExprSetListener();

  /// Returns any additional http filters.
  virtual std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
  getAdditionalHttpServerFilters();

  virtual std::vector<std::string> registerConnectors(
      const fs::path& configDirectoryPath);

  /// Invoked by presto shutdown procedure to unregister connectors.
  virtual void unregisterConnectors();

  virtual void registerShuffleInterfaceFactories();

  virtual void registerCustomOperators();

  virtual void registerFunctions();

  virtual void registerRemoteFunctions();

  virtual void registerVectorSerdes();

  virtual void registerFileSystems();

  virtual void registerStatsCounters();

  /// Invoked to get the list of filters passed to the http server.
  std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
  getHttpServerFilters();

  void initializeVeloxMemory();

 protected:
  void reportMemoryInfo(proxygen::ResponseHandler* downstream);

  void reportServerInfo(proxygen::ResponseHandler* downstream);

  void reportNodeStatus(proxygen::ResponseHandler* downstream);

  void populateMemAndCPUInfo();

  const std::string configDirectoryPath_;

  // Executor for background writing into SSD cache.
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;

  // Executor for async IO for connectors.
  std::unique_ptr<folly::IOThreadPoolExecutor> connectorIoExecutor_;

  // Instance of AsyncDataCache used for all large allocations.
  std::shared_ptr<velox::cache::AsyncDataCache> cache_;

  std::unique_ptr<http::HttpServer> httpServer_;
  std::unique_ptr<SignalHandler> signalHandler_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<TaskManager> taskManager_;
  std::unique_ptr<TaskResource> taskResource_;
  std::atomic<NodeState> nodeState_{NodeState::ACTIVE};
  std::atomic_bool shuttingDown_{false};
  std::chrono::steady_clock::time_point start_;
  std::unique_ptr<PeriodicTaskManager> periodicTaskManager_;

  // We update these members asynchronously and return in http requests w/o
  // delay.
  folly::Synchronized<std::unique_ptr<protocol::MemoryInfo>> memoryInfo_;
  CPUMon cpuMon_;

  std::string environment_;
  std::string nodeVersion_;
  std::string nodeId_;
  std::string address_;
  std::string nodeLocation_;
};

} // namespace facebook::presto
