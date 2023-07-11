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
#include "presto_cpp/main/CoordinatorDiscoverer.h"
#include "presto_cpp/main/PrestoServerOperations.h"
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

class Announcer;
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
  virtual void addAdditionalPeriodicTasks(){};

  virtual void initializeCoordinatorDiscoverer();

  virtual std::shared_ptr<velox::exec::TaskListener> getTaskListener();

  virtual std::shared_ptr<velox::exec::ExprSetListener> getExprSetListener();

  /// Returns any additional http filters.
  virtual std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
  getAdditionalHttpServerFilters();

  virtual std::vector<std::string> registerConnectors(
      const fs::path& configDirectoryPath);

  /// Invoked to register the required dwio data sinks which are used by
  /// connectors.
  virtual void registerFileSinks() {}

  /// Invoked by presto shutdown procedure to unregister connectors.
  virtual void unregisterConnectors();

  virtual void registerShuffleInterfaceFactories();

  virtual void registerCustomOperators();

  virtual void registerFunctions();

  virtual void registerRemoteFunctions();

  virtual void registerVectorSerdes();

  virtual void registerFileSystems();

  virtual void registerStatsCounters();

  /// Invoked after creating global (singleton) config objects (SystemConfig and
  /// NodeConfig) and before loading their properties from the file.
  /// In the implementation any extra config properties can be registered.
  virtual void registerExtraConfigProperties() {}

  /// Invoked to get the ip address of the process. In certain deployment
  /// setup, each process has different ip address. Deployment environment
  /// may provide there own library to get process specific ip address.
  /// In such cases, getLocalIp can be overriden to pass process specific
  /// ip address.
  virtual std::string getLocalIp() const;

  /// Invoked to get the list of filters passed to the http server.
  std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
  getHttpServerFilters();

  void initializeVeloxMemory();

 protected:
  void addServerPeriodicTasks();

  void reportMemoryInfo(proxygen::ResponseHandler* downstream);

  void reportServerInfo(proxygen::ResponseHandler* downstream);

  void reportNodeStatus(proxygen::ResponseHandler* downstream);

  void populateMemAndCPUInfo();

  // Periodically yield tasks if there are tasks queued.
  void yieldTasks();

  const std::string configDirectoryPath_;

  std::shared_ptr<CoordinatorDiscoverer> coordinatorDiscoverer_;

  // Executor for background writing into SSD cache.
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;

  // Executor for async IO for connectors.
  std::unique_ptr<folly::IOThreadPoolExecutor> connectorIoExecutor_;

  // Executor for exchange data over http.
  std::shared_ptr<folly::IOThreadPoolExecutor> exchangeExecutor_;

  // If not null,  the instance of AsyncDataCache used for in-memory file cache.
  std::shared_ptr<velox::cache::AsyncDataCache> cache_;

  // Instance of MemoryAllocator used for all query memory allocations.
  //
  // NOTE: AsyncDataCache implements MemoryAllocator interface and wraps on top
  // of a real memory allocator for the actual memory allocation. So if 'cache_'
  // has been set, 'allocator_' is also set to 'cache_'. It provides memory
  // allocations for both file cache and query memory.
  std::shared_ptr<velox::memory::MemoryAllocator> allocator_;

  std::unique_ptr<http::HttpServer> httpServer_;
  std::unique_ptr<SignalHandler> signalHandler_;
  std::unique_ptr<Announcer> announcer_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<TaskManager> taskManager_;
  std::unique_ptr<TaskResource> taskResource_;
  std::atomic<NodeState> nodeState_{NodeState::ACTIVE};
  std::atomic_bool shuttingDown_{false};
  std::chrono::steady_clock::time_point start_;
  std::unique_ptr<PeriodicTaskManager> periodicTaskManager_;
  std::unique_ptr<PrestoServerOperations> prestoServerOperations_;

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
