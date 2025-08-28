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
#include <folly/io/async/SSLContext.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <velox/exec/Task.h>
#include <velox/expression/Expr.h>
#include "presto_cpp/main/CPUMon.h"
#include "presto_cpp/main/CoordinatorDiscoverer.h"
#include "presto_cpp/main/PeriodicHeartbeatManager.h"
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/PrestoServerOperations.h"
#include "presto_cpp/main/types/VeloxPlanValidator.h"
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

/// Three states server can be in.
enum class NodeState : int8_t { kActive, kInActive, kShuttingDown };

std::string nodeState2String(NodeState nodeState);

class Announcer;
class SignalHandler;
class TaskManager;
class TaskResource;
class PeriodicMemoryChecker;
class PeriodicTaskManager;
class SystemConfig;

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

  /// Returns true if the worker needs and has a coordinator discovery and the
  /// announcer.
  bool hasCoordinatorDiscoverer() const {
    return coordinatorDiscoverer_ != nullptr;
  }

  /// Returns the number of threads in the Driver executor.
  size_t numDriverThreads() const;

  /// Returns true if the server got terminate signal and in the 'shutting down'
  /// mode. False otherwise.
  bool isShuttingDown() const {
    return *shuttingDown_.rlock();
  }

  /// Set worker into the SHUTTING_DOWN state even if we aren't shutting down.
  /// This will prevent coordinator from sending new tasks to this worker.
  void detachWorker();

  /// Set worker into the ACTIVE state if we aren't shutting down.
  /// This will enable coordinator to send new tasks to this worker.
  void maybeAttachWorker();

  /// Changes this node's state.
  void setNodeState(NodeState nodeState);

  /// Enable/disable announcer (process notifying coordinator about this
  /// worker).
  void enableAnnouncer(bool enable);

 protected:
  virtual void createPeriodicMemoryChecker();

  /// Hook for derived PrestoServer implementations to add/stop additional
  /// periodic tasks.
  virtual void addAdditionalPeriodicTasks(){};

  virtual void stopAdditionalPeriodicTasks(){};

  virtual void initializeCoordinatorDiscoverer();

  virtual void initializeThreadPools();

  virtual std::shared_ptr<velox::exec::TaskListener> getTaskListener();

  virtual std::shared_ptr<velox::exec::ExprSetListener> getExprSetListener();

  virtual std::shared_ptr<facebook::velox::exec::SplitListenerFactory>
    getSplitListenerFactory();

  virtual std::vector<std::string> registerVeloxConnectors(
      const fs::path& configDirectoryPath);

  /// Invoked to register the required dwio data sinks which are used by
  /// connectors.
  virtual void registerFileSinks();

  virtual void registerFileReadersAndWriters();

  virtual void unregisterFileReadersAndWriters();

  /// Invoked by presto shutdown procedure to unregister connectors.
  virtual void unregisterConnectors();

  virtual void registerShuffleInterfaceFactories();

  virtual void registerCustomOperators();

  virtual void registerFunctions();

  virtual void registerRemoteFunctions();

  virtual void registerVectorSerdes();

  virtual void registerFileSystems();

  virtual void unregisterFileSystems();

  virtual void registerMemoryArbitrators();

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

  /// Invoked to get the spill directory.
  virtual std::string getBaseSpillDirectory() const;

  /// Invoked to enable stats reporting and register counters.
  virtual void enableWorkerStatsReporting();

  /// Invoked to initialize Presto to Velox plan validator.
  virtual void initVeloxPlanValidator();

  VeloxPlanValidator* getVeloxPlanValidator();

  void registerDynamicFunctions();

  /// Invoked to get the list of filters passed to the http server.
  virtual std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
  getHttpServerFilters() const;

  void initializeVeloxMemory();

  void registerStatsCounters();

  void updateAnnouncerDetails();

  void addServerPeriodicTasks();

  void reportMemoryInfo(proxygen::ResponseHandler* downstream);

  void reportServerInfo(proxygen::ResponseHandler* downstream);

  void reportNodeStatus(proxygen::ResponseHandler* downstream);

  void reportNodeStats(proxygen::ResponseHandler* downstream);

  void handleGracefulShutdown(
      const std::vector<std::unique_ptr<folly::IOBuf>>& body,
      proxygen::ResponseHandler* downstream);

  protocol::NodeStatus fetchNodeStatus();

  void populateMemAndCPUInfo();

  // Periodically yield tasks if there are tasks queued.
  void yieldTasks();

  void registerSystemConnector();

  void registerSidecarEndpoints();

  std::unique_ptr<velox::cache::SsdCache> setupSsdCache();

  void checkOverload();

  virtual void createTaskManager();

  const std::string configDirectoryPath_;

  std::shared_ptr<CoordinatorDiscoverer> coordinatorDiscoverer_;

  // Executor for background writing into SSD cache.
  std::unique_ptr<folly::CPUThreadPoolExecutor> cacheExecutor_;

  // Executor for async execution for connectors.
  std::unique_ptr<folly::CPUThreadPoolExecutor> connectorCpuExecutor_;

  // Executor for async IO for connectors.
  std::unique_ptr<folly::IOThreadPoolExecutor> connectorIoExecutor_;

  // Executor for exchange data over http.
  std::unique_ptr<folly::IOThreadPoolExecutor> exchangeHttpIoExecutor_;

  // Executor for exchange request processing.
  std::unique_ptr<folly::CPUThreadPoolExecutor> exchangeHttpCpuExecutor_;

  // Executor for HTTP request dispatching
  std::shared_ptr<folly::IOThreadPoolExecutor> httpSrvIoExecutor_;

  // Executor for HTTP request processing after dispatching
  std::unique_ptr<folly::CPUThreadPoolExecutor> httpSrvCpuExecutor_;

  // Executor for query engine driver executions. The underlying thread pool 
  // executor is a folly::CPUThreadPoolExecutor. The executor is stored as 
  // abstract type to provide flexibility of thread pool monitoring. The 
  // underlying folly::CPUThreadPoolExecutor can be obtained through 
  // 'driverCpuExecutor()' method.
  std::unique_ptr<folly::Executor> driverExecutor_;
  // Raw pointer pointing to the underlying folly::CPUThreadPoolExecutor of 
  // 'driverExecutor_'.
  folly::CPUThreadPoolExecutor* driverCpuExecutor_;

  // Executor for spilling.
  std::unique_ptr<folly::CPUThreadPoolExecutor> spillerExecutor_;

  std::unique_ptr<VeloxPlanValidator> planValidator_;

  std::unique_ptr<http::HttpClientConnectionPool> exchangeSourceConnectionPool_;

  // If not null,  the instance of AsyncDataCache used for in-memory file cache.
  std::shared_ptr<velox::cache::AsyncDataCache> cache_;

  std::unique_ptr<http::HttpServer> httpServer_;
  std::unique_ptr<SignalHandler> signalHandler_;
  std::unique_ptr<Announcer> announcer_;
  std::unique_ptr<PeriodicHeartbeatManager> heartbeatManager_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::memory::MemoryPool> nativeWorkerPool_;
  std::unique_ptr<TaskManager> taskManager_;
  std::unique_ptr<TaskResource> taskResource_;
  std::atomic<NodeState> nodeState_{NodeState::kActive};
  folly::Synchronized<bool> shuttingDown_{false};
  std::chrono::steady_clock::time_point start_;
  std::unique_ptr<PeriodicTaskManager> periodicTaskManager_;
  std::unique_ptr<PrestoServerOperations> prestoServerOperations_;
  std::unique_ptr<PeriodicMemoryChecker> memoryChecker_;

  // Last known memory overloaded status.
  bool memOverloaded_{false};
  // Last known CPU overloaded status.
  bool cpuOverloaded_{false};
  // Current worker overloaded status. It can still be true when memory and CPU
  // overloaded flags are not due to cooldown period.
  bool serverOverloaded_{false};
  // Last time point (in seconds) when the worker was overloaded.
  uint64_t lastOverloadedTimeInSecs_{0};

  // We update these members asynchronously and return in http requests w/o
  // delay.
  folly::Synchronized<std::unique_ptr<protocol::MemoryInfo>> memoryInfo_;
  CPUMon cpuMon_;

  std::string environment_;
  std::string nodeVersion_;
  std::string nodeId_;
  std::string address_;
  std::string nodeLocation_;
  std::string nodePoolType_;
  folly::SSLContextPtr sslContext_;
  std::string prestoBuiltinFunctionPrefix_;
};

} // namespace facebook::presto
