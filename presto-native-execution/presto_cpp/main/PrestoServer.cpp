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
#include "presto_cpp/main/PrestoServer.h"
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <glog/logging.h>
#include "presto_cpp/main/Announcer.h"
#include "presto_cpp/main/CoordinatorDiscoverer.h"
#include "presto_cpp/main/PeriodicMemoryChecker.h"
#include "presto_cpp/main/PeriodicTaskManager.h"
#include "presto_cpp/main/SessionProperties.h"
#include "presto_cpp/main/SignalHandler.h"
#include "presto_cpp/main/TaskResource.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/connectors/Registration.h"
#include "presto_cpp/main/connectors/SystemConnector.h"
#include "presto_cpp/main/http/HttpConstants.h"
#include "presto_cpp/main/http/filters/AccessLogFilter.h"
#include "presto_cpp/main/http/filters/HttpEndpointLatencyFilter.h"
#include "presto_cpp/main/http/filters/InternalAuthenticationFilter.h"
#include "presto_cpp/main/http/filters/StatsFilter.h"
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/UnsafeRowExchangeSource.h"
#include "presto_cpp/main/types/FunctionMetadata.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/main/types/VeloxPlanConversion.h"
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/caching/CacheTTLController.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/dynamic_registry/DynamicLibraryLoader.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/orc/reader/OrcReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/text/RegisterTextWriter.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"

#ifdef PRESTO_ENABLE_CUDF
#include "velox/experimental/cudf/exec/ToCudf.h"
#endif

#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
#include "presto_cpp/main/RemoteFunctionRegisterer.h"
#endif

#ifdef __linux__
// Required by BatchThreadFactory
#include <pthread.h>
#include <sched.h>
#endif

using namespace facebook;

namespace facebook::presto {
namespace {

constexpr char const* kHttp = "http";
constexpr char const* kHttps = "https";
constexpr char const* kTaskUriFormat =
    "{}://{}:{}"; // protocol, address and port
constexpr char const* kConnectorName = "connector.name";
constexpr char const* kLinuxSharedLibExt = ".so";
constexpr char const* kMacOSSharedLibExt = ".dylib";

protocol::NodeState convertNodeState(presto::NodeState nodeState) {
  switch (nodeState) {
    case presto::NodeState::kActive:
      return protocol::NodeState::ACTIVE;
    case presto::NodeState::kInActive:
      return protocol::NodeState::INACTIVE;
    case presto::NodeState::kShuttingDown:
      return protocol::NodeState::SHUTTING_DOWN;
  }
  return protocol::NodeState::ACTIVE; // For gcc build.
}

void enableChecksum() {
  velox::exec::OutputBufferManager::getInstanceRef()->setListenerFactory([]() {
    return std::make_unique<
        velox::serializer::presto::PrestoOutputStreamListener>();
  });
}

// Log only the catalog keys that are configured to avoid leaking
// secret information. Some values represent secrets used to access
// storage backends.
std::string logConnectorConfigPropertyKeys(
    const std::unordered_map<std::string, std::string>& configs) {
  std::stringstream out;
  for (auto const& [key, value] : configs) {
    out << "  " << key << "\n";
  }
  return out.str();
}

bool isCacheTtlEnabled() {
  const auto* systemConfig = SystemConfig::instance();
  if (systemConfig->cacheVeloxTtlEnabled()) {
    VELOX_USER_CHECK(
        systemConfig->cacheVeloxTtlThreshold() > std::chrono::seconds::zero(),
        "Config cache.velox.ttl-threshold must be positive.");
    VELOX_USER_CHECK(
        systemConfig->cacheVeloxTtlCheckInterval() >
            std::chrono::seconds::zero(),
        "Config cache.velox.ttl-check-interval must be positive.");
    return true;
  }
  return false;
}

bool cachePeriodicPersistenceEnabled() {
  const auto* systemConfig = SystemConfig::instance();
  return systemConfig->asyncDataCacheEnabled() &&
      systemConfig->asyncCacheSsdGb() > 0 &&
      systemConfig->asyncCachePersistenceInterval() >
      std::chrono::seconds::zero();
}

bool isSharedLibrary(const fs::path& path) {
  std::string pathExt = path.extension().string();
  std::transform(pathExt.begin(), pathExt.end(), pathExt.begin(), ::tolower);
  return pathExt == kLinuxSharedLibExt || pathExt == kMacOSSharedLibExt;
}

void registerVeloxCudf() {
#ifdef PRESTO_ENABLE_CUDF
  facebook::velox::cudf_velox::CudfOptions::getInstance().setPrefix(
      SystemConfig::instance()->prestoDefaultNamespacePrefix());
  facebook::velox::cudf_velox::registerCudf();
  PRESTO_STARTUP_LOG(INFO) << "cuDF is registered.";
#endif
}

void unregisterVeloxCudf() {
#ifdef PRESTO_ENABLE_CUDF
  facebook::velox::cudf_velox::unregisterCudf();
  PRESTO_SHUTDOWN_LOG(INFO) << "cuDF is unregistered.";
#endif
}

} // namespace

std::string nodeState2String(NodeState nodeState) {
  switch (nodeState) {
    case presto::NodeState::kActive:
      return "active";
    case presto::NodeState::kInActive:
      return "inactive";
    case presto::NodeState::kShuttingDown:
      return "shutting_down";
  }
  return fmt::format("<unknown>:{}>", static_cast<int>(nodeState));
}

PrestoServer::PrestoServer(const std::string& configDirectoryPath)
    : configDirectoryPath_(configDirectoryPath),
      signalHandler_(std::make_unique<SignalHandler>(this)),
      start_(std::chrono::steady_clock::now()),
      memoryInfo_(std::make_unique<protocol::MemoryInfo>()) {}

PrestoServer::~PrestoServer() {}

void PrestoServer::run() {
  auto systemConfig = SystemConfig::instance();
  auto nodeConfig = NodeConfig::instance();
  int httpPort{0};

  std::string certPath;
  std::string keyPath;
  std::string ciphers;
  std::string clientCertAndKeyPath;
  std::optional<int> httpsPort;

  try {
    // Allow registering extra config properties before we load them from files.
    registerExtraConfigProperties();
    systemConfig->initialize(
        fmt::format("{}/config.properties", configDirectoryPath_));
    nodeConfig->initialize(
        fmt::format("{}/node.properties", configDirectoryPath_));

    httpPort = systemConfig->httpServerHttpPort();
    if (systemConfig->httpServerHttpsEnabled()) {
      httpsPort = systemConfig->httpServerHttpsPort();

      ciphers = systemConfig->httpsSupportedCiphers();
      if (ciphers.empty()) {
        VELOX_USER_FAIL("Https is enabled without ciphers");
      }

      auto optionalCertPath = systemConfig->httpsCertPath();
      if (!optionalCertPath.has_value()) {
        VELOX_USER_FAIL("Https is enabled without certificate path");
      }
      certPath = optionalCertPath.value();

      auto optionalKeyPath = systemConfig->httpsKeyPath();
      if (!optionalKeyPath.has_value()) {
        VELOX_USER_FAIL("Https is enabled without key path");
      }
      keyPath = optionalKeyPath.value();

      auto optionalClientCertPath = systemConfig->httpsClientCertAndKeyPath();
      if (!optionalClientCertPath.has_value()) {
        // This config is not used in server but validated here, otherwise, it
        // will fail later in the HttpClient during query execution.
        VELOX_USER_FAIL(
            "Https Client Certificates are not configured correctly");
      }

      sslContext_ =
          util::createSSLContext(optionalClientCertPath.value(), ciphers);
    }

    if (systemConfig->internalCommunicationJwtEnabled()) {
#ifndef PRESTO_ENABLE_JWT
      VELOX_USER_FAIL("Internal JWT is enabled but not supported");
#endif
      VELOX_USER_CHECK(
          !(systemConfig->internalCommunicationSharedSecret().empty()),
          "Internal JWT is enabled without a corresponding shared secret");
    }

    nodeVersion_ = systemConfig->prestoVersion();
    environment_ = nodeConfig->nodeEnvironment();
    nodeId_ = nodeConfig->nodeId();
    address_ = nodeConfig->nodeInternalAddress(
        std::bind(&PrestoServer::getLocalIp, this));
    // Add [] to an ipv6 address.
    if (address_.find(':') != std::string::npos && address_.front() != '[') {
      address_ = fmt::format("[{}]", address_);
    }
    nodeLocation_ = nodeConfig->nodeLocation();
    nodePoolType_ = systemConfig->poolType();
    prestoBuiltinFunctionPrefix_ = systemConfig->prestoDefaultNamespacePrefix();
  } catch (const velox::VeloxUserError& e) {
    PRESTO_STARTUP_LOG(ERROR) << "Failed to start server due to " << e.what();
    exit(EXIT_FAILURE);
  }

  registerFileSystems();
  registerFileSinks();
  registerFileReadersAndWriters();
  registerMemoryArbitrators();
  registerShuffleInterfaceFactories();
  registerCustomOperators();

  // Register Presto connector factories and connectors
  registerConnectors();

  initializeVeloxMemory();
  initializeThreadPools();

  auto catalogNames = registerVeloxConnectors(fs::path(configDirectoryPath_));

  const bool bindToNodeInternalAddressOnly =
      systemConfig->httpServerBindToNodeInternalAddressOnlyEnabled();
  folly::SocketAddress httpSocketAddress;
  if (bindToNodeInternalAddressOnly) {
    httpSocketAddress.setFromHostPort(address_, httpPort);
  } else {
    httpSocketAddress.setFromLocalPort(httpPort);
  }
  PRESTO_STARTUP_LOG(INFO) << fmt::format(
      "Starting server at {}:{} ({})",
      httpSocketAddress.getIPAddress().str(),
      httpPort,
      address_);

  initializeCoordinatorDiscoverer();

  const bool reusePort = SystemConfig::instance()->httpServerReusePort();
  auto httpConfig =
      std::make_unique<http::HttpConfig>(httpSocketAddress, reusePort);

  std::unique_ptr<http::HttpsConfig> httpsConfig;
  if (httpsPort.has_value()) {
    folly::SocketAddress httpsSocketAddress;
    if (bindToNodeInternalAddressOnly) {
      httpsSocketAddress.setFromHostPort(address_, httpsPort.value());
    } else {
      httpsSocketAddress.setFromLocalPort(httpsPort.value());
    }

    const bool http2Enabled =
        SystemConfig::instance()->httpServerHttp2Enabled();
    httpsConfig = std::make_unique<http::HttpsConfig>(
        httpsSocketAddress,
        certPath,
        keyPath,
        ciphers,
        reusePort,
        http2Enabled);
  }

  httpServer_ = std::make_unique<http::HttpServer>(
      httpSrvIoExecutor_, std::move(httpConfig), std::move(httpsConfig));

  httpServer_->registerPost(
      "/v1/memory",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        server->reportMemoryInfo(downstream);
      });
  httpServer_->registerGet(
      "/v1/info",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        server->reportServerInfo(downstream);
      });
  httpServer_->registerGet(
      "/v1/info/state",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        json infoStateJson = convertNodeState(server->nodeState());
        http::sendOkResponse(downstream, infoStateJson);
      });
  httpServer_->registerGet(
      "/v1/info/stats",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        server->reportNodeStats(downstream);
      });
  httpServer_->registerPut(
      "/v1/info/state",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& body,
          proxygen::ResponseHandler* downstream) {
        server->handleGracefulShutdown(body, downstream);
      });
  httpServer_->registerGet(
      "/v1/status",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        server->reportNodeStatus(downstream);
      });
  httpServer_->registerHead(
      "/v1/status",
      [](proxygen::HTTPMessage* /*message*/,
         const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
         proxygen::ResponseHandler* downstream) {
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpOk, "OK")
            .header(
                proxygen::HTTP_HEADER_CONTENT_TYPE,
                http::kMimeTypeApplicationJson)
            .sendWithEOM();
      });

  if (systemConfig->enableRuntimeMetricsCollection()) {
    enableWorkerStatsReporting();
    if (folly::Singleton<velox::BaseStatsReporter>::try_get()) {
      httpServer_->registerGet(
          "/v1/info/metrics",
          [](proxygen::HTTPMessage* /*message*/,
             const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
             proxygen::ResponseHandler* downstream) {
            http::sendOkResponse(
                downstream,
                folly::Singleton<velox::BaseStatsReporter>::try_get()
                    ->fetchMetrics());
          });
    }
  }
  registerVeloxCudf();
  registerFunctions();
  registerRemoteFunctions();
  registerVectorSerdes();
  registerPrestoPlanNodeSerDe();
  registerDynamicFunctions();

  facebook::velox::exec::ExchangeSource::registerFactory(
      [this](
          const std::string& taskId,
          int destination,
          std::shared_ptr<velox::exec::ExchangeQueue> queue,
          velox::memory::MemoryPool* pool) {
        return PrestoExchangeSource::create(
            taskId,
            destination,
            queue,
            pool,
            exchangeHttpCpuExecutor_.get(),
            exchangeHttpIoExecutor_.get(),
            exchangeSourceConnectionPool_.get(),
            sslContext_);
      });

  velox::exec::ExchangeSource::registerFactory(
      operators::UnsafeRowExchangeSource::createExchangeSource);

  // Batch broadcast exchange source.
  velox::exec::ExchangeSource::registerFactory(
      operators::BroadcastExchangeSource::createExchangeSource);

  pool_ =
      velox::memory::MemoryManager::getInstance()->addLeafPool("PrestoServer");
  nativeWorkerPool_ = velox::memory::MemoryManager::getInstance()->addLeafPool(
      "PrestoNativeWorker");

  createTaskManager();

  if (systemConfig->prestoNativeSidecar()) {
    registerSidecarEndpoints();
  }

  taskManager_->setNodeId(nodeId_);
  taskManager_->setOldTaskCleanUpMs(systemConfig->oldTaskCleanUpMs());

  auto baseSpillDirectory = getBaseSpillDirectory();
  if (!baseSpillDirectory.empty()) {
    taskManager_->setBaseSpillDirectory(baseSpillDirectory);
    PRESTO_STARTUP_LOG(INFO)
        << "Spilling root directory: " << baseSpillDirectory;
  }

  initVeloxPlanValidator();
  taskResource_ = std::make_unique<TaskResource>(
      pool_.get(),
      httpSrvCpuExecutor_.get(),
      getVeloxPlanValidator(),
      *taskManager_);
  taskResource_->registerUris(*httpServer_);
  if (systemConfig->enableSerializedPageChecksum()) {
    enableChecksum();
  }

  if (systemConfig->enableVeloxTaskLogging()) {
    if (auto listener = getTaskListener()) {
      velox::exec::registerTaskListener(listener);
    }
  }

  if (auto factory = getSplitListenerFactory()) {
    velox::exec::registerSplitListenerFactory(factory);
  }

  if (systemConfig->enableVeloxExprSetLogging()) {
    if (auto listener = getExprSetListener()) {
      velox::exec::registerExprSetListener(listener);
    }
  }
  prestoServerOperations_ =
      std::make_unique<PrestoServerOperations>(taskManager_.get(), this);
  registerSystemConnector();

  // The endpoint used by operation in production.
  httpServer_->registerGet(
      "/v1/operation/.*",
      [this](
          proxygen::HTTPMessage* message,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        prestoServerOperations_->runOperation(message, downstream);
      });

  PRESTO_STARTUP_LOG(INFO) << "Driver CPU executor '"
                           << driverCpuExecutor_->getName() << "' has "
                           << driverCpuExecutor_->numThreads() << " threads.";
  if (httpServer_->getExecutor()) {
    PRESTO_STARTUP_LOG(INFO)
        << "HTTP Server IO executor '" << httpServer_->getExecutor()->getName()
        << "' has " << httpServer_->getExecutor()->numThreads() << " threads.";
  }
  if (httpSrvCpuExecutor_ != nullptr) {
    PRESTO_STARTUP_LOG(INFO)
        << "HTTP Server CPU executor '" << httpSrvCpuExecutor_->getName()
        << "' has " << httpSrvCpuExecutor_->numThreads() << " threads.";
    for (auto evb : httpSrvIoExecutor_->getAllEventBases()) {
      evb->setMaxLatency(
          std::chrono::milliseconds(
              systemConfig->httpSrvIoEvbViolationThresholdMs()),
          []() { RECORD_METRIC_VALUE(kCounterHttpServerIoEvbViolation, 1); },
          /*dampen=*/false);
    }
  }
  if (spillerExecutor_ != nullptr) {
    PRESTO_STARTUP_LOG(INFO)
        << "Spiller CPU executor '" << spillerExecutor_->getName() << "', has "
        << spillerExecutor_->numThreads() << " threads.";
  } else {
    PRESTO_STARTUP_LOG(INFO) << "Spill executor was not configured.";
  }

  PRESTO_STARTUP_LOG(INFO) << "Starting all periodic tasks";

  auto* memoryAllocator = velox::memory::memoryManager()->allocator();
  auto* asyncDataCache = velox::cache::AsyncDataCache::getInstance();
  periodicTaskManager_ = std::make_unique<PeriodicTaskManager>(
      driverCpuExecutor_,
      spillerExecutor_.get(),
      httpSrvIoExecutor_.get(),
      httpSrvCpuExecutor_.get(),
      exchangeHttpIoExecutor_.get(),
      exchangeHttpCpuExecutor_.get(),
      taskManager_.get(),
      memoryAllocator,
      asyncDataCache,
      velox::connector::getAllConnectors(),
      this);
  addServerPeriodicTasks();
  addAdditionalPeriodicTasks();
  periodicTaskManager_->start();
  createPeriodicMemoryChecker();
  if (memoryChecker_ != nullptr) {
    memoryChecker_->start();
  }

  auto setTaskUriCb = [&](bool useHttps, int port) {
    std::string taskUri;
    if (useHttps) {
      taskUri = fmt::format(kTaskUriFormat, kHttps, address_, port);
    } else {
      taskUri = fmt::format(kTaskUriFormat, kHttp, address_, port);
    }
    taskManager_->setBaseUri(taskUri);
  };

  auto startAnnouncerAndHeartbeatManagerCb = [&](bool useHttps, int port) {
    if (coordinatorDiscoverer_ != nullptr) {
      announcer_ = std::make_unique<Announcer>(
          address_,
          useHttps,
          port,
          coordinatorDiscoverer_,
          nodeVersion_,
          environment_,
          nodeId_,
          nodeLocation_,
          nodePoolType_,
          systemConfig->prestoNativeSidecar(),
          catalogNames,
          systemConfig->announcementMaxFrequencyMs(),
          sslContext_);
      updateAnnouncerDetails();
      announcer_->start();

      uint64_t heartbeatFrequencyMs = systemConfig->heartbeatFrequencyMs();
      if (heartbeatFrequencyMs > 0) {
        heartbeatManager_ = std::make_unique<PeriodicHeartbeatManager>(
            address_,
            port,
            coordinatorDiscoverer_,
            sslContext_,
            [server = this]() { return server->fetchNodeStatus(); },
            heartbeatFrequencyMs);
        heartbeatManager_->start();
      }
    }
  };

  // Start everything. After the return from the following call we are shutting
  // down.
  httpServer_->start(getHttpServerFilters(), [&](proxygen::HTTPServer* server) {
    const auto addresses = server->addresses();
    for (auto address : addresses) {
      PRESTO_STARTUP_LOG(INFO) << fmt::format(
          "Server listening at {}:{} - https {}",
          address.address.getIPAddress().str(),
          address.address.getPort(),
          address.sslConfigs.size() != 0);
      // We could be bound to both http and https ports.
      // If set, we must use the https port and skip http.
      if (httpsPort.has_value() && address.sslConfigs.size() == 0) {
        continue;
      }
      startAnnouncerAndHeartbeatManagerCb(
          httpsPort.has_value(), address.address.getPort());
      setTaskUriCb(httpsPort.has_value(), address.address.getPort());
      break;
    }

    if (coordinatorDiscoverer_ != nullptr) {
      VELOX_CHECK_NOT_NULL(
          announcer_,
          "The announcer is expected to have been created but wasn't.");
      const auto heartbeatFrequencyMs = systemConfig->heartbeatFrequencyMs();
      if (heartbeatFrequencyMs > 0) {
        VELOX_CHECK_NOT_NULL(
            heartbeatManager_,
            "The heartbeat manager is expected to have been created but wasn't.");
      }
    }
  });

  if (announcer_ != nullptr) {
    PRESTO_SHUTDOWN_LOG(INFO) << "Stopping announcer";
    announcer_->stop();
  }

  if (heartbeatManager_ != nullptr) {
    PRESTO_SHUTDOWN_LOG(INFO) << "Stopping Heartbeat manager";
    heartbeatManager_->stop();
  }

  PRESTO_SHUTDOWN_LOG(INFO) << "Stopping all periodic tasks";

  if (memoryChecker_ != nullptr) {
    memoryChecker_->stop();
  }
  periodicTaskManager_->stop();
  stopAdditionalPeriodicTasks();

  // Destroy entities here to ensure we won't get any messages after Server
  // object is gone and to have nice log in case shutdown gets stuck.
  PRESTO_SHUTDOWN_LOG(INFO) << "Destroying Task Resource";
  taskResource_.reset();
  PRESTO_SHUTDOWN_LOG(INFO) << "Destroying Task Manager";
  taskManager_.reset();
  PRESTO_SHUTDOWN_LOG(INFO) << "Destroying HTTP Server";
  httpServer_.reset();

  unregisterFileReadersAndWriters();
  unregisterFileSystems();
  unregisterConnectors();
  unregisterVeloxCudf();

  PRESTO_SHUTDOWN_LOG(INFO)
      << "Joining Driver CPU Executor '" << driverCpuExecutor_->getName()
      << "': threads: " << driverCpuExecutor_->numActiveThreads() << "/"
      << driverCpuExecutor_->numThreads()
      << ", task queue: " << driverCpuExecutor_->getTaskQueueSize();
  // Schedule release of SessionPools held by HttpClients before the exchange
  // HTTP IO executor threads are joined.
  driverExecutor_.reset();

  if (connectorCpuExecutor_) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Joining Connector CPU Executor '"
        << connectorCpuExecutor_->getName()
        << "': threads: " << connectorCpuExecutor_->numActiveThreads() << "/"
        << connectorCpuExecutor_->numThreads();
    connectorCpuExecutor_->join();
  }

  if (connectorIoExecutor_) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Joining Connector IO Executor '" << connectorIoExecutor_->getName()
        << "': threads: " << connectorIoExecutor_->numActiveThreads() << "/"
        << connectorIoExecutor_->numThreads();
    connectorIoExecutor_->join();
  }

  if (httpSrvCpuExecutor_ != nullptr) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Joining HTTP Server CPU Executor '"
        << httpSrvCpuExecutor_->getName()
        << "': threads: " << httpSrvCpuExecutor_->numActiveThreads() << "/"
        << httpSrvCpuExecutor_->numThreads()
        << ", task queue: " << httpSrvCpuExecutor_->getTaskQueueSize();
    httpSrvCpuExecutor_->join();
  }
  if (httpSrvIoExecutor_ != nullptr) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Joining HTTP Server IO Executor '" << httpSrvIoExecutor_->getName()
        << "': threads: " << httpSrvIoExecutor_->numActiveThreads() << "/"
        << httpSrvIoExecutor_->numThreads();
    httpSrvIoExecutor_->join();
  }

  PRESTO_SHUTDOWN_LOG(INFO)
      << "Joining Exchange Http CPU executor '"
      << exchangeHttpCpuExecutor_->getName()
      << "': threads: " << exchangeHttpCpuExecutor_->numActiveThreads() << "/"
      << exchangeHttpCpuExecutor_->numThreads();
  exchangeHttpCpuExecutor_->join();
  // Schedule release of SessionPools held by HttpClients before the exchange
  // HTTP IO executor threads are joined.
  exchangeHttpCpuExecutor_.reset();

  if (exchangeSourceConnectionPool_) {
    // Connection pool needs to be destroyed after CPU threads are joined but
    // before IO threads are joined.
    PRESTO_SHUTDOWN_LOG(INFO) << "Releasing exchange HTTP connection pools";
    exchangeSourceConnectionPool_->destroy();
  }

  PRESTO_SHUTDOWN_LOG(INFO)
      << "Joining Exchange Http IO executor '"
      << exchangeHttpIoExecutor_->getName()
      << "': threads: " << exchangeHttpIoExecutor_->numActiveThreads() << "/"
      << exchangeHttpIoExecutor_->numThreads();
  exchangeHttpIoExecutor_->join();

  PRESTO_SHUTDOWN_LOG(INFO) << "Done joining our executors.";

  auto globalCPUKeepAliveExec = folly::getGlobalCPUExecutor();
  if (auto* pGlobalCPUExecutor = dynamic_cast<folly::CPUThreadPoolExecutor*>(
          globalCPUKeepAliveExec.get())) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Global CPU Executor '" << pGlobalCPUExecutor->getName()
        << "': threads: " << pGlobalCPUExecutor->numActiveThreads() << "/"
        << pGlobalCPUExecutor->numThreads()
        << ", task queue: " << pGlobalCPUExecutor->getTaskQueueSize();
  }

  auto globalIOKeepAliveExec = folly::getGlobalIOExecutor();
  if (auto* pGlobalIOExecutor = dynamic_cast<folly::IOThreadPoolExecutor*>(
          globalIOKeepAliveExec.get())) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Global IO Executor '" << pGlobalIOExecutor->getName()
        << "': threads: " << pGlobalIOExecutor->numActiveThreads() << "/"
        << pGlobalIOExecutor->numThreads();
  }

  if (cache_ != nullptr) {
    PRESTO_SHUTDOWN_LOG(INFO) << "Shutdown AsyncDataCache";
    cache_->shutdown();
  }
}

void PrestoServer::yieldTasks() {
  const auto timeslice = SystemConfig::instance()->taskRunTimeSliceMicros();
  if (timeslice <= 0) {
    return;
  }
  static std::atomic<int32_t> numYields = 0;
  const auto numQueued = driverCpuExecutor_->getTaskQueueSize();
  if (numQueued > 0) {
    numYields += taskManager_->yieldTasks(numQueued, timeslice);
  }
  if (numYields > 100'000) {
    LOG(INFO) << "Yielded " << numYields << " more threads.";
    numYields = 0;
  }
}

#ifdef __linux__
class BatchThreadFactory : public folly::NamedThreadFactory {
 public:
  explicit BatchThreadFactory(const std::string& name)
      : NamedThreadFactory{name} {}

  std::thread newThread(folly::Func&& func) override {
    return folly::NamedThreadFactory::newThread([_func = std::move(
                                                     func)]() mutable {
      sched_param param;
      param.sched_priority = 0;
      const int ret =
          pthread_setschedparam(pthread_self(), SCHED_BATCH, &param);
      VELOX_CHECK_EQ(
          ret, 0, "Failed to set a thread priority: {}", folly::errnoStr(ret));
      _func();
    });
  }
};
#endif

void PrestoServer::initializeThreadPools() {
  const auto hwConcurrency = std::thread::hardware_concurrency();
  auto* systemConfig = SystemConfig::instance();

  const auto numDriverCpuThreads = std::max<size_t>(
      systemConfig->driverNumCpuThreadsHwMultiplier() * hwConcurrency, 1);

  std::shared_ptr<folly::NamedThreadFactory> threadFactory;
  if (systemConfig->driverThreadsBatchSchedulingEnabled()) {
#ifdef __linux__
    threadFactory = std::make_shared<BatchThreadFactory>("Driver");
#else
    VELOX_FAIL("Batch scheduling policy can only be enabled on Linux");
#endif
  } else {
    threadFactory = std::make_shared<folly::NamedThreadFactory>("Driver");
  }

  auto driverExecutor = std::make_unique<folly::CPUThreadPoolExecutor>(
      numDriverCpuThreads, threadFactory);
  driverCpuExecutor_ = driverExecutor.get();
  driverExecutor_ = std::move(driverExecutor);

  const auto numIoThreads = std::max<size_t>(
      systemConfig->httpServerNumIoThreadsHwMultiplier() * hwConcurrency, 1);
  httpSrvIoExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(
      numIoThreads, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));

  const auto numCpuThreads = std::max<size_t>(
      systemConfig->httpServerNumCpuThreadsHwMultiplier() * hwConcurrency, 1);
  httpSrvCpuExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      numCpuThreads, std::make_shared<folly::NamedThreadFactory>("HTTPSrvCpu"));

  const auto numSpillerCpuThreads = std::max<size_t>(
      systemConfig->spillerNumCpuThreadsHwMultiplier() * hwConcurrency, 0);
  if (numSpillerCpuThreads > 0) {
    spillerExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        numSpillerCpuThreads,
        std::make_shared<folly::NamedThreadFactory>("Spiller"));
  }

  const auto numExchangeHttpClientIoThreads = std::max<size_t>(
      systemConfig->exchangeHttpClientNumIoThreadsHwMultiplier() *
          std::thread::hardware_concurrency(),
      1);
  exchangeHttpIoExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
      numExchangeHttpClientIoThreads,
      std::make_shared<folly::NamedThreadFactory>("ExchangeIO"));

  PRESTO_STARTUP_LOG(INFO) << "Exchange Http IO executor '"
                           << exchangeHttpIoExecutor_->getName() << "' has "
                           << exchangeHttpIoExecutor_->numThreads()
                           << " threads.";
  for (auto evb : exchangeHttpIoExecutor_->getAllEventBases()) {
    evb->setMaxLatency(
        std::chrono::milliseconds(
            systemConfig->exchangeIoEvbViolationThresholdMs()),
        []() { RECORD_METRIC_VALUE(kCounterExchangeIoEvbViolation, 1); },
        /*dampen=*/false);
  }

  const auto numExchangeHttpClientCpuThreads = std::max<size_t>(
      systemConfig->exchangeHttpClientNumCpuThreadsHwMultiplier() *
          std::thread::hardware_concurrency(),
      1);

  exchangeHttpCpuExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      numExchangeHttpClientCpuThreads,
      std::make_shared<folly::NamedThreadFactory>("ExchangeCPU"));

  PRESTO_STARTUP_LOG(INFO) << "Exchange Http CPU executor '"
                           << exchangeHttpCpuExecutor_->getName() << "' has "
                           << exchangeHttpCpuExecutor_->numThreads()
                           << " threads.";

  if (systemConfig->exchangeEnableConnectionPool()) {
    PRESTO_STARTUP_LOG(INFO) << "Enable exchange Http Client connection pool.";
    exchangeSourceConnectionPool_ =
        std::make_unique<http::HttpClientConnectionPool>();
  }
}

std::unique_ptr<velox::cache::SsdCache> PrestoServer::setupSsdCache() {
  VELOX_CHECK_NULL(cacheExecutor_);
  auto* systemConfig = SystemConfig::instance();
  if (systemConfig->asyncCacheSsdGb() == 0) {
    return nullptr;
  }

  constexpr int32_t kNumSsdShards = 16;
  cacheExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      kNumSsdShards, std::make_shared<folly::NamedThreadFactory>("SsdCache"));
  velox::cache::SsdCache::Config cacheConfig(
      systemConfig->asyncCacheSsdPath(),
      systemConfig->asyncCacheSsdGb() << 30,
      kNumSsdShards,
      cacheExecutor_.get(),
      systemConfig->asyncCacheSsdCheckpointGb() << 30,
      systemConfig->asyncCacheSsdDisableFileCow(),
      systemConfig->ssdCacheChecksumEnabled(),
      systemConfig->ssdCacheReadVerificationEnabled());
  PRESTO_STARTUP_LOG(INFO) << "Initializing SSD cache with "
                           << cacheConfig.toString();
  return std::make_unique<velox::cache::SsdCache>(cacheConfig);
}

void PrestoServer::initializeVeloxMemory() {
  auto* systemConfig = SystemConfig::instance();
  const uint64_t memoryGb = systemConfig->systemMemoryGb();
  PRESTO_STARTUP_LOG(INFO) << "Starting with node memory " << memoryGb << "GB";

  // Set up velox memory manager.
  velox::memory::MemoryManager::Options options;
  options.allocatorCapacity = memoryGb << 30;
  if (systemConfig->useMmapAllocator()) {
    options.useMmapAllocator = true;
  }
  options.checkUsageLeak = systemConfig->enableMemoryLeakCheck();
  options.trackDefaultUsage =
      systemConfig->enableSystemMemoryPoolUsageTracking();
  options.coreOnAllocationFailureEnabled =
      systemConfig->coreOnAllocationFailureEnabled();
  if (!systemConfig->memoryArbitratorKind().empty()) {
    options.arbitratorKind = systemConfig->memoryArbitratorKind();
    const uint64_t queryMemoryGb = systemConfig->queryMemoryGb();
    VELOX_USER_CHECK_LE(
        queryMemoryGb,
        memoryGb,
        "Query memory capacity must not be larger than system memory capacity");
    options.arbitratorCapacity = queryMemoryGb << 30;
    const uint64_t sharedArbitratorReservedMemoryGb = velox::config::toCapacity(
        systemConfig->sharedArbitratorReservedCapacity(),
        velox::config::CapacityUnit::GIGABYTE);
    VELOX_USER_CHECK_LE(
        sharedArbitratorReservedMemoryGb,
        queryMemoryGb,
        "Shared arbitrator reserved memory capacity must not be larger than "
        "query memory capacity");

    options.largestSizeClassPages = systemConfig->largestSizeClassPages();
    options.arbitrationStateCheckCb = velox::exec::memoryArbitrationStateCheck;

    using SharedArbitratorConfig = velox::memory::SharedArbitrator::ExtraConfig;
    options.extraArbitratorConfigs = {
        {std::string(SharedArbitratorConfig::kReservedCapacity),
         systemConfig->sharedArbitratorReservedCapacity()},
        {std::string(SharedArbitratorConfig::kMemoryPoolInitialCapacity),
         systemConfig->sharedArbitratorMemoryPoolInitialCapacity()},
        {std::string(SharedArbitratorConfig::kMemoryPoolReservedCapacity),
         systemConfig->sharedArbitratorMemoryPoolReservedCapacity()},
        {std::string(SharedArbitratorConfig::kMaxMemoryArbitrationTime),
         systemConfig->sharedArbitratorMaxMemoryArbitrationTime()},
        {std::string(SharedArbitratorConfig::kMemoryPoolMinFreeCapacity),
         systemConfig->sharedArbitratorMemoryPoolMinFreeCapacity()},
        {std::string(SharedArbitratorConfig::kMemoryPoolMinFreeCapacityPct),
         systemConfig->sharedArbitratorMemoryPoolMinFreeCapacityPct()},
        {std::string(SharedArbitratorConfig::kGlobalArbitrationEnabled),
         systemConfig->sharedArbitratorGlobalArbitrationEnabled()},
        {std::string(
             SharedArbitratorConfig::kFastExponentialGrowthCapacityLimit),
         systemConfig->sharedArbitratorFastExponentialGrowthCapacityLimit()},
        {std::string(SharedArbitratorConfig::kSlowCapacityGrowPct),
         systemConfig->sharedArbitratorSlowCapacityGrowPct()},
        {std::string(SharedArbitratorConfig::kCheckUsageLeak),
         folly::to<std::string>(systemConfig->enableMemoryLeakCheck())},
        {std::string(SharedArbitratorConfig::kMemoryPoolAbortCapacityLimit),
         systemConfig->sharedArbitratorMemoryPoolAbortCapacityLimit()},
        {std::string(SharedArbitratorConfig::kMemoryPoolMinReclaimBytes),
         systemConfig->sharedArbitratorMemoryPoolMinReclaimBytes()},
        {std::string(SharedArbitratorConfig::kMemoryReclaimThreadsHwMultiplier),
         systemConfig->sharedArbitratorMemoryReclaimThreadsHwMultiplier()},
        {std::string(
             SharedArbitratorConfig::kGlobalArbitrationMemoryReclaimPct),
         systemConfig->sharedArbitratorGlobalArbitrationMemoryReclaimPct()},
        {std::string(SharedArbitratorConfig::kGlobalArbitrationAbortTimeRatio),
         systemConfig->sharedArbitratorGlobalArbitrationAbortTimeRatio()},
        {std::string(SharedArbitratorConfig::kGlobalArbitrationWithoutSpill),
         systemConfig->sharedArbitratorGlobalArbitrationWithoutSpill()}};
  }
  velox::memory::initializeMemoryManager(options);
  PRESTO_STARTUP_LOG(INFO) << "Memory manager has been setup: "
                           << velox::memory::memoryManager()->toString();

  if (systemConfig->asyncDataCacheEnabled()) {
    std::unique_ptr<velox::cache::SsdCache> ssd = setupSsdCache();
    std::string cacheStr =
        ssd == nullptr ? "AsyncDataCache" : "AsyncDataCache with SSD";

    velox::cache::AsyncDataCache::Options cacheOptions{
        systemConfig->asyncCacheMaxSsdWriteRatio(),
        systemConfig->asyncCacheSsdSavableRatio(),
        systemConfig->asyncCacheMinSsdSavableBytes()};
    cache_ = velox::cache::AsyncDataCache::create(
        velox::memory::memoryManager()->allocator(),
        std::move(ssd),
        cacheOptions);
    velox::cache::AsyncDataCache::setInstance(cache_.get());
    PRESTO_STARTUP_LOG(INFO) << cacheStr << " has been setup";

    if (isCacheTtlEnabled()) {
      velox::cache::CacheTTLController::create(*cache_);
      PRESTO_STARTUP_LOG(INFO) << fmt::format(
          "Cache TTL is enabled, with TTL {} enforced every {}.",
          velox::succinctMillis(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  systemConfig->cacheVeloxTtlThreshold())
                  .count()),
          velox::succinctMillis(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  systemConfig->cacheVeloxTtlCheckInterval())
                  .count()));
    }
  } else {
    VELOX_CHECK_EQ(
        systemConfig->asyncCacheSsdGb(),
        0,
        "Async data cache cannot be disabled if ssd cache is enabled");
  }
}

void PrestoServer::stop() {
  // Make sure we only go here once and change the state under the lock.
  {
    auto writeLockedShuttingDown = shuttingDown_.wlock();
    if (*writeLockedShuttingDown) {
      return;
    }

    PRESTO_SHUTDOWN_LOG(INFO) << "Shutdown has been requested. "
                                 "Setting node state to 'shutting down'.";
    *writeLockedShuttingDown = true;
    setNodeState(NodeState::kShuttingDown);
  }

  auto shutdownOnsetSec = SystemConfig::instance()->shutdownOnsetSec();
  PRESTO_SHUTDOWN_LOG(INFO)
      << "Waiting for " << shutdownOnsetSec
      << " second(s) before proceeding with the shutdown...";
  // Give coordinator some time to receive our new node state and stop sending
  // any tasks.
  std::this_thread::sleep_for(std::chrono::seconds(shutdownOnsetSec));

  taskManager_->shutdown();

  // Give coordinator some time to request tasks stats for completed or failed
  // tasks.
  std::this_thread::sleep_for(std::chrono::seconds(shutdownOnsetSec));

  if (httpServer_) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "All tasks are completed. Stopping HTTP Server...";
    httpServer_->stop();
    PRESTO_SHUTDOWN_LOG(INFO) << "HTTP Server stopped.";
  }
}

size_t PrestoServer::numDriverThreads() const {
  VELOX_CHECK(
      driverExecutor_ != nullptr,
      "Driver executor is expected to be not null, but it is null!");
  return driverCpuExecutor_->numThreads();
}

void PrestoServer::detachWorker() {
  auto readLockedShuttingDown = shuttingDown_.rlock();
  if (!*readLockedShuttingDown && nodeState() == NodeState::kActive) {
    // Benefit of shutting down is that the queries that aren't stuck yet will
    // be finished.  While stopping announcement would kill them.
    LOG(WARNING) << "Changing node status to SHUTTING_DOWN.";
    setNodeState(NodeState::kShuttingDown);
  }
}

void PrestoServer::maybeAttachWorker() {
  auto readLockedShuttingDown = shuttingDown_.rlock();
  if (!*readLockedShuttingDown && nodeState() == NodeState::kShuttingDown) {
    LOG(WARNING) << "Changing node status to ACTIVE.";
    setNodeState(NodeState::kActive);
  }
}

void PrestoServer::setNodeState(NodeState nodeState) {
  nodeState_ = nodeState;
  updateAnnouncerDetails();
}

void PrestoServer::enableAnnouncer(bool enable) {
  if (announcer_ != nullptr) {
    announcer_->enableRequest(enable);
  }
}

void PrestoServer::initializeCoordinatorDiscoverer() {
  // Do not create CoordinatorDiscoverer if we don't have discovery uri.
  if (SystemConfig::instance()->discoveryUri().has_value()) {
    coordinatorDiscoverer_ = std::make_shared<CoordinatorDiscoverer>();
  }
}

void PrestoServer::updateAnnouncerDetails() {
  if (announcer_ != nullptr) {
    announcer_->setDetails(
        fmt::format("State: {}.", nodeState2String(nodeState_)));
  }
}

void PrestoServer::addServerPeriodicTasks() {
  periodicTaskManager_->addTask(
      [server = this]() { server->populateMemAndCPUInfo(); },
      1'000'000, // 1 second
      "populate_mem_cpu_info");

  const auto timeslice = SystemConfig::instance()->taskRunTimeSliceMicros();
  if (timeslice > 0) {
    periodicTaskManager_->addTask(
        [server = this]() { server->yieldTasks(); }, timeslice, "yield_tasks");
  }

  if (isCacheTtlEnabled()) {
    const int64_t ttlThreshold =
        std::chrono::duration_cast<std::chrono::seconds>(
            SystemConfig::instance()->cacheVeloxTtlThreshold())
            .count();
    const int64_t ttlCheckInterval =
        std::chrono::duration_cast<std::chrono::microseconds>(
            SystemConfig::instance()->cacheVeloxTtlCheckInterval())
            .count();
    periodicTaskManager_->addTask(
        [ttlThreshold]() {
          if (auto* cacheTTLController =
                  velox::cache::CacheTTLController::getInstance()) {
            cacheTTLController->applyTTL(ttlThreshold);
          }
        },
        ttlCheckInterval,
        "cache_ttl");
  }

  if (cachePeriodicPersistenceEnabled()) {
    PRESTO_STARTUP_LOG(INFO)
        << "Initializing cache periodic full persistence task...";
    auto* cache = velox::cache::AsyncDataCache::getInstance();
    VELOX_CHECK_NOT_NULL(cache);
    auto* ssdCache = cache->ssdCache();
    VELOX_CHECK_NOT_NULL(ssdCache);
    const auto* systemConfig = SystemConfig::instance();
    const int64_t cacheFullPersistenceIntervalUs =
        std::chrono::duration_cast<std::chrono::microseconds>(
            systemConfig->asyncCachePersistenceInterval())
            .count();
    periodicTaskManager_->addTask(
        [cache, ssdCache]() {
          try {
            if (!ssdCache->startWrite()) {
              return;
            }
            LOG(INFO) << "Flush in-memory cache to SSD...";
            cache->saveToSsd();
            ssdCache->waitForWriteToFinish();
            LOG(INFO) << "Flushing in-memory cache to SSD completed.";
          } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to persistent cache to SSD: " << e.what();
          }
        },
        cacheFullPersistenceIntervalUs,
        "cache_full_persistence");
  }
}

void PrestoServer::createPeriodicMemoryChecker() {
  // The call below will either produce nullptr or unique pointer to an instance
  // of LinuxMemoryChecker.
  memoryChecker_ = createMemoryChecker();
}

std::shared_ptr<velox::exec::TaskListener> PrestoServer::getTaskListener() {
  return nullptr;
}

std::shared_ptr<velox::exec::ExprSetListener>
PrestoServer::getExprSetListener() {
  return nullptr;
}

std::shared_ptr<facebook::velox::exec::SplitListenerFactory>
PrestoServer::getSplitListenerFactory() {
  return nullptr;
}

std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
PrestoServer::getHttpServerFilters() const {
  std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters;
  const auto* systemConfig = SystemConfig::instance();
  if (systemConfig->enableHttpAccessLog()) {
    filters.push_back(
        std::make_unique<http::filters::AccessLogFilterFactory>());
  }

  if (systemConfig->enableHttpStatsFilter()) {
    filters.push_back(std::make_unique<http::filters::StatsFilterFactory>());
  }

  if (systemConfig->enableHttpEndpointLatencyFilter()) {
    filters.push_back(
        std::make_unique<http::filters::HttpEndpointLatencyFilterFactory>(
            httpServer_.get()));
  }

  // Always add the authentication filter to make sure the worker configuration
  // is in line with the overall cluster configuration e.g. cannot have a worker
  // without JWT enabled.
  filters.push_back(
      std::make_unique<http::filters::InternalAuthenticationFilterFactory>());
  return filters;
}

std::vector<std::string> PrestoServer::registerVeloxConnectors(
    const fs::path& configDirectoryPath) {
  static const std::string kPropertiesExtension = ".properties";

  const auto numConnectorCpuThreads = std::max<size_t>(
      SystemConfig::instance()->connectorNumCpuThreadsHwMultiplier() *
          std::thread::hardware_concurrency(),
      0);
  if (numConnectorCpuThreads > 0) {
    connectorCpuExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        numConnectorCpuThreads,
        std::make_shared<folly::NamedThreadFactory>("Connector"));

    PRESTO_STARTUP_LOG(INFO)
        << "Connector CPU executor has " << connectorCpuExecutor_->numThreads()
        << " threads.";
  }

  const auto numConnectorIoThreads = std::max<size_t>(
      SystemConfig::instance()->connectorNumIoThreadsHwMultiplier() *
          std::thread::hardware_concurrency(),
      0);
  if (numConnectorIoThreads > 0) {
    connectorIoExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        numConnectorIoThreads,
        std::make_shared<folly::NamedThreadFactory>("Connector"));

    PRESTO_STARTUP_LOG(INFO)
        << "Connector IO executor has " << connectorIoExecutor_->numThreads()
        << " threads.";
  }

  std::vector<std::string> catalogNames;
  for (const auto& entry :
       fs::directory_iterator(configDirectoryPath / "catalog")) {
    if (entry.path().extension() == kPropertiesExtension) {
      auto fileName = entry.path().filename().string();
      auto catalogName =
          fileName.substr(0, fileName.size() - kPropertiesExtension.size());

      auto connectorConf = util::readConfig(entry.path());
      PRESTO_STARTUP_LOG(INFO)
          << "Registered catalog property keys from " << entry.path() << ":\n"
          << logConnectorConfigPropertyKeys(connectorConf);

      std::shared_ptr<const velox::config::ConfigBase> properties =
          std::make_shared<const velox::config::ConfigBase>(
              std::move(connectorConf));

      auto connectorName = util::requiredProperty(*properties, kConnectorName);

      catalogNames.emplace_back(catalogName);

      PRESTO_STARTUP_LOG(INFO) << "Registering catalog " << catalogName
                               << " using connector " << connectorName;

      // make sure connector type is supported
      getPrestoToVeloxConnector(connectorName);

      std::shared_ptr<velox::connector::Connector> connector =
          velox::connector::getConnectorFactory(connectorName)
              ->newConnector(
                  catalogName,
                  std::move(properties),
                  connectorIoExecutor_.get(),
                  connectorCpuExecutor_.get());
      velox::connector::registerConnector(connector);
    }
  }
  return catalogNames;
}

void PrestoServer::registerSystemConnector() {
  PRESTO_STARTUP_LOG(INFO) << "Registering system catalog "
                           << " using connector SystemConnector";
  VELOX_CHECK(taskManager_);
  auto systemConnector =
      std::make_shared<SystemConnector>("$system@system", taskManager_.get());
  velox::connector::registerConnector(systemConnector);
}

void PrestoServer::unregisterConnectors() {
  PRESTO_SHUTDOWN_LOG(INFO) << "Unregistering connectors";
  auto connectors = velox::connector::getAllConnectors();
  if (connectors.empty()) {
    PRESTO_SHUTDOWN_LOG(INFO) << "No connectors to unregister";
    return;
  }

  PRESTO_SHUTDOWN_LOG(INFO)
      << "Unregistering " << connectors.size() << " connectors";
  for (const auto& connectorEntry : connectors) {
    if (velox::connector::unregisterConnector(connectorEntry.first)) {
      PRESTO_SHUTDOWN_LOG(INFO)
          << "Unregistered connector: " << connectorEntry.first;
    } else {
      PRESTO_SHUTDOWN_LOG(INFO)
          << "Unable to unregister connector: " << connectorEntry.first;
    }
  }

  velox::connector::unregisterConnector("$system@system");
  PRESTO_SHUTDOWN_LOG(INFO)
      << "Unregistered " << connectors.size() << " connectors";
}

void PrestoServer::registerShuffleInterfaceFactories() {
  operators::ShuffleInterfaceFactory::registerFactory(
      operators::LocalPersistentShuffleFactory::kShuffleName.toString(),
      std::make_unique<operators::LocalPersistentShuffleFactory>());
}

void PrestoServer::registerCustomOperators() {
  velox::exec::Operator::registerOperator(
      std::make_unique<operators::PartitionAndSerializeTranslator>());
  velox::exec::Operator::registerOperator(
      std::make_unique<operators::ShuffleWriteTranslator>());
  velox::exec::Operator::registerOperator(
      std::make_unique<operators::ShuffleReadTranslator>());

  // Todo - Split Presto & Presto-on-Spark server into different classes
  // which will allow server specific operator registration.
  velox::exec::Operator::registerOperator(
      std::make_unique<operators::BroadcastWriteTranslator>());
}

void PrestoServer::registerFunctions() {
  velox::functions::prestosql::registerAllScalarFunctions(
      prestoBuiltinFunctionPrefix_);
  velox::aggregate::prestosql::registerAllAggregateFunctions(
      prestoBuiltinFunctionPrefix_);
  velox::window::prestosql::registerAllWindowFunctions(
      prestoBuiltinFunctionPrefix_);
}

void PrestoServer::registerRemoteFunctions() {
#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
  auto* systemConfig = SystemConfig::instance();
  if (auto dirPath =
          systemConfig->remoteFunctionServerSignatureFilesDirectoryPath()) {
    PRESTO_STARTUP_LOG(INFO)
        << "Registering remote functions from path: " << *dirPath;
    if (auto remoteLocation = systemConfig->remoteFunctionServerLocation()) {
      const auto catalogName = systemConfig->remoteFunctionServerCatalogName();
      const auto serdeName = systemConfig->remoteFunctionServerSerde();
      size_t registeredCount = presto::registerRemoteFunctions(
          *dirPath, *remoteLocation, catalogName, serdeName);

      PRESTO_STARTUP_LOG(INFO)
          << registeredCount << " remote functions registered in the '"
          << catalogName << "' catalog.";
    } else {
      VELOX_FAIL(
          "To register remote functions using a json file path you need to "
          "specify the remote server location using '{}', '{}' or '{}'.",
          SystemConfig::kRemoteFunctionServerThriftAddress,
          SystemConfig::kRemoteFunctionServerThriftPort,
          SystemConfig::kRemoteFunctionServerThriftUdsPath);
    }
  }
#endif
}

void PrestoServer::registerVectorSerdes() {
  if (!velox::isRegisteredVectorSerde()) {
    velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!velox::isRegisteredNamedVectorSerde(velox::VectorSerde::Kind::kPresto)) {
    velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!velox::isRegisteredNamedVectorSerde(
          velox::VectorSerde::Kind::kCompactRow)) {
    velox::serializer::CompactRowVectorSerde::registerNamedVectorSerde();
  }
  if (!velox::isRegisteredNamedVectorSerde(
          velox::VectorSerde::Kind::kUnsafeRow)) {
    velox::serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
  }
}

void PrestoServer::registerFileSinks() {
  velox::dwio::common::registerFileSinks();
}

void PrestoServer::registerFileSystems() {
  velox::filesystems::registerLocalFileSystem();
  velox::filesystems::registerS3FileSystem();
  velox::filesystems::registerHdfsFileSystem();
  velox::filesystems::registerGcsFileSystem();
  velox::filesystems::registerAbfsFileSystem();
}

void PrestoServer::unregisterFileSystems() {
  velox::filesystems::finalizeS3FileSystem();
}

void PrestoServer::registerMemoryArbitrators() {
  velox::memory::SharedArbitrator::registerFactory();
}

void PrestoServer::registerFileReadersAndWriters() {
  velox::dwrf::registerDwrfReaderFactory();
  velox::dwrf::registerDwrfWriterFactory();
  velox::orc::registerOrcReaderFactory();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
  if (SystemConfig::instance()->textWriterEnabled()) {
    velox::text::registerTextWriterFactory();
  }
}

void PrestoServer::unregisterFileReadersAndWriters() {
  velox::dwrf::unregisterDwrfReaderFactory();
  velox::dwrf::unregisterDwrfWriterFactory();
  velox::parquet::unregisterParquetReaderFactory();
  velox::parquet::unregisterParquetWriterFactory();
  if (SystemConfig::instance()->textWriterEnabled()) {
    velox::text::unregisterTextWriterFactory();
  }
}

void PrestoServer::registerStatsCounters() {
  registerPrestoMetrics();
  velox::registerVeloxMetrics();
  velox::filesystems::registerS3Metrics();
}

std::string PrestoServer::getLocalIp() const {
  using boost::asio::ip::tcp;
  boost::asio::io_service io_service;
  tcp::resolver resolver(io_service);
  tcp::resolver::query query(boost::asio::ip::host_name(), kHttp);
  tcp::resolver::iterator it = resolver.resolve(query);
  while (it != tcp::resolver::iterator()) {
    boost::asio::ip::address addr = (it++)->endpoint().address();
    // simple check to see if the address is not ::
    if (addr.to_string().length() > 4) {
      return fmt::format("{}", addr.to_string());
    }
  }
  VELOX_FAIL(
      "Could not infer Node IP. Please specify node.internal-address in the node.properties file.");
}

std::string PrestoServer::getBaseSpillDirectory() const {
  return SystemConfig::instance()->spillerSpillPath().value_or("");
}

void PrestoServer::enableWorkerStatsReporting() {
  // This flag must be set to register the counters.
  facebook::velox::BaseStatsReporter::registered = true;
  registerStatsCounters();
}

void PrestoServer::initVeloxPlanValidator() {
  VELOX_CHECK_NULL(planValidator_);
  planValidator_ = std::make_unique<VeloxPlanValidator>();
}

VeloxPlanValidator* PrestoServer::getVeloxPlanValidator() {
  return planValidator_.get();
}

void PrestoServer::populateMemAndCPUInfo() {
  auto systemConfig = SystemConfig::instance();
  const int64_t nodeMemoryGb = systemConfig->systemMemoryGb();
  protocol::MemoryInfo memoryInfo{
      {double(nodeMemoryGb), protocol::DataUnit::GIGABYTE}};

  // Fill the only memory pool info (general)
  auto& poolInfo = memoryInfo.pools["general"];

  // Fill global pool fields.
  poolInfo.maxBytes = nodeMemoryGb * 1024 * 1024 * 1024;
  poolInfo.reservedRevocableBytes = 0;

  // Fill basic per-query fields.
  const auto* queryCtxMgr = taskManager_->getQueryContextManager();
  size_t numContexts{0};
  queryCtxMgr->visitAllContexts([&](const protocol::QueryId& queryId,
                                    const velox::core::QueryCtx* queryCtx) {
    const protocol::Long bytes = queryCtx->pool()->usedBytes();
    poolInfo.queryMemoryReservations.insert({queryId, bytes});
    // TODO(spershin): Might want to see what Java exports and export similar
    // info (like child memory pools).
    poolInfo.queryMemoryAllocations.insert(
        {queryId, {protocol::MemoryAllocation{"total", bytes}}});
    ++numContexts;
    poolInfo.reservedBytes += bytes;
  });
  RECORD_METRIC_VALUE(kCounterNumQueryContexts, numContexts);
  RECORD_METRIC_VALUE(
      kCounterMemoryManagerTotalBytes,
      velox::memory::memoryManager()->getTotalBytes());
  cpuMon_.update();
  checkOverload();
  **memoryInfo_.wlock() = std::move(memoryInfo);
}

void PrestoServer::checkOverload() {
  auto systemConfig = SystemConfig::instance();

  const auto overloadedThresholdMemBytes =
      systemConfig->workerOverloadedThresholdMemGb() * 1024 * 1024 * 1024;
  if (overloadedThresholdMemBytes > 0) {
    const auto currentUsedMemoryBytes = (memoryChecker_ != nullptr)
        ? memoryChecker_->cachedSystemUsedMemoryBytes()
        : 0;
    const bool memOverloaded =
        (currentUsedMemoryBytes > overloadedThresholdMemBytes);
    if (memOverloaded && !memOverloaded_) {
      LOG(WARNING) << "OVERLOAD: Server memory is overloaded. Currently used: "
                   << velox::succinctBytes(currentUsedMemoryBytes)
                   << ", including tracked: "
                   << velox::succinctBytes(
                          velox::memory::memoryManager()->getTotalBytes())
                   << ", threshold: "
                   << velox::succinctBytes(overloadedThresholdMemBytes);
    } else if (!memOverloaded && memOverloaded_) {
      LOG(INFO)
          << "OVERLOAD: Server memory is no longer overloaded. Currently used: "
          << velox::succinctBytes(currentUsedMemoryBytes)
          << ", including tracked: "
          << velox::succinctBytes(
                 velox::memory::memoryManager()->getTotalBytes())
          << ", threshold: "
          << velox::succinctBytes(overloadedThresholdMemBytes);
    }
    RECORD_METRIC_VALUE(kCounterOverloadedMem, memOverloaded ? 100 : 0);
    memOverloaded_ = memOverloaded;
  }

  static const auto hwConcurrency = std::thread::hardware_concurrency();
  const auto overloadedThresholdCpuPct =
      systemConfig->workerOverloadedThresholdCpuPct();
  const auto overloadedThresholdQueuedDrivers = hwConcurrency *
      systemConfig->workerOverloadedThresholdNumQueuedDriversHwMultiplier();
  if (overloadedThresholdCpuPct > 0 && overloadedThresholdQueuedDrivers > 0) {
    const auto currentUsedCpuPct = cpuMon_.getCPULoadPct();
    const auto currentQueuedDrivers = taskManager_->numQueuedDrivers();
    const bool cpuOverloaded =
        (currentUsedCpuPct > overloadedThresholdCpuPct) &&
        (currentQueuedDrivers > overloadedThresholdQueuedDrivers);
    if (cpuOverloaded && !cpuOverloaded_) {
      LOG(WARNING) << "OVERLOAD: Server CPU is overloaded. Currently used: "
                   << currentUsedCpuPct
                   << "% CPU (threshold: " << overloadedThresholdCpuPct
                   << "%), " << currentQueuedDrivers
                   << " queued drivers (threshold: "
                   << overloadedThresholdQueuedDrivers << ")";
    } else if (!cpuOverloaded && cpuOverloaded_) {
      LOG(INFO)
          << "OVERLOAD: Server CPU is no longer overloaded. Currently used: "
          << currentUsedCpuPct
          << "% CPU (threshold: " << overloadedThresholdCpuPct << "%), "
          << currentQueuedDrivers
          << " queued drivers (threshold: " << overloadedThresholdQueuedDrivers
          << ")";
    }
    RECORD_METRIC_VALUE(kCounterOverloadedCpu, cpuOverloaded ? 100 : 0);
    cpuOverloaded_ = cpuOverloaded;
  }

  // Determine if the server is overloaded. We require both memory and CPU to be
  // not overloaded for some time (continuous period) to consider the server as
  // not overloaded.
  const uint64_t currentTimeSecs = velox::getCurrentTimeSec();
  if (memOverloaded_ || cpuOverloaded_) {
    lastOverloadedTimeInSecs_ = currentTimeSecs;
  }
  VELOX_CHECK_GE(currentTimeSecs, lastOverloadedTimeInSecs_);
  const bool serverOverloaded =
      ((cpuOverloaded_ || memOverloaded_) ||
       ((currentTimeSecs - lastOverloadedTimeInSecs_) <
        systemConfig->workerOverloadedCooldownPeriodSec()));

  if (serverOverloaded && !serverOverloaded_) {
    LOG(WARNING) << "OVERLOAD: Server is overloaded.";
  } else if (!serverOverloaded && serverOverloaded_) {
    LOG(INFO) << "OVERLOAD: Server is no longer overloaded.";
  }
  RECORD_METRIC_VALUE(kCounterOverloaded, serverOverloaded ? 100 : 0);
  serverOverloaded_ = serverOverloaded;

  if (systemConfig->workerOverloadedTaskQueuingEnabled()) {
    taskManager_->setServerOverloaded(serverOverloaded_);
  }
  taskManager_->maybeStartNextQueuedTask();
}

static protocol::Duration getUptime(
    std::chrono::steady_clock::time_point& start) {
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();
  if (seconds >= 86400) {
    return protocol::Duration(seconds / 86400.0, protocol::TimeUnit::DAYS);
  }
  if (seconds >= 3600) {
    return protocol::Duration(seconds / 3600.0, protocol::TimeUnit::HOURS);
  }
  if (seconds >= 60) {
    return protocol::Duration(seconds / 60.0, protocol::TimeUnit::MINUTES);
  }

  return protocol::Duration(seconds, protocol::TimeUnit::SECONDS);
}

void PrestoServer::reportMemoryInfo(proxygen::ResponseHandler* downstream) {
  http::sendOkResponse(downstream, json(**memoryInfo_.rlock()));
}

void PrestoServer::reportServerInfo(proxygen::ResponseHandler* downstream) {
  const protocol::ServerInfo serverInfo{
      {nodeVersion_},
      environment_,
      false,
      false,
      std::make_shared<protocol::Duration>(getUptime(start_))};
  http::sendOkResponse(downstream, json(serverInfo));
}

void PrestoServer::reportNodeStatus(proxygen::ResponseHandler* downstream) {
  http::sendOkResponse(downstream, json(fetchNodeStatus()));
}

void PrestoServer::handleGracefulShutdown(
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream) {
  std::string bodyContent =
      folly::trimWhitespace(body[0]->moveToFbString()).toString();
  if (body.size() == 1 && bodyContent == http::kShuttingDown) {
    LOG(INFO) << "Shutdown requested";
    if (nodeState() == NodeState::kActive) {
      // Run stop() in a separate thread to avoid blocking the main HTTP handler
      // and ensure a timely 200 OK response to the client.
      std::thread([this]() { this->stop(); }).detach();
    } else {
      LOG(INFO) << "Node is inactive or shutdown is already requested";
    }
    http::sendOkResponse(downstream);
  } else {
    LOG(ERROR) << "Bad Request. Received body content: " << bodyContent;
    http::sendErrorResponse(downstream, "Bad Request", http::kHttpBadRequest);
  }
}

void PrestoServer::registerSidecarEndpoints() {
  VELOX_CHECK(httpServer_);
  httpServer_->registerGet(
      "/v1/properties/session",
      [this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        const auto* sessionProperties = SessionProperties::instance();
        http::sendOkResponse(downstream, sessionProperties->serialize());
      });
  httpServer_->registerGet(
      "/v1/functions",
      [](proxygen::HTTPMessage* /*message*/,
         const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
         proxygen::ResponseHandler* downstream) {
        http::sendOkResponse(downstream, getFunctionsMetadata());
      });
  httpServer_->registerPost(
      "/v1/velox/plan",
      [server = this](
          proxygen::HTTPMessage* message,
          const std::vector<std::unique_ptr<folly::IOBuf>>& body,
          proxygen::ResponseHandler* downstream) {
        std::string planFragmentJson = util::extractMessageBody(body);
        protocol::PlanConversionResponse response = prestoToVeloxPlanConversion(
            planFragmentJson,
            server->nativeWorkerPool_.get(),
            server->getVeloxPlanValidator());
        if (response.failures.empty()) {
          http::sendOkResponse(downstream, json(response));
        } else {
          http::sendResponse(
              downstream, json(response), http::kHttpUnprocessableContent);
        }
      });
}

protocol::NodeStatus PrestoServer::fetchNodeStatus() {
  auto systemConfig = SystemConfig::instance();
  const int64_t nodeMemoryGb = systemConfig->systemMemoryGb();

  const double cpuLoadPct{cpuMon_.getCPULoadPct()};

  // TODO(spershin): As 'nonHeapUsed' we could export the cache memory.
  const int64_t nonHeapUsed{0};

  protocol::NodeStatus nodeStatus{
      nodeId_,
      {nodeVersion_},
      environment_,
      false,
      getUptime(start_),
      address_,
      address_,
      **memoryInfo_.rlock(),
      (int)std::thread::hardware_concurrency(),
      cpuLoadPct,
      cpuLoadPct,
      pool_ ? pool_->usedBytes() : 0,
      nodeMemoryGb * 1024 * 1024 * 1024,
      nonHeapUsed};

  return nodeStatus;
}

void PrestoServer::registerDynamicFunctions() {
  // For using the non-throwing overloads of functions below.
  std::error_code ec;
  const auto systemConfig = SystemConfig::instance();
  fs::path pluginDir = std::filesystem::current_path().append("plugin");
  if (!systemConfig->pluginDir().empty()) {
    pluginDir = systemConfig->pluginDir();
  }
  // If it is a valid directory, traverse and call dynamic function loader.
  if (fs::is_directory(pluginDir, ec)) {
    PRESTO_STARTUP_LOG(INFO)
        << "Loading dynamic libraries from directory path: " << pluginDir;
    for (const auto& dirEntry :
         std::filesystem::directory_iterator(pluginDir)) {
      if (isSharedLibrary(dirEntry.path())) {
        PRESTO_STARTUP_LOG(INFO)
            << "Loading dynamic libraries from: " << dirEntry.path().string();
        velox::loadDynamicLibrary(dirEntry.path().c_str());
      }
    }
  } else {
    PRESTO_STARTUP_LOG(INFO)
        << "Plugin directory path: " << pluginDir << " is invalid.";
    return;
  }
}

void PrestoServer::createTaskManager() {
  taskManager_ = std::make_unique<TaskManager>(
      driverExecutor_.get(), httpSrvCpuExecutor_.get(), spillerExecutor_.get());
}

void PrestoServer::reportNodeStats(proxygen::ResponseHandler* downstream) {
  protocol::NodeStats nodeStats;

  auto loadMetrics = std::make_shared<protocol::NodeLoadMetrics>();
  loadMetrics->cpuOverload = cpuOverloaded_;
  loadMetrics->memoryOverload = memOverloaded_;

  nodeStats.loadMetrics = loadMetrics;
  nodeStats.nodeState = convertNodeState(this->nodeState());

  http::sendOkResponse(downstream, json(nodeStats));
}
} // namespace facebook::presto
