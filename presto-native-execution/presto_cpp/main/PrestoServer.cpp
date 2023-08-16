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
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include "CoordinatorDiscoverer.h"
#include "presto_cpp/main/Announcer.h"
#include "presto_cpp/main/PeriodicTaskManager.h"
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/SignalHandler.h"
#include "presto_cpp/main/TaskResource.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/http/filters/AccessLogFilter.h"
#include "presto_cpp/main/http/filters/StatsFilter.h"
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/UnsafeRowExchangeSource.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/Connectors.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/Config.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/serializers/PrestoSerializer.h"

#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
#include "presto_cpp/main/RemoteFunctionRegisterer.h"
#endif

namespace facebook::presto {
using namespace facebook::velox;

namespace {

constexpr char const* kHttp = "http";
constexpr char const* kHttps = "https";
constexpr char const* kTaskUriFormat =
    "{}://{}:{}"; // protocol, address and port
constexpr char const* kConnectorName = "connector.name";

protocol::NodeState convertNodeState(presto::NodeState nodeState) {
  switch (nodeState) {
    case presto::NodeState::ACTIVE:
      return protocol::NodeState::ACTIVE;
    case presto::NodeState::INACTIVE:
      return protocol::NodeState::INACTIVE;
    case presto::NodeState::SHUTTING_DOWN:
      return protocol::NodeState::SHUTTING_DOWN;
  }
  return protocol::NodeState::ACTIVE; // For gcc build.
}

void enableChecksum() {
  velox::exec::PartitionedOutputBufferManager::getInstance()
      .lock()
      ->setListenerFactory([]() {
        return std::make_unique<
            serializer::presto::PrestoOutputStreamListener>();
      });
}

} // namespace

PrestoServer::PrestoServer(const std::string& configDirectoryPath)
    : configDirectoryPath_(configDirectoryPath),
      signalHandler_(std::make_unique<SignalHandler>(this)),
      start_(std::chrono::steady_clock::now()),
      memoryInfo_(std::make_unique<protocol::MemoryInfo>()) {}

PrestoServer::~PrestoServer() {}

void PrestoServer::run() {
  auto systemConfig = SystemConfig::instance();
  auto nodeConfig = NodeConfig::instance();
  auto baseVeloxQueryConfig = BaseVeloxQueryConfig::instance();
  int httpPort{0};
  int httpExecThreads{0};

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
    baseVeloxQueryConfig->initialize(
        fmt::format("{}/velox.properties", configDirectoryPath_));

    httpPort = systemConfig->httpServerHttpPort();
    if (systemConfig->httpServerHttpsEnabled()) {
      httpsPort = systemConfig->httpServerHttpsPort();

      ciphers = systemConfig->httpsSupportedCiphers();
      if (ciphers.empty()) {
        VELOX_USER_FAIL("Https is enabled without ciphers")
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
      clientCertAndKeyPath = optionalClientCertPath.value();
    }

    nodeVersion_ = systemConfig->prestoVersion();
    httpExecThreads = systemConfig->httpExecThreads();
    environment_ = nodeConfig->nodeEnvironment();
    nodeId_ = nodeConfig->nodeId();
    address_ = nodeConfig->nodeIp(std::bind(&PrestoServer::getLocalIp, this));
    // Add [] to an ipv6 address.
    if (address_.find(':') != std::string::npos && address_.front() != '[') {
      address_ = fmt::format("[{}]", address_);
    }
    nodeLocation_ = nodeConfig->nodeLocation();
  } catch (const VeloxUserError& e) {
    // VeloxUserError is always logged as an error.
    // Avoid logging again.
    exit(EXIT_FAILURE);
  }

  registerStatsCounters();
  registerFileSinks();
  registerFileSystems();
  registerMemoryArbitrators();
  registerShuffleInterfaceFactories();
  registerCustomOperators();
  protocol::registerHiveConnectors();
  protocol::registerTpchConnector();

  initializeVeloxMemory();

  auto catalogNames = registerConnectors(fs::path(configDirectoryPath_));

  folly::SocketAddress httpSocketAddress;
  httpSocketAddress.setFromLocalPort(httpPort);
  PRESTO_STARTUP_LOG(INFO) << fmt::format(
      "Starting server at {}:{} ({})",
      httpSocketAddress.getIPAddress().str(),
      httpPort,
      address_);

  initializeCoordinatorDiscoverer();
  if (coordinatorDiscoverer_ != nullptr) {
    announcer_ = std::make_unique<Announcer>(
        address_,
        httpsPort.has_value(),
        httpsPort.has_value() ? httpsPort.value() : httpPort,
        coordinatorDiscoverer_,
        nodeVersion_,
        environment_,
        nodeId_,
        nodeLocation_,
        catalogNames,
        systemConfig->announcementMinFrequencyMs(),
        systemConfig->announcementMaxFrequencyMs(),
        clientCertAndKeyPath,
        ciphers);
    announcer_->start();
  }

  const bool reusePort = SystemConfig::instance()->httpServerReusePort();
  auto httpConfig =
      std::make_unique<http::HttpConfig>(httpSocketAddress, reusePort);

  std::unique_ptr<http::HttpsConfig> httpsConfig;
  if (httpsPort.has_value()) {
    folly::SocketAddress httpsSocketAddress;
    httpsSocketAddress.setFromLocalPort(httpsPort.value());

    httpsConfig = std::make_unique<http::HttpsConfig>(
        httpsSocketAddress, certPath, keyPath, ciphers, reusePort);
  }

  httpServer_ = std::make_unique<http::HttpServer>(
      std::move(httpConfig), std::move(httpsConfig), httpExecThreads);

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

  registerFunctions();
  registerRemoteFunctions();
  registerVectorSerdes();
  registerPrestoPlanNodeSerDe();

  exchangeExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(
      systemConfig->numIoThreads(),
      std::make_shared<folly::NamedThreadFactory>("PrestoWorkerNetwork"));

  PRESTO_STARTUP_LOG(INFO) << "Exchange executor has "
                           << exchangeExecutor_->numThreads() << " threads.";

  facebook::velox::exec::ExchangeSource::registerFactory(
      [this](
          const std::string& taskId,
          int destination,
          std::shared_ptr<velox::exec::ExchangeQueue> queue,
          memory::MemoryPool* pool) {
        return PrestoExchangeSource::create(
            taskId, destination, queue, pool, exchangeExecutor_);
      });

  facebook::velox::exec::ExchangeSource::registerFactory(
      operators::UnsafeRowExchangeSource::createExchangeSource);

  // Batch broadcast exchange source.
  facebook::velox::exec::ExchangeSource::registerFactory(
      operators::BroadcastExchangeSource::createExchangeSource);

  pool_ = velox::memory::addDefaultLeafMemoryPool();
  taskManager_ = std::make_unique<TaskManager>();

  std::string taskUri;
  if (httpsPort.has_value()) {
    taskUri = fmt::format(kTaskUriFormat, kHttps, address_, httpsPort.value());
  } else {
    taskUri = fmt::format(kTaskUriFormat, kHttp, address_, httpPort);
  }

  taskManager_->setBaseUri(taskUri);
  taskManager_->setNodeId(nodeId_);

  auto baseSpillDirectory = getBaseSpillDirectory();
  if (!baseSpillDirectory.empty()) {
    taskManager_->setBaseSpillDirectory(baseSpillDirectory);
    PRESTO_STARTUP_LOG(INFO)
        << "Spilling root directory: " << baseSpillDirectory;
  }

  taskResource_ = std::make_unique<TaskResource>(*taskManager_, pool_.get());
  taskResource_->registerUris(*httpServer_);
  if (systemConfig->enableSerializedPageChecksum()) {
    enableChecksum();
  }

  if (systemConfig->enableVeloxTaskLogging()) {
    if (auto listener = getTaskListener()) {
      exec::registerTaskListener(listener);
    }
  }

  if (systemConfig->enableVeloxExprSetLogging()) {
    if (auto listener = getExprSetListener()) {
      exec::registerExprSetListener(listener);
    }
  }
  prestoServerOperations_ =
      std::make_unique<PrestoServerOperations>(taskManager_.get());

  // The endpoint used by operation in production.
  httpServer_->registerGet(
      "/v1/operation/.*",
      [this](
          proxygen::HTTPMessage* message,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        prestoServerOperations_->runOperation(message, downstream);
      });

  PRESTO_STARTUP_LOG(INFO) << "Driver CPU executor has "
                           << driverCPUExecutor()->numThreads() << " threads.";
  if (httpServer_->getExecutor()) {
    PRESTO_STARTUP_LOG(INFO)
        << "HTTP Server executor has "
        << httpServer_->getExecutor()->numThreads() << " threads.";
  }
  if (spillExecutorPtr()) {
    PRESTO_STARTUP_LOG(INFO) << "Spill executor has "
                             << spillExecutorPtr()->numThreads() << " threads.";
  } else {
    PRESTO_STARTUP_LOG(INFO) << "Spill executor was not configured.";
  }

  PRESTO_STARTUP_LOG(INFO) << "Starting all periodic tasks...";
  std::vector<std::shared_ptr<velox::connector::Connector>> connectors;
  for (auto connectorId : catalogNames) {
    connectors.emplace_back(velox::connector::getConnector(connectorId));
  }

  auto memoryAllocator = velox::memory::MemoryAllocator::getInstance();
  auto asyncDataCache = cache::AsyncDataCache::getInstance();
  periodicTaskManager_ = std::make_unique<PeriodicTaskManager>(
      driverCPUExecutor(),
      httpServer_->getExecutor(),
      taskManager_.get(),
      memoryAllocator,
      asyncDataCache,
      velox::connector::getAllConnectors());
  addServerPeriodicTasks();
  addAdditionalPeriodicTasks();
  periodicTaskManager_->start();

  // Start everything. After the return from the following call we are shutting
  // down.
  httpServer_->start(getHttpServerFilters());

  if (announcer_ != nullptr) {
    PRESTO_SHUTDOWN_LOG(INFO) << "Stopping announcer";
    announcer_->stop();
  }
  PRESTO_SHUTDOWN_LOG(INFO) << "Stopping all periodic tasks...";
  periodicTaskManager_->stop();

  // Destroy entities here to ensure we won't get any messages after Server
  // object is gone and to have nice log in case shutdown gets stuck.
  PRESTO_SHUTDOWN_LOG(INFO) << "Destroying Task Resource...";
  taskResource_.reset();
  PRESTO_SHUTDOWN_LOG(INFO) << "Destroying Task Manager...";
  taskManager_.reset();
  PRESTO_SHUTDOWN_LOG(INFO) << "Destroying HTTP Server...";
  httpServer_.reset();

  unregisterConnectors();

  auto cpuExecutor = driverCPUExecutor();
  PRESTO_SHUTDOWN_LOG(INFO)
      << "Joining driver CPU Executor '" << cpuExecutor->getName()
      << "': threads: " << cpuExecutor->numActiveThreads() << "/"
      << cpuExecutor->numThreads()
      << ", task queue: " << cpuExecutor->getTaskQueueSize();
  cpuExecutor->join();

  if (connectorIoExecutor_) {
    PRESTO_SHUTDOWN_LOG(INFO)
        << "Joining connector IO Executor '" << connectorIoExecutor_->getName()
        << "': threads: " << connectorIoExecutor_->numActiveThreads() << "/"
        << connectorIoExecutor_->numThreads();
    connectorIoExecutor_->join();
  }

  PRESTO_SHUTDOWN_LOG(INFO)
      << "Joining exchange Executor '" << exchangeExecutor_->getName()
      << "': threads: " << exchangeExecutor_->numActiveThreads() << "/"
      << exchangeExecutor_->numThreads();
  exchangeExecutor_->join();

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
  const auto numQueued = driverCPUExecutor()->getTaskQueueSize();
  if (numQueued > 0) {
    numYields += taskManager_->yieldTasks(numQueued, timeslice);
  }
  if (numYields > 100'000) {
    LOG(INFO) << "Yielded " << numYields << " more threads.";
    numYields = 0;
  }
}

void PrestoServer::initializeVeloxMemory() {
  auto nodeConfig = NodeConfig::instance();
  auto systemConfig = SystemConfig::instance();
  uint64_t memoryGb = systemConfig->systemMemoryGb();
  PRESTO_STARTUP_LOG(INFO) << "Starting with node memory " << memoryGb << "GB";

  const int64_t memoryBytes = memoryGb << 30;
  if (systemConfig->useMmapAllocator()) {
    memory::MmapAllocator::Options options;
    options.capacity = memoryBytes;
    options.useMmapArena = systemConfig->useMmapArena();
    options.mmapArenaCapacityRatio = systemConfig->mmapArenaCapacityRatio();
    allocator_ = std::make_shared<memory::MmapAllocator>(options);
  } else {
    allocator_ = memory::MemoryAllocator::createDefaultInstance();
  }
  if (systemConfig->asyncDataCacheEnabled()) {
    std::unique_ptr<cache::SsdCache> ssd;
    const auto asyncCacheSsdGb = systemConfig->asyncCacheSsdGb();
    if (asyncCacheSsdGb > 0) {
      constexpr int32_t kNumSsdShards = 16;
      cacheExecutor_ =
          std::make_unique<folly::IOThreadPoolExecutor>(kNumSsdShards);
      auto asyncCacheSsdCheckpointGb =
          systemConfig->asyncCacheSsdCheckpointGb();
      auto asyncCacheSsdDisableFileCow =
          systemConfig->asyncCacheSsdDisableFileCow();
      PRESTO_STARTUP_LOG(INFO)
          << "Initializing SSD cache with capacity " << asyncCacheSsdGb
          << "GB, checkpoint size " << asyncCacheSsdCheckpointGb
          << "GB, file cow "
          << (asyncCacheSsdDisableFileCow ? "DISABLED" : "ENABLED");
      ssd = std::make_unique<cache::SsdCache>(
          systemConfig->asyncCacheSsdPath(),
          asyncCacheSsdGb << 30,
          kNumSsdShards,
          cacheExecutor_.get(),
          asyncCacheSsdCheckpointGb << 30,
          asyncCacheSsdDisableFileCow);
    }
    std::string cacheStr =
        ssd == nullptr ? "AsyncDataCache" : "AsyncDataCache with SSD";
    cache_ = cache::AsyncDataCache::create(allocator_.get(), std::move(ssd));
    cache::AsyncDataCache::setInstance(cache_.get());
    PRESTO_STARTUP_LOG(INFO) << cacheStr << " has been setup";
  } else {
    VELOX_CHECK_EQ(
        systemConfig->asyncCacheSsdGb(),
        0,
        "Async data cache cannot be disabled if ssd cache is enabled");
  }

  memory::MemoryAllocator::setDefaultInstance(allocator_.get());
  // Set up velox memory manager.
  memory::MemoryManagerOptions options;
  options.capacity = memoryBytes;
  options.checkUsageLeak = systemConfig->enableMemoryLeakCheck();
  if (!systemConfig->memoryArbitratorKind().empty()) {
    options.arbitratorKind = systemConfig->memoryArbitratorKind();
    options.capacity =
        memoryBytes * 100 / systemConfig->reservedMemoryPoolCapacityPct();
    options.memoryPoolInitCapacity = systemConfig->memoryPoolInitCapacity();
    options.memoryPoolTransferCapacity =
        systemConfig->memoryPoolTransferCapacity();
  }
  const auto& manager = memory::MemoryManager::getInstance(options);
  PRESTO_STARTUP_LOG(INFO) << "Memory manager has been setup: "
                           << manager.toString();
}

void PrestoServer::stop() {
  // Make sure we only go here once.
  auto shutdownOnsetSec = SystemConfig::instance()->shutdownOnsetSec();
  if (!shuttingDown_.exchange(true)) {
    PRESTO_SHUTDOWN_LOG(INFO) << "Initiating shutdown. Will wait for "
                              << shutdownOnsetSec << " seconds.";
    this->setNodeState(NodeState::SHUTTING_DOWN);

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
}

void PrestoServer::initializeCoordinatorDiscoverer() {
  // Do not create CoordinatorDiscoverer if we don't have discovery uri.
  if (SystemConfig::instance()->discoveryUri().has_value()) {
    coordinatorDiscoverer_ = std::make_shared<CoordinatorDiscoverer>();
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
}

std::shared_ptr<velox::exec::TaskListener> PrestoServer::getTaskListener() {
  return nullptr;
}

std::shared_ptr<velox::exec::ExprSetListener>
PrestoServer::getExprSetListener() {
  return nullptr;
}

std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
PrestoServer::getHttpServerFilters() {
  std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters;

  if (SystemConfig::instance()->enableHttpAccessLog()) {
    filters.push_back(
        std::make_unique<http::filters::AccessLogFilterFactory>());
  }

  if (SystemConfig::instance()->enableHttpStatsFilter()) {
    auto additionalFilters = getAdditionalHttpServerFilters();
    for (auto& filter : additionalFilters) {
      filters.push_back(std::move(filter));
    }
  }

  return filters;
}

std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>>
PrestoServer::getAdditionalHttpServerFilters() {
  std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters;
  filters.emplace_back(std::make_unique<http::filters::StatsFilterFactory>());
  return filters;
}

std::vector<std::string> PrestoServer::registerConnectors(
    const fs::path& configDirectoryPath) {
  static const std::string kPropertiesExtension = ".properties";

  const auto numConnectorIoThreads =
      SystemConfig::instance()->numConnectorIoThreads();
  if (numConnectorIoThreads) {
    connectorIoExecutor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(numConnectorIoThreads);

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
      std::shared_ptr<const velox::Config> properties =
          std::make_shared<const velox::core::MemConfig>(
              std::move(connectorConf));

      auto connectorName = util::requiredProperty(*properties, kConnectorName);

      catalogNames.emplace_back(catalogName);

      PRESTO_STARTUP_LOG(INFO) << "Registering catalog " << catalogName
                               << " using connector " << connectorName;

      std::shared_ptr<velox::connector::Connector> connector =
          facebook::velox::connector::getConnectorFactory(connectorName)
              ->newConnector(
                  catalogName,
                  std::move(properties),
                  connectorIoExecutor_.get());
      velox::connector::registerConnector(connector);
    }
  }
  return catalogNames;
}

void PrestoServer::unregisterConnectors() {
  PRESTO_SHUTDOWN_LOG(INFO) << "Unregistering connectors";
  auto connectors = facebook::velox::connector::getAllConnectors();
  std::unordered_set<std::string> connectorIds;
  connectorIds.reserve(connectors.size());
  for (const auto& connectorEntry : connectors) {
    facebook::velox::connector::unregisterConnector(connectorEntry.first);
  }
  PRESTO_SHUTDOWN_LOG(INFO)
      << "Unregistered connectors: " << folly::join(',', connectorIds);
}

void PrestoServer::registerShuffleInterfaceFactories() {
  operators::ShuffleInterfaceFactory::registerFactory(
      operators::LocalPersistentShuffleFactory::kShuffleName.toString(),
      std::make_unique<operators::LocalPersistentShuffleFactory>());
}

void PrestoServer::registerCustomOperators() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<operators::PartitionAndSerializeTranslator>());
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<facebook::presto::operators::ShuffleWriteTranslator>());
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<operators::ShuffleReadTranslator>());

  // Todo - Split Presto & Presto-on-Spark server into different classes
  // which will allow server specific operator registration.
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<
          facebook::presto::operators::BroadcastWriteTranslator>());
}

void PrestoServer::registerFunctions() {
  static const std::string kPrestoDefaultPrefix{"presto.default."};
  velox::functions::prestosql::registerAllScalarFunctions(kPrestoDefaultPrefix);
  velox::aggregate::prestosql::registerAllAggregateFunctions(
      kPrestoDefaultPrefix);
  velox::window::prestosql::registerAllWindowFunctions(kPrestoDefaultPrefix);
  if (SystemConfig::instance()->registerTestFunctions()) {
    velox::functions::prestosql::registerAllScalarFunctions(
        "json.test_schema.");
    velox::aggregate::prestosql::registerAllAggregateFunctions(
        "json.test_schema.");
  }
}

void PrestoServer::registerRemoteFunctions() {
#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
  auto* systemConfig = SystemConfig::instance();
  if (auto dirPath =
          systemConfig->remoteFunctionServerSignatureFilesDirectoryPath()) {
    PRESTO_STARTUP_LOG(INFO)
        << "Registering remote functions from path: " << *dirPath;
    if (auto remoteLocation = systemConfig->remoteFunctionServerLocation()) {
      auto catalogName = systemConfig->remoteFunctionServerCatalogName();
      size_t registeredCount = presto::registerRemoteFunctions(
          *dirPath, *remoteLocation, catalogName);
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
}

void PrestoServer::registerFileSystems() {
  velox::filesystems::registerLocalFileSystem();
}

void PrestoServer::registerMemoryArbitrators() {
  velox::memory::MemoryArbitrator::registerAllFactories();
}

void PrestoServer::registerStatsCounters() {
  registerPrestoCppCounters();
  registerVeloxCounters();
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
      "Could not infer Node IP. Please specify node.ip in the node.properties file.");
}

std::string PrestoServer::getBaseSpillDirectory() const {
  return SystemConfig::instance()->spillerSpillPath().value_or("");
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
  // TODO(sperhsin): If 'current bytes' is the same as we get by summing up all
  // child contexts below, then use the one we sum up, rather than call
  // 'getCurrentBytes'.
  poolInfo.reservedBytes = pool_->currentBytes();
  poolInfo.reservedRevocableBytes = 0;

  // Fill basic per-query fields.
  const auto* queryCtxMgr = taskManager_->getQueryContextManager();
  size_t numContexts{0};
  queryCtxMgr->visitAllContexts([&](const protocol::QueryId& queryId,
                                    const velox::core::QueryCtx* queryCtx) {
    const protocol::Long bytes = queryCtx->pool()->currentBytes();
    poolInfo.queryMemoryReservations.insert({queryId, bytes});
    // TODO(spershin): Might want to see what Java exports and export similar
    // info (like child memory pools).
    poolInfo.queryMemoryAllocations.insert(
        {queryId, {protocol::MemoryAllocation{"total", bytes}}});
    ++numContexts;
  });
  REPORT_ADD_STAT_VALUE(kCounterNumQueryContexts, numContexts);
  cpuMon_.update();
  **memoryInfo_.wlock() = std::move(memoryInfo);
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
      pool_->currentBytes(),
      nodeMemoryGb * 1024 * 1024 * 1024,
      nonHeapUsed};

  http::sendOkResponse(downstream, json(nodeStatus));
}

} // namespace facebook::presto
