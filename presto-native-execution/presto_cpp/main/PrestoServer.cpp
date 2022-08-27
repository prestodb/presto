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
#include "presto_cpp/main/Announcer.h"
#include "presto_cpp/main/PeriodicTaskManager.h"
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/SignalHandler.h"
#include "presto_cpp/main/TaskResource.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/connectors/hive/storage_adapters/FileSystems.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/presto_protocol/Connectors.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/Connector.h"
#include "velox/core/Context.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/Driver.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/serializers/PrestoSerializer.h"

#ifdef PRESTO_ENABLE_PARQUET
#include "velox/dwio/parquet/RegisterParquetReader.h" // @manual
#endif

namespace facebook::presto {
using namespace facebook::velox;

namespace {

constexpr char const* kHttp = "http";
constexpr char const* kBaseUriFormat = "http://{}:{}";
constexpr char const* kConnectorName = "connector.name";
constexpr char const* kCacheEnabled = "cache.enabled";
constexpr char const* kCacheMaxCacheSize = "cache.max-cache-size";

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

void sendOkResponse(proxygen::ResponseHandler* downstream, const json& body) {
  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "OK")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
      .body(body.dump())
      .sendWithEOM();
}

std::string getLocalIp() {
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
  registerPrestoCppCounters();
  velox::filesystems::registerLocalFileSystem();
  registerOptionalHiveStorageAdapters();
  protocol::registerHiveConnectors();

  auto systemConfig = SystemConfig::instance();

  auto executor = std::make_shared<folly::IOThreadPoolExecutor>(
      systemConfig->numIoThreads(),
      std::make_shared<folly::NamedThreadFactory>("PrestoWorkerNetwork"));
  folly::setUnsafeMutableGlobalIOExecutor(executor);

  auto nodeConfig = NodeConfig::instance();
  int servicePort;
  int httpExecThreads;
  try {
    systemConfig->initialize(
        fmt::format("{}/config.properties", configDirectoryPath_));
    nodeConfig->initialize(
        fmt::format("{}/node.properties", configDirectoryPath_));
    servicePort = systemConfig->httpServerHttpPort();
    nodeVersion_ = systemConfig->prestoVersion();
    httpExecThreads = systemConfig->httpExecThreads();
    environment_ = nodeConfig->nodeEnvironment();
    nodeId_ = nodeConfig->nodeId();
    address_ = nodeConfig->nodeIp(getLocalIp);
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
  initializeAsyncCache();

  auto catalogNames = registerConnectors(fs::path(configDirectoryPath_));

  folly::SocketAddress socketAddress;
  socketAddress.setFromLocalPort(servicePort);
  LOG(INFO) << fmt::format(
      "STARTUP: Starting server at {}:{} ({})",
      socketAddress.getIPAddress().str(),
      servicePort,
      address_);

  Announcer announcer(
      address_,
      servicePort,
      discoveryAddressLookup(),
      nodeVersion_,
      environment_,
      nodeId_,
      nodeLocation_,
      catalogNames,
      30'000 /*milliseconds*/);
  announcer.start();

  httpServer_ =
      std::make_unique<http::HttpServer>(socketAddress, httpExecThreads);

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
        sendOkResponse(downstream, infoStateJson);
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
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpOk, "OK")
            .header(
                proxygen::HTTP_HEADER_CONTENT_TYPE,
                http::kMimeTypeApplicationJson)
            .sendWithEOM();
      });

  velox::functions::prestosql::registerAllScalarFunctions();
  if (!velox::isRegisteredVectorSerde()) {
    velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }

  facebook::velox::exec::ExchangeSource::registerFactory(
      PrestoExchangeSource::createExchangeSource);

  velox::dwrf::registerDwrfReaderFactory();
#ifdef PRESTO_ENABLE_PARQUET
  velox::parquet::registerParquetReaderFactory();
#endif

  taskManager_ = std::make_unique<TaskManager>(
      systemConfig->values(), nodeConfig->values());
  taskManager_->setBaseUri(fmt::format(kBaseUriFormat, address_, servicePort));
  taskResource_ = std::make_unique<TaskResource>(*taskManager_);
  taskResource_->registerUris(*httpServer_);
  if (systemConfig->enableSerializedPageChecksum()) {
    enableChecksum();
  }

  if (systemConfig->enableVeloxTaskLogging()) {
    if (auto listener = getTaskListiner()) {
      exec::registerTaskListener(listener);
    }
  }

  LOG(INFO) << "STARTUP: Starting all periodic tasks...";
  PeriodicTaskManager periodicTaskManager(
      driverCPUExecutor(), httpServer_->getExecutor(), taskManager_.get());
  periodicTaskManager.addTask(
      [server = this]() { server->populateMemAndCPUInfo(); },
      1'000'000, // 1 second
      "populate_mem_cpu_info");
  periodicTaskManager.start();

  // Start everything. After the return from the following call we are shutting
  // down.
  httpServer_->start();

  LOG(INFO) << "SHUTDOWN: Stopping all periodic tasks...";
  periodicTaskManager.stop();

  // Destroy entities here to ensure we won't get any messages after Server
  // object is gone and to have nice log in case shutdown gets stuck.
  LOG(INFO) << "SHUTDOWN: Destroying Task Resource...";
  taskResource_.reset();
  LOG(INFO) << "SHUTDOWN: Destroying Task Manager...";
  taskManager_.reset();
  LOG(INFO) << "SHUTDOWN: Destroying HTTP Server...";
  httpServer_.reset();

  auto cpuExecutor = driverCPUExecutor();
  LOG(INFO) << "SHUTDOWN: Joining Driver CPU Executor '"
            << cpuExecutor->getName()
            << "': threads: " << cpuExecutor->numActiveThreads() << "/"
            << cpuExecutor->numThreads()
            << ", task queue: " << cpuExecutor->getTaskQueueSize();
  cpuExecutor->join();

  LOG(INFO) << "SHUTDOWN: Joining IO Executor '"
            << connectorIoExecutor_->getName()
            << "': threads: " << connectorIoExecutor_->numActiveThreads() << "/"
            << connectorIoExecutor_->numThreads();
  connectorIoExecutor_->join();

  LOG(INFO) << "SHUTDOWN: Done joining our executors.";

  auto globalCPUKeepAliveExec = folly::getGlobalCPUExecutor();
  if (auto* pGlobalCPUExecutor = dynamic_cast<folly::CPUThreadPoolExecutor*>(
          globalCPUKeepAliveExec.get())) {
    LOG(INFO) << "SHUTDOWN: Global CPU Executor '"
              << pGlobalCPUExecutor->getName()
              << "': threads: " << pGlobalCPUExecutor->numActiveThreads() << "/"
              << pGlobalCPUExecutor->numThreads()
              << ", task queue: " << pGlobalCPUExecutor->getTaskQueueSize();
  }

  auto globalIOKeepAliveExec = folly::getGlobalIOExecutor();
  if (auto* pGlobalIOExecutor = dynamic_cast<folly::IOThreadPoolExecutor*>(
          globalIOKeepAliveExec.get())) {
    LOG(INFO) << "SHUTDOWN: Global IO Executor '"
              << pGlobalIOExecutor->getName()
              << "': threads: " << pGlobalIOExecutor->numActiveThreads() << "/"
              << pGlobalIOExecutor->numThreads();
  }
}

void PrestoServer::initializeAsyncCache() {
  auto nodeConfig = NodeConfig::instance();
  auto systemConfig = SystemConfig::instance();
  uint64_t memoryGb = nodeConfig->nodeMemoryGb(
      [&]() { return systemConfig->systemMemoryGb(); });
  LOG(INFO) << "Starting with node memory " << memoryGb << "GB";
  std::unique_ptr<cache::SsdCache> ssd;
  auto asyncCacheSsdGb = systemConfig->asyncCacheSsdGb();
  if (asyncCacheSsdGb) {
    constexpr int32_t kNumSsdShards = 16;
    cacheExecutor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(kNumSsdShards);
    ssd = std::make_unique<cache::SsdCache>(
        systemConfig->asyncCacheSsdPath(),
        asyncCacheSsdGb << 30,
        kNumSsdShards,
        cacheExecutor_.get());
  }
  auto memoryBytes = memoryGb << 30;

  memory::MmapAllocatorOptions options = {memoryBytes};
  auto allocator = std::make_shared<memory::MmapAllocator>(options);
  mappedMemory_ = std::make_shared<cache::AsyncDataCache>(
      allocator, memoryBytes, std::move(ssd));

  memory::MappedMemory::setDefaultInstance(mappedMemory_.get());
}

void PrestoServer::stop() {
  // Make sure we only go here once.
  auto shutdownOnsetSec = SystemConfig::instance()->shutdownOnsetSec();
  if (not shuttingDown_.exchange(true)) {
    LOG(INFO) << "SHUTDOWN: Initiating shutdown. Will wait for "
              << shutdownOnsetSec << " seconds.";
    this->setNodeState(NodeState::SHUTTING_DOWN);

    // Give coordinator some time to receive our new node state and stop sending
    // any tasks.
    std::this_thread::sleep_for(std::chrono::seconds(shutdownOnsetSec));

    size_t numTasks{0};
    auto taskNumbers = taskManager_->getTaskNumbers(numTasks);
    size_t seconds = 0;
    while (taskNumbers[velox::exec::TaskState::kRunning] > 0) {
      LOG(INFO) << "SHUTDOWN: Waiting (" << seconds
                << " seconds so far) for 'Running' tasks to complete. "
                << numTasks << " tasks left: "
                << PrestoTask::taskNumbersToString(taskNumbers);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      taskNumbers = taskManager_->getTaskNumbers(numTasks);
      ++seconds;
    }

    // Give coordinator some time to request tasks stats for completed or failed
    // tasks.
    std::this_thread::sleep_for(std::chrono::seconds(shutdownOnsetSec));

    if (httpServer_) {
      LOG(INFO) << "SHUTDOWN: All tasks are completed. Stopping HTTP Server...";
      httpServer_->stop();
      LOG(INFO) << "SHUTDOWN: HTTP Server stopped.";
    }
  }
}

std::function<folly::SocketAddress()> PrestoServer::discoveryAddressLookup() {
  try {
    auto uri = folly::Uri(SystemConfig::instance()->discoveryUri());

    return [uri]() {
      return folly::SocketAddress(uri.hostname(), uri.port(), true);
    };
  } catch (const VeloxUserError& e) {
    // VeloxUserError is always logged as an error.
    // Avoid logging again.
    exit(EXIT_FAILURE);
  }
}

std::shared_ptr<velox::exec::TaskListener> PrestoServer::getTaskListiner() {
  return nullptr;
}

std::vector<std::string> PrestoServer::registerConnectors(
    const fs::path& configDirectoryPath) {
  static const std::string kPropertiesExtension = ".properties";

  auto numIoThreads = SystemConfig::instance()->numIoThreads();
  if (numIoThreads) {
    connectorIoExecutor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(numIoThreads);
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
      const bool cacheEnabled = properties->get(kCacheEnabled, false);
      const size_t cacheSize = properties->get(kCacheMaxCacheSize, 0);

      catalogNames.emplace_back(catalogName);

      LOG(INFO) << "STARTUP: Registering catalog " << catalogName
                << " using connector " << connectorName
                << ", cache enabled: " << cacheEnabled
                << ", cache size: " << cacheSize;

      std::shared_ptr<velox::connector::Connector> connector;
      if (cacheEnabled && cacheSize > 0) {
        connector = connectorWithCache(connectorName, catalogName, properties);
        cacheRamCapacityGb_ += cacheSize / 1024; // cacheSize is in Mb.
      } else {
        connector =
            facebook::velox::connector::getConnectorFactory(connectorName)
                ->newConnector(catalogName, std::move(properties));
      }
      velox::connector::registerConnector(connector);
    }
  }
  return catalogNames;
}

std::shared_ptr<velox::connector::Connector> PrestoServer::connectorWithCache(
    const std::string& connectorName,
    const std::string& catalogName,
    std::shared_ptr<const velox::Config> properties) {
  VELOX_CHECK_NOT_NULL(
      dynamic_cast<cache::AsyncDataCache*>(mappedMemory_.get()));
  LOG(INFO) << "STARTUP: Using AsyncDataCache";
  return facebook::velox::connector::getConnectorFactory(connectorName)
      ->newConnector(
          connectorName, std::move(properties), connectorIoExecutor_.get());
}

void PrestoServer::populateMemAndCPUInfo() {
  auto systemConfig = SystemConfig::instance();
  const int64_t nodeMemoryGb =
      systemConfig->systemMemoryGb() - cacheRamCapacityGb_;
  protocol::MemoryInfo memoryInfo{
      {double(nodeMemoryGb), protocol::DataUnit::GIGABYTE}};

  // Fill the only memory pool info (general)
  auto& poolInfo = memoryInfo.pools["general"];
  auto* pool = taskResource_->getPool();

  // Fill global pool fields.
  poolInfo.maxBytes = nodeMemoryGb * 1024 * 1024 * 1204;
  // TODO(sperhsin): If 'current bytes' is the same as we get by summing up all
  // child contexts below, then use the one we sum up, rather than call
  // 'getCurrentBytes'.
  poolInfo.reservedBytes = pool->getCurrentBytes();
  poolInfo.reservedRevocableBytes = 0;

  // Fill basic per-query fields.
  const auto* queryCtxMgr = taskManager_->getQueryContextManager();
  size_t numContexts{0};
  queryCtxMgr->visitAllContexts([&](const protocol::QueryId& queryId,
                                    const velox::core::QueryCtx* queryCtx) {
    const protocol::Long bytes = queryCtx->pool()->getCurrentBytes();
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
  sendOkResponse(downstream, json(**memoryInfo_.rlock()));
}

void PrestoServer::reportServerInfo(proxygen::ResponseHandler* downstream) {
  const protocol::ServerInfo serverInfo{
      {nodeVersion_},
      environment_,
      false,
      false,
      std::make_shared<protocol::Duration>(getUptime(start_))};
  sendOkResponse(downstream, json(serverInfo));
}

void PrestoServer::reportNodeStatus(proxygen::ResponseHandler* downstream) {
  auto systemConfig = SystemConfig::instance();
  const int64_t nodeMemoryGb =
      systemConfig->systemMemoryGb() - cacheRamCapacityGb_;

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
      taskResource_->getPool()->getCurrentBytes(),
      nodeMemoryGb * 1024 * 1024 * 1024,
      nonHeapUsed};

  sendOkResponse(downstream, json(nodeStatus));
}

} // namespace facebook::presto
