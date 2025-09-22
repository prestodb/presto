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

#include "presto_cpp/main/ThriftServer.h"

#include <glog/logging.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/services/cpp/ServiceFramework.h"
#include "common/services/cpp/TLSConstants.h"
#include "presto_cpp/main/PrestoThriftServiceHandler.h"
#include "presto_cpp/main/thrift/gen-cpp2/PrestoThrift.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::thrift {

ThriftConfig::ThriftConfig(
    const folly::SocketAddress& address,
    const std::string& certPath,
    const std::string& keyPath,
    const std::string& supportedCiphers,
    bool reusePort)
    : address_(address),
      certPath_(certPath),
      keyPath_(keyPath),
      supportedCiphers_(supportedCiphers),
      reusePort_(reusePort) {
  auto systemConfig = SystemConfig::instance();
  numIoThreads = systemConfig
            ->optionalProperty<int>(std::string_view("presto.thrift.io-threads"))
            .value_or(4);
  numCpuThreads = systemConfig
            ->optionalProperty<int>(std::string_view("presto.thrift.cpu-threads"))
            .value_or(8);
  taskExpireTimeMs =
        systemConfig
            ->optionalProperty<int>(
                std::string_view("thrift-task-expire-time-ms"))
            .value_or(30000);
  streamExpireTimeMs =
        systemConfig
            ->optionalProperty<int>(
                std::string_view("thrift-stream-expire-time"))
            .value_or(30000);
  maxRequest =
        systemConfig
            ->optionalProperty<int>(std::string_view("thrift-max-request"))
            .value_or(10000);
  idleTimeout =
        systemConfig
            ->optionalProperty<int>(std::string_view("thrift-idle-timeout"))
            .value_or(60000);
  maxConnections =
        systemConfig
            ->optionalProperty<int>(std::string_view("thrift-max-connections"))
            .value_or(10000);
  listenBacklog =
        systemConfig
            ->optionalProperty<int>(std::string_view("thrift-max-listen-backlog"))
            .value_or(1024);
  sslVerification =
        systemConfig
            ->optionalProperty<int>(std::string_view("thrift-ssl-verification"))
            .value_or(0);
}

ThriftServer::ThriftServer(
    std::unique_ptr<ThriftConfig> config,
    facebook::velox::memory::MemoryPool* pool,
    VeloxPlanValidator* planValidator,
    TaskManager* taskManager)
    : config_(std::move(config)),
      pool_(pool),
      planValidator_(planValidator),
      taskManager_(taskManager),
      started_(false) {
  VELOX_CHECK_NOT_NULL(config_);
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK_NOT_NULL(planValidator_);
  VELOX_CHECK_NOT_NULL(taskManager_);
}

ThriftServer::~ThriftServer() {
  if (started_) {
    stop();
  }
}

void ThriftServer::start() {
  if (started_) {
    return;
  }

  try {
    // Create the Thrift server
    server_ = std::make_unique<apache::thrift::ThriftServer>();

    // Create the service handler
    auto handler = std::make_shared<PrestoThriftServiceHandler>(
        pool_, planValidator_, taskManager_);

    // Configure the server - let it manage its own threads completely
    server_->setInterface(handler);
    server_->setAddress(config_->address);
    server_->setNumIOWorkerThreads(config_->numIoThreads);
    server_->setNumCPUWorkerThreads(config_->numCpuThreads);
    server_->setReusePort(config_->reusePort);
    server_->setMaxConnections(config_->maxConnections);
    server_->setIdleTimeout(std::chrono::milliseconds(config_->idleTimeout));
    server_->setListenBacklog(config_->listenBacklog);
    server_->setTaskExpireTime(std::chrono::milliseconds(
        config_->taskExpireTimeMs));
    server_->setStreamExpireTime(
        std::chrono::milliseconds(config_->streamExpireTimeMs));
    // Use the stream expire time for the queue timeout for consistency/simplicity
    server_->setQueueTimeout(
        std::chrono::milliseconds(config_->streamExpireTimeMs));
    server_->setMaxRequests(config_->maxRequest);

    // configure SSL if sslVerification is >1
    std::string ssl_status = "disabled";
    if (config_->sslVerification >= 1) {
      auto sslContextConfig = services::TLSConfig::createDefaultConfig();
      sslContextConfig->clientVerification = folly::SSLContext::VerifyClientCertificate::IF_PRESENTED;

      std::list<std::string> protocols = {"thrift", "h2", "http/1.1", "http"};
      if (config_->sslVerification == 3) {
        protocols = {"thrift"};
      }
      sslContextConfig->setNextProtocols(protocols);
      server_->setSSLConfig(std::move(sslContextConfig));
      
      ssl_status = "enabled (Level " + std::to_string(config_->sslVerification) + 
                   ", Protocols: " + folly::join(", ", protocols) + ")";
    }

    PRESTO_STARTUP_LOG(INFO) << "Starting Thrift server on "
              << config_->address.getAddressStr() << ":"
              << config_->address.getPort();

    PRESTO_STARTUP_LOG(INFO) << "=== THRIFT SERVER CONFIGURATION SUMMARY ==="
              << "\n  Address: " << config_->address.getAddressStr() << ":" << config_->address.getPort()
              << "\n  I/O Threads: " << config_->numIoThreads
              << "\n  CPU Threads: " << config_->numCpuThreads
              << "\n  Reuse Port: " << (config_->reusePort ? "Yes" : "No")
              << "\n  Max Conns: " << config_->maxConnections
              << "\n  Max Requests: " << config_->maxRequest
              << "\n  Idle Timeout: " << config_->idleTimeout << "ms"
              << "\n  Task/Stream Timeout: " << config_->taskExpireTimeMs << "ms"
              << "\n  SSL Status: " << ssl_status;

    // Use the blocking serve method directly - this avoids the PrimaryPtr issue
    // by not calling setup() separately
    started_ = true;
    server_->serve();

    LOG(INFO) << "Thrift server finished serving";

  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to start Thrift server: " << e.what();
    started_ = false;
    throw;
  }
}

void ThriftServer::stop() {
  if (!started_ || !server_) {
    return;
  }

  try {
    PRESTO_SHUTDOWN_LOG(INFO) << "Stopping Thrift server...";
    server_->stop();
    server_.reset();
    started_ = false;
    PRESTO_SHUTDOWN_LOG(INFO) << "Thrift server stopped";
  } catch (const std::exception& e) {
    PRESTO_SHUTDOWN_LOG(ERROR) << "Error stopping Thrift server: " << e.what();
  }
}

folly::SocketAddress ThriftServer::address() const {
  if (server_) {
    return server_->getAddress();
  }
  return config_->address;
}

} // namespace facebook::presto::thrift
