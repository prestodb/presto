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
    // ===== ENABLE BASIC LOGGING =====

    // Enable verbose glog logging
    FLAGS_v = 3;  // Verbose logging (reduced from 3 to avoid too much noise)
    FLAGS_logtostderr = 1;  // Log to stderr

    LOG(INFO) << "=== EXTENSIVE THRIFT LOGGING ENABLED ===";
    LOG(INFO) << "Verbose level: " << FLAGS_v;

    // Create the Thrift server
    server_ = std::make_unique<apache::thrift::ThriftServer>();

    // Create the service handler
    auto handler = std::make_shared<PrestoThriftServiceHandler>(
        pool_, planValidator_, taskManager_);

    LOG(INFO) << "Created Thrift service handler";

    // Configure the server - let it manage its own threads completely
    server_->setInterface(handler);
    LOG(INFO) << "Set Thrift service interface";
    server_->setAddress(config_->address);
    server_->setNumIOWorkerThreads(config_->numIoThreads);
    server_->setNumCPUWorkerThreads(config_->numCpuThreads);
    server_->setReusePort(config_->reusePort);

    // ===== LOG ALL CONFIGURATION =====
    LOG(INFO) << "=== THRIFT SERVER CONFIGURATION ===";
    LOG(INFO) << "Address: " << config_->address.getAddressStr() << ":" << config_->address.getPort();
    LOG(INFO) << "IO Threads: " << config_->numIoThreads;
    LOG(INFO) << "CPU Threads: " << config_->numCpuThreads;
    LOG(INFO) << "Reuse Port: " << config_->reusePort;
    LOG(INFO) << "SSL Verification: " << config_->sslVerification;

    // Configure load shedding and queue settings to handle high load
    server_->setTaskExpireTime(std::chrono::milliseconds(
        config_->taskExpireTimeMs)); // 30 second timeout
    server_->setStreamExpireTime(
        std::chrono::milliseconds(config_->streamExpireTimeMs));
    server_->setQueueTimeout(std::chrono::milliseconds(
        config_
            ->streamExpireTimeMs)); // Increase queue timeout from default 100ms
    server_->setMaxRequests(
        config_->maxRequest); // Increase max concurrent requests

    server_->setMaxConnections(10000);
    server_->setIdleTimeout(std::chrono::milliseconds(60000));
    server_->setListenBacklog(1024);

    LOG(INFO)
        << "Thrift server configured with increased timeouts and queue limits";

    // configure SSL if sslVerification is >1
    if (config_->sslVerification >= 1) {
      LOG(INFO) << "=== CONFIGURING SSL/TLS ===";
      auto sslContextConfig = services::TLSConfig::createDefaultConfig();
      sslContextConfig->clientVerification = folly::SSLContext::VerifyClientCertificate::IF_PRESENTED;
      LOG(INFO) << "SSL Client Verification: IF_PRESENTED";

      if (config_->sslVerification == 2) {
        // Drift client doesn't support Rocket Socket, so prioritize "thrift" protocol
        std::list<std::string> driftCompatibleProtocols = {"thrift", "h2", "http/1.1", "http"};
        LOG(INFO) << "Setting ALPN protocols for Drift compatibility: " << folly::join(", ", driftCompatibleProtocols);
        sslContextConfig->setNextProtocols(driftCompatibleProtocols);
      }

      if (config_->sslVerification == 3) {
        // Drift client doesn't support Rocket Socket, so prioritize "thrift" protocol
        std::list<std::string> driftCompatibleProtocols = {"thrift"};
        LOG(INFO) << "Setting ALPN protocols for Drift compatibility: " << folly::join(", ", driftCompatibleProtocols);
        sslContextConfig->setNextProtocols(driftCompatibleProtocols);
      }

      server_->setSSLConfig(std::move(sslContextConfig));
      LOG(INFO) << "=== SSL/TLS CONFIGURATION COMPLETED ===";
      LOG(INFO) << "Thrift server configured with SSL/TLS enabled";
    } else {
      LOG(INFO) << "=== SSL/TLS DISABLED ===";
      LOG(INFO) << "Thrift server configured for plain TCP (SSL disabled)";
    }

    LOG(INFO) << "Starting Thrift server on "
              << config_->address.getAddressStr() << ":"
              << config_->address.getPort();

    // Use the blocking serve method directly - this avoids the PrimaryPtr issue
    // by not calling setup() separately
    started_ = true;

    LOG(INFO) << "Thrift server starting on port "
              << config_->address.getPort();

    // This will setup and serve (blocking until stopped)
    LOG(INFO) << "About to call server_->serve()";
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
    LOG(INFO) << "Stopping Thrift server...";
    server_->stop();
    server_.reset();
    started_ = false;
    LOG(INFO) << "Thrift server stopped";
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error stopping Thrift server: " << e.what();
  }
}

folly::SocketAddress ThriftServer::address() const {
  if (server_) {
    return server_->getAddress();
  }
  return config_->address;
}

} // namespace facebook::presto::thrift
