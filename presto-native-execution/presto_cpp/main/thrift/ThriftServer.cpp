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

#include "presto_cpp/main/thrift/ThriftServer.h"

#include <glog/logging.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/services/cpp/ServiceFramework.h"
#include "common/services/cpp/TLSConstants.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/thrift/PrestoThriftServiceHandler.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::thrift {

int ThriftConfig::getSystemConfigValue(const std::string& key, int defaultValue) {
  auto systemConfig = SystemConfig::instance();
  return systemConfig ?
    systemConfig->optionalProperty<int>(std::string_view(key)).value_or(defaultValue) :
    defaultValue;
}

ThriftConfig::ThriftConfig(
    const folly::SocketAddress& address,
    const std::string& certPath,
    const std::string& keyPath,
    const std::string& supportedCiphers)
    : address_(address),
      certPath_(certPath),
      keyPath_(keyPath),
      supportedCiphers_(supportedCiphers),
      taskExpireTimeMs_(getSystemConfigValue("presto.thrift-server.task-expire-time-ms", 300000)),
      streamExpireTimeMs_(getSystemConfigValue("presto.thrift-server.stream-expire-time", 300000)),
      maxRequest_(getSystemConfigValue("presto.thrift-server.max-requests", 10000)),
      maxConnections_(getSystemConfigValue("presto.thrift-server.max-connections", 10000)),
      idleTimeout_(getSystemConfigValue("presto.thrift-server.idle-timeout", 300000)) {
}

ThriftServer::ThriftServer(
    std::unique_ptr<ThriftConfig> config,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    std::shared_ptr<VeloxPlanValidator> planValidator,
    std::shared_ptr<TaskManager> taskManager)
    : config_(std::move(config)) {
  VELOX_CHECK_NOT_NULL(config_);
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_CHECK_NOT_NULL(planValidator);
  VELOX_CHECK_NOT_NULL(taskManager);

  server_ = std::make_unique<apache::thrift::ThriftServer>();
  handler_ = std::make_shared<PrestoThriftServiceHandler>(
      pool, planValidator, taskManager);

  server_->setInterface(handler_);
  server_->setAddress(config_->getAddress());

  // Set connection limits and timeouts
  server_->setMaxConnections(config_->getMaxConnections());
  server_->setMaxRequests(config_->getMaxRequest());
  server_->setIdleTimeout(std::chrono::milliseconds(config_->getIdleTimeout()));
  server_->setTaskExpireTime(std::chrono::milliseconds(config_->getTaskExpireTimeMs()));
  server_->setStreamExpireTime(std::chrono::milliseconds(config_->getStreamExpireTimeMs()));

  // Configure SSL if cert path is provided
  if (!config_->getCertPath().empty() && !config_->getKeyPath().empty()) {
    auto sslContextConfig = services::TLSConfig::createDefaultConfig();
    sslContextConfig->isDefault = true;
    sslContextConfig->clientVerification =
        folly::SSLContext::VerifyClientCertificate::DO_NOT_REQUEST;
    sslContextConfig->setCertificate(config_->getCertPath(), config_->getKeyPath(), "");
    sslContextConfig->sslCiphers = config_->getSupportedCiphers();
    server_->setSSLConfig(std::move(sslContextConfig));
  }
}

void ThriftServer::start() {
  PRESTO_STARTUP_LOG(INFO) << "=== THRIFT SERVER CONFIGURATION SUMMARY ==="
            << "\n  Address: " << config_->getAddress().getAddressStr() << ":" << config_->getAddress().getPort()
            << "\n  Max Conns: " << config_->getMaxConnections()
            << "\n  Max Requests: " << config_->getMaxRequest()
            << "\n  Idle Timeout: " << config_->getIdleTimeout() << "ms"
            << "\n  Task/Stream Timeout: " << config_->getTaskExpireTimeMs() << "ms";
  server_->serve();
}

} // namespace facebook::presto::thrift
