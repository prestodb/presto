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

#include <folly/io/async/EventBaseThread.h>
#include <presto_cpp/main/http/HttpClient.h>
#include "presto_cpp/main/CoordinatorDiscoverer.h"

namespace facebook::presto {

class PeriodicServiceInventoryManager {
 public:
  PeriodicServiceInventoryManager(
      std::string address,
      int port,
      std::shared_ptr<CoordinatorDiscoverer> coordinatorDiscoverer,
      folly::SSLContextPtr sslContext,
      std::string id,
      uint64_t frequencyMs);

  void start();

  void stop();

  /// If disabled then won't send any requests, but keeps itself running.
  void enableRequest(bool enable);

 protected:
  // Denotes whether we retry failed requests due to network errors.
  virtual bool retryFailed() {
    return true;
  }

  // For every N requests, we update the service address. This might be
  // needed for cases where we need to send requests so often that we cannot
  // afford to update service each time.
  virtual int updateServiceTimes() {
    return 1;
  }

  virtual std::tuple<proxygen::HTTPMessage, std::string> httpRequest() = 0;

  void sendRequest();

  void scheduleNext();

  uint64_t getDelayMs();

  const std::string address_;
  const int port_;
  const std::shared_ptr<CoordinatorDiscoverer> coordinatorDiscoverer_;
  const folly::SSLContextPtr sslContext_;
  const std::string id_;
  const uint64_t frequencyMs_;
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  /// jitter value for backoff delay time in case of failure
  const double backOffjitterParam_{0.1};

  folly::EventBaseThread eventBaseThread_;
  std::unique_ptr<proxygen::SessionPool> sessionPool_;
  folly::SocketAddress serviceAddress_;
  std::shared_ptr<http::HttpClient> client_;
  std::atomic_bool stopped_{true};
  std::atomic_bool requestEnabled_{true};
  uint64_t failedAttempts_{0};
  uint64_t attempts_{0};

  virtual ~PeriodicServiceInventoryManager() {}
};
} // namespace facebook::presto
