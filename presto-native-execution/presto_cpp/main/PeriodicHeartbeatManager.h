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

#include "presto_cpp/main/PeriodicServiceInventoryManager.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"

namespace facebook::presto {

class PeriodicHeartbeatManager : public PeriodicServiceInventoryManager {
 public:
  PeriodicHeartbeatManager(
      const std::string& address,
      int port,
      const std::shared_ptr<CoordinatorDiscoverer>& coordinatorDiscoverer,
      folly::SSLContextPtr sslContext,
      std::function<protocol::NodeStatus()> nodeStatusFetcher,
      uint64_t frequencyMs);

 protected:
  bool retryFailed() override {
    return false;
  }

  int updateServiceTimes() override {
    return 10;
  }

  std::tuple<proxygen::HTTPMessage, std::string> httpRequest() override;

 private:
  std::function<protocol::NodeStatus()> nodeStatusFetcher_;
};
} // namespace facebook::presto
