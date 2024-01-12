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
#include "presto_cpp/main/PeriodicHeartbeatManager.h"
#include <velox/common/memory/Memory.h>

namespace facebook::presto {
PeriodicHeartbeatManager::PeriodicHeartbeatManager(
    const std::string& address,
    int port,
    const std::shared_ptr<CoordinatorDiscoverer>& coordinatorDiscoverer,
    folly::SSLContextPtr sslContext,
    std::function<protocol::NodeStatus()> nodeStatusFetcher,
    uint64_t frequencyMs)
    : PeriodicServiceInventoryManager(
          address,
          port,
          coordinatorDiscoverer,
          std::move(sslContext),
          "Heartbeat",
          frequencyMs),
      nodeStatusFetcher_(std::move(nodeStatusFetcher)) {}

std::tuple<proxygen::HTTPMessage, std::string>
PeriodicHeartbeatManager::httpRequest() {
  nlohmann::json j;
  to_json(j, nodeStatusFetcher_());
  std::string body = j.dump();
  proxygen::HTTPMessage request;
  request.setMethod(proxygen::HTTPMethod::PUT);
  request.setURL("/v1/heartbeat");
  request.getHeaders().set(
      proxygen::HTTP_HEADER_HOST, fmt::format("{}:{}", address_, port_));
  request.getHeaders().set(
      proxygen::HTTP_HEADER_CONTENT_TYPE, "application/json");
  request.getHeaders().set(
      proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
  return {request, body};
}

} // namespace facebook::presto
