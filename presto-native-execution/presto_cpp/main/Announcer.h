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
#include "presto_cpp/main/PeriodicServiceInventoryManager.h"

namespace facebook::presto {

class Announcer : public PeriodicServiceInventoryManager {
 public:
  Announcer(
      const std::string& address,
      bool useHttps,
      int port,
      const std::shared_ptr<CoordinatorDiscoverer>& coordinatorDiscoverer,
      const std::string& nodeVersion,
      const std::string& environment,
      const std::string& nodeId,
      const std::string& nodeLocation,
      const std::vector<std::string>& connectorIds,
      const uint64_t maxFrequencyMs_,
      folly::SSLContextPtr sslContext);

  ~Announcer() = default;

 protected:
  std::tuple<proxygen::HTTPMessage, std::string> httpRequest() override;

 private:
  const std::string announcementBody_;
  const proxygen::HTTPMessage announcementRequest_;
};

} // namespace facebook::presto
