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

class Announcer {
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
      uint64_t frequencyMs,
      const std::string& clientCertAndKeyPath = "",
      const std::string& ciphers = "");

  ~Announcer() = default;

  void start();

  void stop();

 private:
  void makeAnnouncement();

  void scheduleNext();

  const std::shared_ptr<CoordinatorDiscoverer> coordinatorDiscoverer_;
  const uint64_t frequencyMs_;
  const std::string announcementBody_;
  const proxygen::HTTPMessage announcementRequest_;
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  folly::SocketAddress address_;
  std::shared_ptr<http::HttpClient> client_;
  std::atomic_bool stopped_{true};
  folly::EventBaseThread eventBaseThread_;
  const std::string clientCertAndKeyPath_;
  const std::string ciphers_;
};

} // namespace facebook::presto
