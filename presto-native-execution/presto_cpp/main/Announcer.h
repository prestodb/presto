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

namespace facebook::presto {

class Announcer {
 public:
  Announcer(
      const std::string& address,
      int port,
      std::function<folly::SocketAddress()> discoveryAddressLookup,
      const std::string& nodeVersion,
      const std::string& environment,
      const std::string& nodeId,
      const std::string& nodeLocation,
      const std::vector<std::string>& connectorIds,
      int frequencyMs);

  ~Announcer();

  void start();

  void stop();

 private:
  void makeAnnouncement();

  void scheduleNext();

  std::function<folly::SocketAddress()> discoveryAddressLookup_;
  const int frequencyMs_;
  const std::string announcementBody_;
  const proxygen::HTTPMessage announcementRequest_;
  folly::SocketAddress address_;
  std::unique_ptr<http::HttpClient> client_;
  std::atomic_bool stopped_{true};
  folly::EventBaseThread eventBaseThread_;
};

} // namespace facebook::presto
