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

#include <folly/SocketAddress.h>
#include <mutex>
#include <string>

namespace facebook::presto {

// TODO: Rename to ServiceDiscoverer as this may talk to either
// Coordinator or Resource Manager.
class CoordinatorDiscoverer {
 public:
  virtual ~CoordinatorDiscoverer() = default;

  /// Fetches the coordinator address and update the cached address. The
  /// function returns the refreshed one on fetch success or the previously
  /// cached address. It will return an empty address if the fetches have never
  /// succeeded.
  virtual folly::SocketAddress updateAddress();

  /// Returns the cached coordinator address.
  folly::SocketAddress getAddress();

  /// Returns the cached coordinator hostname as "host:port", derived from
  /// discovery.uri. Used to set the HTTP Host header on announcements so
  /// that http-server.https.sni-host-check on the coordinator passes.
  std::string getHostname();

 protected:
  /// Updates the cached coordinator address with the new one.
  void updateAddressSafe(const folly::SocketAddress& address);

  /// Updates the cached coordinator hostname string.
  void updateHostnameSafe(const std::string& hostname);

 private:
  /// Mutex to protect 'coordinatorAddress_' and 'coordinatorHostname_'.
  std::mutex mutex_;
  folly::SocketAddress coordinatorAddress_;
  std::string coordinatorHostname_;
};

} // namespace facebook::presto
