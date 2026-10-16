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

#include "presto_cpp/main/CoordinatorDiscoverer.h"
#include <fmt/core.h>
#include <folly/Uri.h>
#include "presto_cpp/main/common/Configs.h"

namespace facebook::presto {

folly::SocketAddress CoordinatorDiscoverer::updateAddress() {
  // Check if discovery URI is specified. Presto-on-Spark doesn't specify it.
  auto discoveryUri = SystemConfig::instance()->discoveryUri();
  if (!discoveryUri.has_value()) {
    // Returns the previous cached address or empty address if fetch never
    // succeeded.
    return coordinatorAddress_;
  }

  auto uri = folly::Uri(discoveryUri.value());
  updateAddressSafe(folly::SocketAddress(uri.hostname(), uri.port(), true));
  // Format host:port for use as the HTTP Host header. IPv6 literals must be
  // bracketed (RFC 7230 §5.4): "[::1]:8080" not "::1:8080".
  const auto& host = uri.hostname();
  const bool isIpv6 = host.find(':') != std::string::npos;
  updateHostHeaderSafe(
      isIpv6 ? fmt::format("[{}]:{}", host, uri.port())
             : fmt::format("{}:{}", host, uri.port()));
  return coordinatorAddress_;
}

folly::SocketAddress CoordinatorDiscoverer::getAddress() {
  std::lock_guard<std::mutex> l(mutex_);
  return coordinatorAddress_;
}

std::string CoordinatorDiscoverer::getHostHeader() {
  std::lock_guard<std::mutex> l(mutex_);
  return coordinatorHostHeader_;
}

void CoordinatorDiscoverer::updateAddressSafe(
    const folly::SocketAddress& address) {
  std::lock_guard<std::mutex> l(mutex_);
  if (coordinatorAddress_ != address) {
    LOG(INFO) << "Coordinator address changed to " << address;
  }
  coordinatorAddress_ = address;
}

void CoordinatorDiscoverer::updateHostHeaderSafe(
    const std::string& hostHeader) {
  std::lock_guard<std::mutex> l(mutex_);
  coordinatorHostHeader_ = hostHeader;
}

} // namespace facebook::presto
