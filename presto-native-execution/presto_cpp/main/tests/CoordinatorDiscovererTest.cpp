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
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/tests/MutableConfigs.h"
#include "velox/common/file/FileSystems.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {

class CoordinatorDiscovererTest : public testing::Test {
 public:
  void SetUp() override {
    filesystems::registerLocalFileSystem();
    test::setupMutableSystemConfig();
  }
};

TEST_F(CoordinatorDiscovererTest, basic) {
  auto systemConfig = SystemConfig::instance();
  auto coordinatorDiscoverer = std::make_unique<CoordinatorDiscoverer>();
  EXPECT_TRUE(coordinatorDiscoverer->updateAddress().empty());

  systemConfig->setValue(
      std::string(SystemConfig::kDiscoveryUri), "http://88.88.88.88:8888");

  EXPECT_TRUE(coordinatorDiscoverer->getAddress().empty());
  auto updatedAddress = coordinatorDiscoverer->updateAddress();
  EXPECT_FALSE(updatedAddress.empty());
  EXPECT_EQ(updatedAddress.getAddressStr(), "88.88.88.88");
  EXPECT_EQ(updatedAddress.getPort(), 8888);

  auto cachedAddress = coordinatorDiscoverer->getAddress();
  EXPECT_EQ(cachedAddress.getAddressStr(), "88.88.88.88");
  EXPECT_EQ(cachedAddress.getPort(), 8888);
}

// Verify that getHostHeader() returns the coordinator's hostname:port string
// derived from discovery.uri. This value is used to set the HTTP Host header
// on announcements so that http-server.https.sni-host-check passes.
// Use a DNS name (localhost) so that the stored host header differs from the
// resolved IP address -- that is the exact scenario the bug covered.
TEST_F(CoordinatorDiscovererTest, hostnameStoredFromDiscoveryUri) {
  auto systemConfig = SystemConfig::instance();
  auto coordinatorDiscoverer = std::make_unique<CoordinatorDiscoverer>();

  // Before any update, host header is empty.
  EXPECT_TRUE(coordinatorDiscoverer->getHostHeader().empty());

  systemConfig->setValue(
      std::string(SystemConfig::kDiscoveryUri), "http://localhost:8888");

  coordinatorDiscoverer->updateAddress();
  // Host header preserves the DNS name from discovery.uri unchanged.
  EXPECT_EQ(coordinatorDiscoverer->getHostHeader(), "localhost:8888");
  // The resolved socket address must be a loopback IP (127.0.0.1 or ::1),
  // confirming DNS resolution occurred and the stored hostname was not used.
  EXPECT_TRUE(coordinatorDiscoverer->getAddress().isLoopbackAddress());
}

// Verify that IPv6 literal hostnames are bracketed in the Host header value
// (RFC 7230 §5.4) so that "::1:8580" is not produced.
TEST_F(CoordinatorDiscovererTest, ipv6HostnameBracketed) {
  auto systemConfig = SystemConfig::instance();
  auto coordinatorDiscoverer = std::make_unique<CoordinatorDiscoverer>();

  systemConfig->setValue(
      std::string(SystemConfig::kDiscoveryUri), "http://[::1]:8580");

  coordinatorDiscoverer->updateAddress();
  EXPECT_EQ(coordinatorDiscoverer->getHostHeader(), "[::1]:8580");
}

} // namespace
} // namespace facebook::presto
