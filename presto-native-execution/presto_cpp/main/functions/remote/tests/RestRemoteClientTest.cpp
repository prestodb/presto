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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::presto::functions::remote::rest;

namespace facebook::presto::functions::remote::rest::test {
namespace {

// Helper to set SystemConfig properties for each test.
void setConfig(const std::unordered_map<std::string, std::string>& props) {
  std::unordered_map<std::string, std::string> values(props);
  SystemConfig::instance()->initialize(
      std::make_unique<velox::config::ConfigBase>(std::move(values)));
}

// ──────────────────────────────────────────────────────────────
// Positive scenario: HTTP (no TLS) — constructor succeeds.
// ──────────────────────────────────────────────────────────────
TEST(RestRemoteClientTest, constructsSuccessfullyOverHttp) {
  setConfig({{"http-server.https.enabled", "false"}});
  // Should not throw — plain HTTP needs no certs.
  EXPECT_NO_THROW(RestRemoteClient client("http://127.0.0.1:9999"));
}

// ──────────────────────────────────────────────────────────────
// Negative scenario 1: HTTPS enabled but ciphers not configured.
// ──────────────────────────────────────────────────────────────
TEST(RestRemoteClientTest, throwsWhenHttpsEnabledButCiphersEmpty) {
  setConfig({
      {"http-server.https.enabled", "true"},
      {"https-supported-ciphers", ""},
      {"https-client-cert-key-path", "/some/path/client.pem"},
  });
  VELOX_ASSERT_THROW(
      RestRemoteClient("https://127.0.0.1:9999"),
      "HTTPS is enabled for remote function server but ciphers are not configured");
}

// ──────────────────────────────────────────────────────────────
// Negative scenario 2: HTTPS enabled, ciphers set, but client cert missing.
// ──────────────────────────────────────────────────────────────
TEST(RestRemoteClientTest, throwsWhenHttpsEnabledButClientCertMissing) {
  setConfig({
      {"http-server.https.enabled", "true"},
      {"https-supported-ciphers", "ECDHE-RSA-AES128-GCM-SHA256"},
      // https-client-cert-key-path intentionally absent
  });
  VELOX_ASSERT_THROW(
      RestRemoteClient("https://127.0.0.1:9999"),
      "HTTPS client certificates are not configured correctly");
}

} // namespace
} // namespace facebook::presto::functions::remote::rest::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
