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
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConfig.h"
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::presto;

TEST(ArrowFlightConfigTest, defaultConfig) {
  auto rawConfig = std::make_shared<config::ConfigBase>(
      std::move(std::unordered_map<std::string, std::string>{}));
  auto config = ArrowFlightConfig(rawConfig);
  ASSERT_EQ(config.authenticatorName(), "none");
  ASSERT_EQ(config.defaultServerHostname(), std::nullopt);
  ASSERT_EQ(config.defaultServerPort(), std::nullopt);
  ASSERT_EQ(config.defaultServerSslEnabled(), false);
  ASSERT_EQ(config.serverVerify(), true);
  ASSERT_EQ(config.serverSslCertificate(), std::nullopt);
  ASSERT_EQ(config.clientSslCertificate(), std::nullopt);
  ASSERT_EQ(config.clientSslKey(), std::nullopt);
}

TEST(ArrowFlightConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> configMap = {
      {ArrowFlightConfig::kAuthenticatorName, "my-authenticator"},
      {ArrowFlightConfig::kDefaultServerHost, "my-server-host"},
      {ArrowFlightConfig::kDefaultServerPort, "9000"},
      {ArrowFlightConfig::kDefaultServerSslEnabled, "true"},
      {ArrowFlightConfig::kServerVerify, "false"},
      {ArrowFlightConfig::kServerSslCertificate, "my-cert.crt"},
      {ArrowFlightConfig::kClientSslCertificate, "/path/to/client.crt"},
      {ArrowFlightConfig::kClientSslKey, "/path/to/client.key"}};
  auto config = ArrowFlightConfig(
      std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_EQ(config.authenticatorName(), "my-authenticator");
  ASSERT_EQ(config.defaultServerHostname(), "my-server-host");
  ASSERT_EQ(config.defaultServerPort(), 9000);
  ASSERT_EQ(config.defaultServerSslEnabled(), true);
  ASSERT_EQ(config.serverVerify(), false);
  ASSERT_EQ(config.serverSslCertificate(), "my-cert.crt");
  ASSERT_EQ(config.clientSslCertificate(), "/path/to/client.crt");
  ASSERT_EQ(config.clientSslKey(), "/path/to/client.key");
}
