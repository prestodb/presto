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
#include "presto_cpp/main/connectors/arrow_flight/FlightConfig.h"
#include "gtest/gtest.h"

namespace facebook::presto::connector::arrow_flight::test {

TEST(FlightConfigTest, DefaultConfig) {
  auto rawConfig = std::make_shared<velox::config::ConfigBase>(
      std::move(std::unordered_map<std::string, std::string>{}));
  auto config = FlightConfig(rawConfig);
  ASSERT_EQ(config.authenticatorName(), "none");
  ASSERT_EQ(config.defaultServerHostname(), std::nullopt);
  ASSERT_EQ(config.defaultServerPort(), std::nullopt);
  ASSERT_EQ(config.defaultServerSslEnabled(), false);
  ASSERT_EQ(config.serverVerify(), true);
  ASSERT_EQ(config.serverSslCertificate(), folly::none);
}

TEST(FlightConfigTest, OverrideConfig) {
  std::unordered_map<std::string, std::string> configMap = {
      {FlightConfig::kAuthenticatorName, "my-authenticator"},
      {FlightConfig::kDefaultServerHost, "my-server-host"},
      {FlightConfig::kDefaultServerPort, "9000"},
      {FlightConfig::kDefaultServerSslEnabled, "true"},
      {FlightConfig::kServerVerify, "false"},
      {FlightConfig::kServerSslCertificate, "my-cert.crt"}};
  auto config = FlightConfig(
      std::make_shared<velox::config::ConfigBase>(std::move(configMap)));
  ASSERT_EQ(config.authenticatorName(), "my-authenticator");
  ASSERT_EQ(config.defaultServerHostname(), "my-server-host");
  ASSERT_EQ(config.defaultServerPort(), 9000);
  ASSERT_EQ(config.defaultServerSslEnabled(), true);
  ASSERT_EQ(config.serverVerify(), false);
  ASSERT_EQ(config.serverSslCertificate(), "my-cert.crt");
}

} // namespace facebook::presto::connector::arrow_flight::test
