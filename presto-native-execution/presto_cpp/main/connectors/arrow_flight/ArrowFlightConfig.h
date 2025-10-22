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

#include "velox/common/config/Config.h"

namespace facebook::presto {

class ArrowFlightConfig {
 public:
  explicit ArrowFlightConfig(
      std::shared_ptr<const velox::config::ConfigBase> config)
      : config_{config} {}

  static constexpr const char* kAuthenticatorName =
      "arrow-flight.authenticator.name";

  static constexpr const char* kDefaultServerHost = "arrow-flight.server";

  static constexpr const char* kDefaultServerPort = "arrow-flight.server.port";

  static constexpr const char* kDefaultServerSslEnabled =
      "arrow-flight.server-ssl-enabled";

  static constexpr const char* kServerVerify = "arrow-flight.server.verify";

  static constexpr const char* kServerSslCertificate =
      "arrow-flight.server-ssl-certificate";

  static constexpr const char* kClientSslCertificate =
      "arrow-flight.client-ssl-certificate";

  static constexpr const char* kClientSslKey =
      "arrow-flight.client-ssl-key";

  std::string authenticatorName() const;

  std::optional<std::string> defaultServerHostname() const;

  std::optional<uint16_t> defaultServerPort() const;

  bool defaultServerSslEnabled() const;

  bool serverVerify() const;

  std::optional<std::string> serverSslCertificate() const;

  std::optional<std::string> clientSslCertificate() const;
  
  std::optional<std::string> clientSslKey() const;

 private:
  const std::shared_ptr<const velox::config::ConfigBase> config_;
};

} // namespace facebook::presto
