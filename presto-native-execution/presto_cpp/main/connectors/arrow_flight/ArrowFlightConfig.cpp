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

namespace facebook::presto {

std::string ArrowFlightConfig::authenticatorName() const {
  return config_->get<std::string>(kAuthenticatorName, "none");
}

std::optional<std::string> ArrowFlightConfig::defaultServerHostname() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kDefaultServerHost));
}

std::optional<uint16_t> ArrowFlightConfig::defaultServerPort() const {
  return static_cast<std::optional<uint16_t>>(
      config_->get<uint16_t>(kDefaultServerPort));
}

bool ArrowFlightConfig::defaultServerSslEnabled() const {
  return config_->get<bool>(kDefaultServerSslEnabled, false);
}

bool ArrowFlightConfig::serverVerify() const {
  return config_->get<bool>(kServerVerify, true);
}

std::optional<std::string> ArrowFlightConfig::serverSslCertificate() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kServerSslCertificate));
}

std::optional<std::string> ArrowFlightConfig::clientSslCertificate() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kClientSslCertificate));
}

std::optional<std::string> ArrowFlightConfig::clientSslKey() const {
  return static_cast<std::optional<std::string>>(
      config_->get<std::string>(kClientSslKey));
}

} // namespace facebook::presto
