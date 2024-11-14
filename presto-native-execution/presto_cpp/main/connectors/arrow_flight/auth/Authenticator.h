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

#include "arrow/flight/api.h"
#include "velox/common/config/Config.h"

namespace facebook::presto::connector::arrow_flight::auth {

class Authenticator {
 public:
  /// @brief Override this method to define implementation-specific
  /// authentication This could be through client->Authenticate, or
  /// client->AuthenticateBasicToken or any other custom strategy
  /// @param client the Flight client which is to be authenticated
  /// @param extraCredentials extra credential data used for authentication
  /// @param headerWriter write-only object used to set authentication headers
  virtual void authenticateClient(
      std::unique_ptr<arrow::flight::FlightClient>& client,
      const std::map<std::string, std::string>& extraCredentials,
      arrow::flight::AddCallHeaders& headerWriter) = 0;
};

class AuthenticatorFactory {
 public:
  AuthenticatorFactory(std::string_view name) : name_{name} {}
  const std::string& name() const {
    return name_;
  }
  virtual std::shared_ptr<Authenticator> newAuthenticator(
      const std::shared_ptr<const velox::config::ConfigBase> config) = 0;

 private:
  std::string name_;
};

bool registerAuthenticatorFactory(
    std::shared_ptr<AuthenticatorFactory> factory);
std::shared_ptr<AuthenticatorFactory> getAuthenticatorFactory(
    const std::string& name);

#define AFC_REGISTER_AUTH_FACTORY(factory)                                     \
  namespace {                                                                  \
  static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) = ::facebook::presto:: \
      connector::arrow_flight::auth::registerAuthenticatorFactory((factory));  \
  }

class NoOpAuthenticator : public Authenticator {
 public:
  void authenticateClient(
      std::unique_ptr<arrow::flight::FlightClient>& client,
      const std::map<std::string, std::string>& extraCredentials,
      arrow::flight::AddCallHeaders& headerWriter) override {}
};

class NoOpAuthenticatorFactory : public AuthenticatorFactory {
 public:
  static constexpr const std::string_view kNoOpAuthenticatorName{"none"};
  NoOpAuthenticatorFactory() : AuthenticatorFactory{kNoOpAuthenticatorName} {}
  NoOpAuthenticatorFactory(std::string_view name)
      : AuthenticatorFactory{name} {}
  std::shared_ptr<Authenticator> newAuthenticator(
      const std::shared_ptr<const velox::config::ConfigBase> config) override {
    return std::make_shared<NoOpAuthenticator>();
  }
};

} // namespace facebook::presto::connector::arrow_flight::auth
