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
#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include <arrow/flight/api.h>
#include "velox/common/base/Exceptions.h"

namespace facebook::presto {
namespace {
auto& authenticatorFactories() {
  static std::unordered_map<std::string, std::shared_ptr<AuthenticatorFactory>>
      factories;
  return factories;
}
} // namespace

bool registerAuthenticatorFactory(
    std::shared_ptr<AuthenticatorFactory> factory) {
  bool ok = authenticatorFactories().insert({factory->name(), factory}).second;
  VELOX_CHECK(
      ok,
      "Flight AuthenticatorFactory with name {} is already registered",
      factory->name());
  return true;
}

std::shared_ptr<AuthenticatorFactory> getAuthenticatorFactory(
    const std::string& name) {
  auto it = authenticatorFactories().find(name);
  VELOX_CHECK(
      it != authenticatorFactories().end(),
      "Flight AuthenticatorFactory with name {} not registered",
      name);
  return it->second;
}

AFC_REGISTER_AUTH_FACTORY(std::make_shared<NoOpAuthenticatorFactory>())

} // namespace facebook::presto
