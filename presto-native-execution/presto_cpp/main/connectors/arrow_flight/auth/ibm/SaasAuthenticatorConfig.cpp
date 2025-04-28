#include "presto_cpp/main/connectors/arrow_flight/auth/ibm/SaasAuthenticatorConfig.h"

namespace facebook::presto::ibm {

folly::Optional<std::string> SaasAuthenticatorConfig::apiKey() {
  return config_->get<std::string>(kApiKey);
}

folly::Optional<std::string> SaasAuthenticatorConfig::tokenUrl() {
  return config_->get<std::string>(kTokenUrl);
}

int64_t SaasAuthenticatorConfig::tokenExpiryTimeSec() {
  return config_->get<int64_t>(kTokenExpiryTimeSec, 1200);
}

} // namespace facebook::presto::ibm
