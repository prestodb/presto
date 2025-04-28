#pragma once

#include "velox/common/config/Config.h"

namespace facebook::presto::ibm {

class SaasAuthenticatorConfig {
 public:
  static constexpr const char* kApiKey = "arrow-flight.apikey";

  static constexpr const char* kTokenUrl = "arrow-flight.cloud.token-url";

  static constexpr const char* kTokenExpiryTimeSec =
      "arrow-flight.cloud.token-expiry-time-sec";

  folly::Optional<std::string> apiKey();

  folly::Optional<std::string> tokenUrl();

  int64_t tokenExpiryTimeSec();

  SaasAuthenticatorConfig(
      std::shared_ptr<const velox::config::ConfigBase> config) {
    VELOX_CHECK_NOT_NULL(
        config, "Config is null for SaasAuthenticatorConfig initialization");
    config_ = std::move(config);
  }

 private:
  std::shared_ptr<const velox::config::ConfigBase> config_;
};

} // namespace facebook::presto::ibm
