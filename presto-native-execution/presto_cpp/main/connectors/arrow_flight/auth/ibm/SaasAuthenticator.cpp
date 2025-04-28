#include "presto_cpp/main/connectors/arrow_flight/auth/ibm/SaasAuthenticator.h"
#include <arrow/flight/api.h>
#include <cpr/cpr.h>
#include "presto_cpp/external/json/nlohmann/json.hpp"

namespace facebook::presto::ibm {

using json = nlohmann::json;

void SaasAuthenticator::authenticateClient(
    std::unique_ptr<arrow::flight::FlightClient>& client,
    const velox::config::ConfigBase* sessionProperties,
    arrow::flight::AddCallHeaders& headerWriter) {
  headerWriter.AddHeader("authorization", "Bearer " + getToken());
}

std::string SaasAuthenticator::getToken() {
  auto tokenExpiryDuration =
      std::chrono::seconds(saasAuthenticatorConfig_->tokenExpiryTimeSec());
  std::lock_guard lock(tokenMutex_);
  auto now = std::chrono::system_clock::now();
  auto elapsed = now - tokenGeneratedTime_;
  if (!currentToken_.hasValue() || elapsed >= tokenExpiryDuration) {
    auto tokenUrl = saasAuthenticatorConfig_->tokenUrl();
    auto apiKey = saasAuthenticatorConfig_->apiKey();
    VELOX_CHECK(tokenUrl, "Arrow flight server token url not given");
    VELOX_CHECK(apiKey, "Arrow flight server api key not given");
    currentToken_ = fetchToken(apiKey.value(), tokenUrl.value());
    tokenGeneratedTime_ = now;
  }
  return currentToken_.value();
}

std::string SaasAuthenticator::fetchToken(
    std::string_view apiKey,
    std::string_view tokenUrl) {
  auto response = cpr::Post(
      cpr::Url{tokenUrl},
      cpr::Header{{"Content-Type", "application/x-www-form-urlencoded"}},
      cpr::Header{{"Accept", "application/json"}},
      cpr::Body{
          "response_type=cloud_iam&grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=" +
          std::string(apiKey)});
  VELOX_USER_CHECK(
      response.status_code == 200,
      "Arrow flight server token fetching failed with error code: {} and error: {}.",
      response.status_code,
      response.error.message);
  return json::parse(response.text).at("access_token").get<std::string>();
}

AFC_REGISTER_AUTH_FACTORY(std::make_shared<SaasAuthenticatorFactory>())

} // namespace facebook::presto::ibm
