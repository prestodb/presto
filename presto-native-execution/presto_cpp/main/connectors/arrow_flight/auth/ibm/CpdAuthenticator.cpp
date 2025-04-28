#include "presto_cpp/main/connectors/arrow_flight/auth/ibm/CpdAuthenticator.h"
#include <arrow/flight/api.h>

namespace facebook::presto::ibm {

void CpdAuthenticator::authenticateClient(
    std::unique_ptr<arrow::flight::FlightClient>& client,
    const velox::config::ConfigBase* sessionProperties,
    arrow::flight::AddCallHeaders& headerWriter) {
  VELOX_USER_CHECK(
      sessionProperties->valueExists(CpdAuthenticator::kCpdTokenKey),
      "Arrow flight server token is missing from session properties");
  auto token =
      sessionProperties->get<std::string>(CpdAuthenticator::kCpdTokenKey)
          .value();
  headerWriter.AddHeader(
      CpdAuthenticator::kCpdAuthorizationKey, "Bearer " + token);
}

AFC_REGISTER_AUTH_FACTORY(std::make_shared<CpdAuthenticatorFactory>())

} // namespace facebook::presto::ibm
