#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"

namespace facebook::presto::ibm {

class CpdAuthenticator : public Authenticator {
 public:
  static constexpr const char* kCpdAuthorizationKey{"authorization"};
  static constexpr const char* kCpdTokenKey{"token"};
  void authenticateClient(
      std::unique_ptr<arrow::flight::FlightClient>& client,
      const velox::config::ConfigBase* sessionProperties,
      arrow::flight::AddCallHeaders& headerWriter) override;
};

class CpdAuthenticatorFactory : public AuthenticatorFactory {
 public:
  static constexpr const char* kCpdAuthenticatorName{"ibm-cpd-int"};
  CpdAuthenticatorFactory() : AuthenticatorFactory{kCpdAuthenticatorName} {}
  explicit CpdAuthenticatorFactory(std::string_view name)
      : AuthenticatorFactory{name} {}

  std::shared_ptr<Authenticator> newAuthenticator(
      const std::shared_ptr<const velox::config::ConfigBase> config) override {
    return std::make_shared<CpdAuthenticator>();
  }
};

} // namespace facebook::presto::ibm
