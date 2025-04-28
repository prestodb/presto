#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/ibm/SaasAuthenticatorConfig.h"

namespace facebook::presto::ibm {

class SaasAuthenticator : public Authenticator {
 public:
  explicit SaasAuthenticator(
      const std::shared_ptr<const velox::config::ConfigBase> config)
      : saasAuthenticatorConfig_{
            std::make_shared<SaasAuthenticatorConfig>(config)} {}

  void authenticateClient(
      std::unique_ptr<arrow::flight::FlightClient>& client,
      const velox::config::ConfigBase* sessionProperties,
      arrow::flight::AddCallHeaders& headerWriter) override;

 private:
  std::string getToken();
  static std::string fetchToken(
      std::string_view apiKey,
      std::string_view tokenUrl);

 private:
  const std::shared_ptr<SaasAuthenticatorConfig> saasAuthenticatorConfig_;
  std::mutex tokenMutex_;
  folly::Optional<std::string> currentToken_;
  std::chrono::system_clock::time_point tokenGeneratedTime_;
};

class SaasAuthenticatorFactory : public AuthenticatorFactory {
 public:
  static constexpr const char* kSaasAuthenticatorName{"ibm-cpd-ext"};
  SaasAuthenticatorFactory() : AuthenticatorFactory{kSaasAuthenticatorName} {}
  explicit SaasAuthenticatorFactory(std::string_view name)
      : AuthenticatorFactory{name} {}

  std::shared_ptr<Authenticator> newAuthenticator(
      const std::shared_ptr<const velox::config::ConfigBase> config) override {
    return std::make_shared<SaasAuthenticator>(config);
  }
};

} // namespace facebook::presto::ibm
