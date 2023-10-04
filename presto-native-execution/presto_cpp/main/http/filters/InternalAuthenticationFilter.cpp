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

#include "presto_cpp/main/http/filters/InternalAuthenticationFilter.h"
#ifdef PRESTO_ENABLE_JWT
#include <folly/ssl/OpenSSLHash.h> //@manual
#include <jwt-cpp/jwt.h> //@manual
#include <jwt-cpp/traits/nlohmann-json/traits.h> //@manual
#endif // PRESTO_ENABLE_JWT
#include <proxygen/httpserver/ResponseBuilder.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpConstants.h"

namespace facebook::presto::http::filters {

/// The filter is always enabled by the presto server.
/// Therefore, it is part of processing every request.
/// This enables detecting a misconfiguration if
/// the JWT is present in the request but cannot be
/// processed due to a missing configuration.
/// This filter lets requests pass through the stages of processing
/// provided the JWT in the request has been validated.
/// If the request is rejected on the first stage (onRequest),
/// upstream is notified of the error and the request chain
/// for the subsequent stages ends processing in this filter.
void InternalAuthenticationFilter::onRequest(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  auto token = msg->getHeaders().getSingleOrEmpty(kPrestoInternalBearer);
  if (!token.empty() &&
      !SystemConfig::instance()->internalCommunicationJwtEnabled()) {
    /// Error - catch if cluster uses token but this server is not configured
    /// properly.
    sendUnauthorizedResponse();
    return;
  }

  if (token.empty() &&
      SystemConfig::instance()->internalCommunicationJwtEnabled()) {
    // Error - require a token to be in the request.
    sendUnauthorizedResponse();
    return;
  }

  if (token.empty()) {
    // Forward the request.
    Filter::onRequest(std::move(msg));
  } else {
    processAndVerifyJwt(token, std::move(msg));
  }
}

void InternalAuthenticationFilter::onBody(
    std::unique_ptr<folly::IOBuf> body) noexcept {
  // Passthrough if request was not rejected.
  if (upstream_) {
    // forward
    upstream_->onBody(std::move(body));
  }
}

void InternalAuthenticationFilter::onEOM() noexcept {
  // Passthrough if request was not rejected.
  if (upstream_) {
    // forward
    upstream_->onEOM();
  }
}

void InternalAuthenticationFilter::requestComplete() noexcept {
  // Passthrough if request was not rejected.
  if (upstream_) {
    upstream_->requestComplete();
  }
  delete this;
}

void InternalAuthenticationFilter::onUpgrade(
    proxygen::UpgradeProtocol protocol) noexcept {
  // Passthrough if request was not rejected.
  if (upstream_) {
    upstream_->onUpgrade(protocol);
  }
}

void InternalAuthenticationFilter::onError(
    proxygen::ProxygenError err) noexcept {
  // If onError is invoked before we forward the error.
  if (upstream_) {
    upstream_->onError(err);
    upstream_ = nullptr;
  }
  delete this;
}

void InternalAuthenticationFilter::sendGenericErrorResponse(void) {
  /// Indicate to upstream an error occurred and make sure
  /// no further forwarding occurs.
  upstream_->onError(proxygen::kErrorUnsupportedExpectation);
  upstream_ = nullptr;

  proxygen::ResponseBuilder(downstream_)
      .status(kHttpInternalServerError, "Internal Server Error")
      .sendWithEOM();
}

void InternalAuthenticationFilter::sendUnauthorizedResponse() {
  /// Indicate to upstream an error occurred and make sure
  /// no further processing occurs.
  upstream_->onError(proxygen::kErrorUnauthorized);
  upstream_ = nullptr;

  proxygen::ResponseBuilder(downstream_)
      .status(kHttpUnauthorized, "Unauthorized")
      .sendWithEOM();
}

void InternalAuthenticationFilter::processAndVerifyJwt(
    const std::string& token,
    std::unique_ptr<proxygen::HTTPMessage> msg) {
#ifdef PRESTO_ENABLE_JWT
  try {
    // Build the signature key from the secret and validate the signature.
    auto secretHash = std::vector<uint8_t>(SHA256_DIGEST_LENGTH);
    folly::ssl::OpenSSLHash::sha256(
        folly::range(secretHash),
        folly::ByteRange(folly::StringPiece(
            SystemConfig::instance()->internalCommunicationSharedSecret())));

    // Decode and verify the JWT.
    auto decodedJwt = jwt::decode<jwt::traits::nlohmann_json>(token);
    auto verifier = jwt::verify<jwt::traits::nlohmann_json>().allow_algorithm(
        jwt::algorithm::hs256{std::string(
            reinterpret_cast<char*>(secretHash.data()), secretHash.size())});
    verifier.verify(decodedJwt);

    auto subject = decodedJwt.get_subject();
    // The nodeId of the requester is the subject. Check if it was set.
    if (subject.empty()) {
      std::error_code ec{jwt::error::token_verification_error::missing_claim};
      throw jwt::error::token_verification_exception(ec);
    }
    // Passed the verification, move the message along.
    Filter::onRequest(std::move(msg));
  } catch (const jwt::error::token_verification_exception& e) {
    sendUnauthorizedResponse();
  } catch (const jwt::error::signature_verification_exception& e) {
    sendUnauthorizedResponse();
  } catch (const std::system_error& e) {
    sendGenericErrorResponse();
  }
#endif // PRESTO_ENABLE_JWT
}

} // namespace facebook::presto::http::filters
