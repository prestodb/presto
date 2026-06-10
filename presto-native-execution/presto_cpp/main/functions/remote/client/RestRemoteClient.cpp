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

#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"

#include <folly/Uri.h>
#include <proxygen/lib/http/HTTPMessage.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/functions/remote/utils/ContentTypes.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::velox;

namespace facebook::presto::functions::remote::rest {
namespace {
inline std::string getContentType(velox::functions::remote::PageFormat fmt) {
  return fmt == velox::functions::remote::PageFormat::SPARK_UNSAFE_ROW
      ? remote::CONTENT_TYPE_SPARK_UNSAFE_ROW
      : remote::CONTENT_TYPE_PRESTO_PAGE;
}
} // namespace

RestRemoteClient::RestRemoteClient(const std::string& url) : url_(url) {
  memPool_ = memory::MemoryManager::getInstance()->addLeafPool();
  folly::Uri uri(url_);
  proxygen::Endpoint endpoint(uri.host(), uri.port(), uri.scheme() == "https");
  folly::SocketAddress addr(uri.host().c_str(), uri.port(), true);

  evbThread_ = std::make_unique<folly::ScopedEventBaseThread>("rest-client");
  auto systemConfig = SystemConfig::instance();
  auto httpClientOptions = systemConfig->httpClientOptions();

  if (systemConfig->httpServerHttpsEnabled()) {
    ciphers_ = systemConfig->httpsSupportedCiphers();
    if (ciphers_.empty()) {
      VELOX_USER_FAIL(
          "HTTPS is enabled for remote function server but ciphers are not configured. "
          "Set 'https-supported-ciphers' in config.properties");
    }

    auto optionalClientCertPath = systemConfig->httpsClientCertAndKeyPath();
    if (!optionalClientCertPath.has_value()) {
      // This config is not used in server but validated here, otherwise, it
      // will fail later in the HttpClient during query execution.
      VELOX_USER_FAIL(
          "HTTPS client certificates are not configured correctly. "
          "Set 'https-client-cert-key-path' in config.properties");
    }

    sslContext_ = util::createSSLContext(
        optionalClientCertPath.value(),
        ciphers_,
        systemConfig->httpClientHttp2Enabled());
  }

  httpClient_ = std::make_shared<http::HttpClient>(
      evbThread_->getEventBase(),
      nullptr,
      endpoint,
      addr,
      requestTimeoutMs,
      connectTimeoutMs,
      memPool_,
      sslContext_,
      std::move(httpClientOptions));

  // Initialize JWT options
  jwtOptions_ = systemConfig->jwtOptions();
}

RestRemoteClient::~RestRemoteClient() {
  if (httpClient_) {
    evbThread_->getEventBase()->runInEventBaseThreadAndWait(
        [client = std::move(httpClient_)]() mutable { client.reset(); });
  }
  evbThread_.reset();
}

std::unique_ptr<folly::IOBuf> RestRemoteClient::invokeFunction(
    const std::string& fullUrl,
    velox::functions::remote::PageFormat serdeFormat,
    std::unique_ptr<folly::IOBuf> requestPayload) const {
  try {
    folly::Uri uri(fullUrl);
    const std::string contentType = getContentType(serdeFormat);
    // auto message = std::make_unique<proxygen::HTTPMessage>();
    // message->setMethod(proxygen::HTTPMethod::POST);
    // message->setURL(uri.path());
    // message->setHTTPVersion(1, 1);
    // message->getHeaders().add("Content-Type", contentType);
    // message->getHeaders().add("Accept", contentType);

    requestPayload->coalesce();
    std::string requestBody = requestPayload->moveToFbString().toStdString();

    // auto sendFuture = httpClient_->sendRequest(*message, requestBody);
    // Use RequestBuilder to automatically add JWT token
    auto sendFuture = http::RequestBuilder()
        .jwtOptions(jwtOptions_)  // This enables JWT token addition
        .method(proxygen::HTTPMethod::POST)
        .url(uri.path())
        .header("Content-Type", contentType)
        .header("Accept", contentType)
        .send(httpClient_.get(), requestBody);
    sendFuture.wait();

    VELOX_CHECK(
        sendFuture.hasValue(),
        "Invalid response returned from HTTP request to {}.",
        uri.host());

    std::unique_ptr<http::HttpResponse> resp = std::move(sendFuture).get();

    if (!resp) {
      VELOX_FAIL(
          "Null response object returned from HTTP request to {}.", uri.host());
    }

    if (resp->hasError()) {
      VELOX_FAIL("HTTP error: {}", resp->error());
    }

    int status = resp->headers()->getStatusCode();
    if (status < http::kHttpOk || status >= http::kHttpMultipleChoices) {
      VELOX_FAIL(
          "Server responded with status {}. Body: '{}'. URL: {}",
          status,
          resp->dumpBodyChain(),
          fullUrl);
    }

    return folly::IOBuf::copyBuffer(resp->dumpBodyChain());
  } catch (const std::exception& ex) {
    VELOX_FAIL("HTTP invocation failed for URL {}: {}", fullUrl, ex.what());
  }
  return nullptr;
}

} // namespace facebook::presto::functions::remote::rest
