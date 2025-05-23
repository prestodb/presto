/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "presto_cpp/main/functions/remote/client/RestClient.h"

#include <folly/Uri.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <proxygen/lib/http/HTTPMessage.h>

#include "presto_cpp/main/functions/remote/client/Remote.h"
#include "presto_cpp/main/functions/remote/utils/ContentTypes.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"

namespace facebook::presto::functions {
namespace {
inline std::string getContentType(velox::functions::remote::PageFormat fmt) {
  using velox::functions::remote::PageFormat;
  return fmt == PageFormat::SPARK_UNSAFE_ROW
      ? remote::CONTENT_TYPE_SPARK_UNSAFE_ROW
      : remote::CONTENT_TYPE_PRESTO_PAGE;
}

} // namespace

std::unique_ptr<folly::IOBuf> RestClient::invokeFunction(
    const std::string& fullUrl,
    std::unique_ptr<folly::IOBuf> requestPayload,
    velox::functions::remote::PageFormat serdeFormat) {
  try {
    folly::Uri uri(fullUrl);
    VELOX_USER_CHECK(
        uri.scheme() == "http" || uri.scheme() == "https",
        "Unsupported URL scheme: {}",
        uri.scheme());

    const std::string host = uri.host();
    const uint16_t port = uri.port();
    const bool secure = (uri.scheme() == "https");

    folly::ScopedEventBaseThread evbThread{"rest-client"};
    proxygen::Endpoint endpoint(host, port, secure);
    folly::SocketAddress addr(host.c_str(), port, true);
    auto memPool =
        memory::MemoryManager::getInstance()->addLeafPool("remoteFunction");
    http::HttpClientConnectionPool connPool;

    auto client = std::make_shared<http::HttpClient>(
        evbThread.getEventBase(),
        &connPool,
        endpoint,
        addr,
        std::chrono::milliseconds{connectionTimeout},
        std::chrono::milliseconds{connectionTimeout},
        memPool,
        nullptr);

    const std::string contentType = getContentType(serdeFormat);

    http::RequestBuilder builder;
    builder.method(proxygen::HTTPMethod::POST)
        .url(uri.path())
        .header("Content-Type", contentType)
        .header("Accept", contentType);

    requestPayload->coalesce();
    std::string requestBody = requestPayload->moveToFbString().toStdString();

    std::unique_ptr<http::HttpResponse> resp;
    try {
      auto sendFuture =
          builder.send(client.get(), requestBody, connectionTimeout);
      resp = std::move(sendFuture).get();
    } catch (const std::exception& ex) {
      VELOX_FAIL(
          "Error communicating with server: ({}:{}) - {}.",
          host,
          port,
          ex.what());
    }

    if (resp->hasError()) {
      VELOX_FAIL("HTTP error: {}", resp->error());
    }
    int status = resp->headers()->getStatusCode();
    if (status < 200 || status >= 300) {
      VELOX_FAIL(
          "Server responded with status {}. Body: '{}'. URL: {}",
          status,
          resp->dumpBodyChain(),
          fullUrl);
    }
    return folly::IOBuf::copyBuffer(resp->dumpBodyChain());
  } catch (const std::exception& ex) {
    VELOX_FAIL("Exception during HTTP request: {}", ex.what());
  }
  return nullptr;
}

std::unique_ptr<RestClient> getRestClient() {
  return std::make_unique<RestClient>();
}

} // namespace facebook::presto::functions
