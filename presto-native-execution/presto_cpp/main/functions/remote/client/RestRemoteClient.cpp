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

#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"

#include <folly/Uri.h>
#include <proxygen/lib/http/HTTPMessage.h>

#include "presto_cpp/main/functions/remote/utils/ContentTypes.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/expression/EvalCtx.h"
#include "velox/functions/remote/if/GetSerde.h"

using namespace facebook::velox;

namespace facebook::presto::functions {
namespace {
inline std::string getContentType(velox::functions::remote::PageFormat fmt) {
  return fmt == velox::functions::remote::PageFormat::SPARK_UNSAFE_ROW
      ? remote::CONTENT_TYPE_SPARK_UNSAFE_ROW
      : remote::CONTENT_TYPE_PRESTO_PAGE;
}
} // namespace

RestRemoteClient::RestRemoteClient(
    const std::string& url,
    const std::string& functionName,
    RowTypePtr remoteInputType,
    std::vector<std::string> serializedInputTypes,
    const PrestoRemoteFunctionsMetadata& metadata)
    : functionName_(functionName),
      remoteInputType_(std::move(remoteInputType)),
      serializedInputTypes_(std::move(serializedInputTypes)),
      serdeFormat_(metadata.serdeFormat),
      metadata_(metadata),
      serde_(velox::functions::getSerde(serdeFormat_)),
      url_(url) {
  folly::Uri uri(url_);
  VELOX_USER_CHECK(
      uri.scheme() == "http" || uri.scheme() == "https",
      "Unsupported URL scheme: {}",
      uri.scheme());

  const auto& host = uri.host();
  const auto port = uri.port();
  const bool secure = (uri.scheme() == "https");

  evbThread_ = std::make_unique<folly::ScopedEventBaseThread>("rest-client");
  proxygen::Endpoint endpoint(host, port, secure);
  folly::SocketAddress addr(host.c_str(), port, true);
  auto memPool = memory::MemoryManager::getInstance()->addLeafPool();
  static http::HttpClientConnectionPool connPool;

  httpClient_ = std::make_shared<http::HttpClient>(
      evbThread_->getEventBase(),
      &connPool,
      endpoint,
      addr,
      std::chrono::milliseconds{connectionTimeout},
      std::chrono::milliseconds{connectionTimeout},
      memPool,
      nullptr);
}

void RestRemoteClient::applyRemote(
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    const TypePtr& outputType,
    exec::EvalCtx& context,
    VectorPtr& result) const {
  try {
    auto remoteRowVector = std::make_shared<RowVector>(
        context.pool(),
        remoteInputType_,
        BufferPtr{},
        rows.end(),
        std::move(args));

    auto requestBody = std::make_unique<folly::IOBuf>(rowVectorToIOBuf(
        remoteRowVector, rows.end(), *context.pool(), serde_.get()));

    auto responseBody = invokeFunction(
        metadata_.location, std::move(requestBody), metadata_.serdeFormat);

    auto outputRowVector = IOBufToRowVector(
        *responseBody, ROW({outputType}), *context.pool(), serde_.get());

    result = outputRowVector->childAt(0);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Error while executing remote function '{}': {}",
        functionName_,
        e.what());
  }
}

std::unique_ptr<folly::IOBuf> RestRemoteClient::invokeFunction(
    const std::string& fullUrl,
    std::unique_ptr<folly::IOBuf> requestPayload,
    velox::functions::remote::PageFormat serdeFormat) const {
  try {
    folly::Uri uri(url_);
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
          builder.send(httpClient_.get(), requestBody, connectionTimeout);
      resp = std::move(sendFuture).get();
    } catch (const std::exception& ex) {
      VELOX_FAIL(
          "Error communicating with server: ({}:{}) - {}.",
          uri.host(),
          uri.port(),
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

} // namespace facebook::presto::functions
