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
      url_(url) {}

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

    if (!responseBody) {
      VELOX_FAIL("No response received from remote function invocation.");
    }

    auto outputRowVector = IOBufToRowVector(
        *responseBody, ROW({outputType}), *context.pool(), serde_.get());

    result = outputRowVector->childAt(0);
  } catch (const VeloxRuntimeError&) {
    throw;
  }
}

std::unique_ptr<folly::IOBuf> RestRemoteClient::invokeFunction(
    const std::string& fullUrl,
    std::unique_ptr<folly::IOBuf> requestPayload,
    velox::functions::remote::PageFormat serdeFormat) const {
  try {
    folly::Uri uri(url_);
    const std::string contentType = getContentType(serdeFormat);

    auto message = std::make_unique<proxygen::HTTPMessage>();
    message->setMethod(proxygen::HTTPMethod::POST);
    message->setURL(uri.path());
    message->setHTTPVersion(1, 1);
    message->getHeaders().add("Content-Type", contentType);
    message->getHeaders().add("Accept", contentType);

    requestPayload->coalesce();
    std::string requestBody = requestPayload->moveToFbString().toStdString();

    // Create a new HttpClient per request for thread safety
    auto memPool = memory::MemoryManager::getInstance()->addLeafPool();
    proxygen::Endpoint endpoint(
        uri.host(), uri.port(), uri.scheme() == "https");
    folly::SocketAddress addr(uri.host().c_str(), uri.port(), true);

    auto evbThread =
        std::make_unique<folly::ScopedEventBaseThread>("rest-client");
    auto httpClient = std::make_shared<http::HttpClient>(
        evbThread->getEventBase(),
        nullptr,
        endpoint,
        addr,
        requestTimeoutMs,
        connectTimeoutMs,
        memPool,
        nullptr);

    auto sendFuture = httpClient->sendRequest(*message, requestBody);
    sendFuture.wait();

    // Explicitly destroy httpClient inside the event base thread
    if (httpClient) {
      evbThread->getEventBase()->runInEventBaseThreadAndWait(
          [client = std::move(httpClient)]() mutable { client.reset(); });
    }

    evbThread.reset();

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
    if (status < 200 || status >= 300) {
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

} // namespace facebook::presto::functions
