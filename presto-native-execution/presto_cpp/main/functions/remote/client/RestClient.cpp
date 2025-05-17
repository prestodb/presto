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

#include "RestClient.h"

#include "presto_cpp/main/functions/remote/client/Remote.h"

#include <cpr/cpr.h>
#include <folly/io/IOBufQueue.h>

#include "presto_cpp/main/functions/remote/utils/ContentTypes.h"
#include "velox/common/base/Exceptions.h"

using namespace folly;
using namespace facebook::velox;
namespace facebook::presto::functions {
namespace {
inline std::string getContentType(
    velox::functions::remote::PageFormat serdeFormat) {
  return serdeFormat == velox::functions::remote::PageFormat::SPARK_UNSAFE_ROW
      ? remote::CONTENT_TYPE_SPARK_UNSAFE_ROW
      : remote::CONTENT_TYPE_PRESTO_PAGE;
}
} // namespace

std::unique_ptr<IOBuf> RestClient::invokeFunction(
    const std::string& fullUrl,
    std::unique_ptr<IOBuf> requestPayload,
    velox::functions::remote::PageFormat serdeFormat) {
  IOBufQueue inputBufQueue(IOBufQueue::cacheChainLength());
  inputBufQueue.append(std::move(requestPayload));

  std::string requestBody;
  for (auto range : *inputBufQueue.front()) {
    requestBody.append(
        reinterpret_cast<const char*>(range.data()), range.size());
  }

  std::string contentType = getContentType(serdeFormat);

  cpr::Response response = Post(
      cpr::Url{fullUrl},
      cpr::Header{{"Content-Type", contentType}, {"Accept", contentType}},
      cpr::Body{requestBody});

  if (response.error) {
    VELOX_FAIL(
        fmt::format(
            "Error communicating with server: {} URL: {}",
            response.error.message,
            fullUrl));
  }

  if (response.status_code < 200 || response.status_code >= 300) {
    VELOX_FAIL(
        fmt::format(
            "Server responded with status {}. Message: '{}'. URL: {}",
            response.status_code,
            response.text,
            fullUrl));
  }

  auto outputBuf = IOBuf::copyBuffer(response.text);
  return outputBuf;
}

std::unique_ptr<RestClient> getRestClient() {
  return std::make_unique<RestClient>();
}

} // namespace facebook::presto::functions
