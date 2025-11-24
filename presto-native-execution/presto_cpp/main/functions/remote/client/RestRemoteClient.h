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

#pragma once

#include <folly/io/async/ScopedEventBaseThread.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "velox/functions/remote/if/gen-cpp2/RemoteFunction_types.h"

namespace facebook::presto::functions::remote::rest {

class RestRemoteClient {
 public:
  RestRemoteClient(const std::string& url);

  ~RestRemoteClient();

  std::unique_ptr<folly::IOBuf> invokeFunction(
      const std::string& fullUrl,
      velox::functions::remote::PageFormat serdeFormat,
      std::unique_ptr<folly::IOBuf> requestPayload) const;

 private:
  const std::string url_;
  std::unique_ptr<folly::ScopedEventBaseThread> evbThread_;
  std::shared_ptr<http::HttpClient> httpClient_;
  std::shared_ptr<velox::memory::MemoryPool> memPool_;

  const std::chrono::milliseconds requestTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeRequestTimeoutMs());

  const std::chrono::milliseconds connectTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeConnectTimeoutMs());
};

using RestRemoteClientPtr = std::shared_ptr<RestRemoteClient>;

} // namespace facebook::presto::functions::remote::rest
