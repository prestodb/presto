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

#include <folly/Uri.h>

#include "presto_cpp/main/http/HttpClient.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/Exchange.h"

namespace facebook::presto {
class PrestoExchangeSource : public velox::exec::ExchangeSource {
 public:
  PrestoExchangeSource(
      const folly::Uri& baseUri,
      int destination,
      std::shared_ptr<velox::exec::ExchangeQueue> queue,
      velox::memory::MemoryPool* pool);

  bool shouldRequestLocked() override;

  static std::unique_ptr<ExchangeSource> createExchangeSource(
      const std::string& url,
      int destination,
      std::shared_ptr<velox::exec::ExchangeQueue> queue,
      velox::memory::MemoryPool* pool);

  void close() override;

 private:
  void request() override;

  void doRequest();

  void processDataResponse(std::unique_ptr<http::HttpResponse> response);

  void processDataError(const std::string& path, const std::string& error);

  void acknowledgeResults(int64_t ackSequence);

  void abortResults();

  // Returns a shared ptr owning the current object.
  std::shared_ptr<PrestoExchangeSource> getSelfPtr();

  const std::string basePath_;
  const std::string host_;
  const uint16_t port_;
  std::unique_ptr<http::HttpClient> httpClient_;
  int failedAttempts_;
  std::atomic_bool closed_{false};
  // A boolean indicating whether abortResults() call was issued and was
  // successfully processed by the remote server.
  std::atomic_bool abortResultsSucceeded_{false};
};
} // namespace facebook::presto
