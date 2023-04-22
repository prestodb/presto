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

#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

namespace facebook::presto::http::filters {

class StatsFilter : public proxygen::Filter {
 public:
  explicit StatsFilter(proxygen::RequestHandler* upstream);

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  std::chrono::steady_clock::time_point startTime_;
};

class StatsFilterFactory : public proxygen::RequestHandlerFactory {
 public:
  explicit StatsFilterFactory() {}

  void onServerStart(folly::EventBase* /*evb*/) noexcept override {}

  void onServerStop() noexcept override {}

  proxygen::RequestHandler* onRequest(
      proxygen::RequestHandler* handler,
      proxygen::HTTPMessage*) noexcept override {
    return new StatsFilter(handler);
  }
};

} // namespace facebook::presto::http::filters
