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

class InternalAuthenticationFilter : public proxygen::Filter {
 public:
  explicit InternalAuthenticationFilter(proxygen::RequestHandler* upstream)
      : Filter(upstream), requestRejected_(false) {}

  /// For details on the filter request handling see Filters.h and
  /// RequestHandler.h.

  /// This filter is called to process each stage of the request
  /// processing.
  /// For each stage it has to be decided if the request is passed on or
  /// if it has been rejected. The request can be rejected at the
  /// "onRequest" stage in which case upstream is notified of an error
  /// occurring. Subsequently, the remaining stages for this filter are called.
  /// However, if the request was already rejected nothing is passed through.
  void onRequest(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

  void requestComplete() noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  void sendGenericErrorResponse(void);

  void sendUnauthorizedResponse(void);

  void processAndVerifyJwt(
      const std::string& token,
      std::unique_ptr<proxygen::HTTPMessage> msg);

  bool requestRejected_;
};

class InternalAuthenticationFilterFactory
    : public proxygen::RequestHandlerFactory {
 public:
  explicit InternalAuthenticationFilterFactory() {}

  void onServerStart(folly::EventBase* /*evb*/) noexcept override {}

  void onServerStop() noexcept override {}

  proxygen::RequestHandler* onRequest(
      proxygen::RequestHandler* handler,
      proxygen::HTTPMessage*) noexcept override {
    return new InternalAuthenticationFilter(handler);
  }
};

} // namespace facebook::presto::http::filters
