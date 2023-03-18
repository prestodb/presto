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

#include <fmt/format.h>
#include <glog/logging.h>
#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <time.h>

namespace facebook::presto::http::filters {

/// A filter that does access logging in nginx `combined` format
class AccessLogFilter : public proxygen::Filter {
 public:
  explicit AccessLogFilter(proxygen::RequestHandler* upstream);

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

  void sendHeaders(proxygen::HTTPMessage& msg) noexcept override;

  void sendBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

 private:
  std::string getVersion(const proxygen::HTTPMessage& msg) const noexcept;

  virtual void writeLog(std::string logLine) const noexcept;

  std::string generateLog() const noexcept;

  proxygen::TimePoint startTime_;
  std::string method_;
  std::string url_;
  std::string version_;
  std::string remoteAddr_;

  uint16_t statusCode_{0};
  size_t bytesSent_{0};

  std::string httpReferer_;
  std::string httpUserAgent_;
};

class AccessLogFilterFactory : public proxygen::RequestHandlerFactory {
 public:
  explicit AccessLogFilterFactory() {}

  void onServerStart(folly::EventBase* /*evb*/) noexcept override {}

  void onServerStop() noexcept override {}

  proxygen::RequestHandler* onRequest(
      proxygen::RequestHandler* handler,
      proxygen::HTTPMessage*) noexcept override {
    return new AccessLogFilter(handler);
  }

 private:
};

} // namespace facebook::presto::http::filters
