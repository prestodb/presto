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

#include <fmt/format.h>
#include <glog/logging.h>
#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <string>

#include "presto_cpp/main/http/filters/AccessLogFilter.h"

namespace facebook::presto::http::filters {

AccessLogFilter::AccessLogFilter(proxygen::RequestHandler* upstream)
    : Filter(upstream) {}

void AccessLogFilter::onRequest(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  startTime_ = msg->getStartTime();
  method_ = msg->getMethodString();
  url_ = msg->getURL();
  version_ = getVersion(*msg);
  remoteAddr_ = msg->getClientIP();

  const auto& headers = msg->getHeaders();
  httpReferer_ = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_REFERER);
  httpUserAgent_ = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_USER_AGENT);

  Filter::onRequest(std::move(msg));
}

void AccessLogFilter::requestComplete() noexcept {
  writeLog(generateLog());
  Filter::requestComplete();
}

void AccessLogFilter::onError(proxygen::ProxygenError err) noexcept {
  writeLog(generateLog());
  Filter::onError(err);
}

// Response handler
void AccessLogFilter::sendHeaders(proxygen::HTTPMessage& msg) noexcept {
  statusCode_ = msg.getStatusCode();
  Filter::sendHeaders(msg);
}

void AccessLogFilter::sendBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  bytesSent_ += body->computeChainDataLength();
  Filter::sendBody(std::move(body));
}

std::string AccessLogFilter::getVersion(
    const proxygen::HTTPMessage& msg) const noexcept {
  if (!msg.isAdvancedProto()) {
    return "HTTP/" + msg.getVersionString();
  } else {
    return *msg.getAdvancedProtocolString();
  }
}

void AccessLogFilter::writeLog(std::string logLine) const noexcept {
  LOG(INFO) << logLine;
}

std::string AccessLogFilter::generateLog() const noexcept {
  std::time_t requestStartTime = proxygen::toTimeT(proxygen::getCurrentTime());
  static struct tm formattedStartTime;
  localtime_r(&requestStartTime, &formattedStartTime);
  char timeBuf[64];
  std::strftime(timeBuf, 64, "%F %T", &formattedStartTime);
  const auto latency = proxygen::millisecondsSince(startTime_);

  std::string logLine = fmt::format(
      "{} - - [{}] \"{} {} {}\" {:d} {:d} {} {} {:d}",
      remoteAddr_.c_str(),
      timeBuf,
      method_.c_str(),
      url_.c_str(),
      version_.c_str(),
      statusCode_,
      bytesSent_,
      httpReferer_.c_str(),
      httpUserAgent_.c_str(),
      latency.count());

  return logLine;
}

} // namespace facebook::presto::http::filters
