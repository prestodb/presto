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
#include <folly/futures/Future.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/connpool/SessionPool.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <velox/common/memory/MappedMemory.h>
#include "presto_cpp/main/http/HttpConstants.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::http {

class HttpResponse {
 public:
  explicit HttpResponse(std::unique_ptr<proxygen::HTTPMessage> headers)
      : headers_(std::move(headers)),
        mappedMemory_(velox::memory::MappedMemory::getInstance()) {}

  ~HttpResponse();

  proxygen::HTTPMessage* headers() {
    return headers_.get();
  }

  // Appends payload to the body of this HttpResponse.
  void append(std::unique_ptr<folly::IOBuf>&& iobuf);

  // Returns true if the body of this HttpResponse is empty.
  bool empty() const {
    return bodyChain_.empty();
  }

  // Consumes the response body. The caller is responsible for freeing the
  // backed memory of this IOBuf from MappedMemory. Otherwise it could lead to
  // memory leak.
  std::vector<std::unique_ptr<folly::IOBuf>> consumeBody() {
    return std::move(bodyChain_);
  }

  velox::memory::MappedMemory* FOLLY_NONNULL mappedMemory() {
    return mappedMemory_;
  }

  std::string dumpBodyChain() const;

 private:
  const std::unique_ptr<proxygen::HTTPMessage> headers_;
  velox::memory::MappedMemory* FOLLY_NONNULL const mappedMemory_;

  std::vector<std::unique_ptr<folly::IOBuf>> bodyChain_;
};

// HttpClient uses proxygen::SessionPool which must be destructed on the
// EventBase thread. Hence, the destructor of HttpClient must run on the
// EventBase thread as well. Consider running HttpClient's destructor
// via EventBase::runOnDestruction.
class HttpClient {
 public:
  HttpClient(
      folly::EventBase* eventBase,
      const folly::SocketAddress& address,
      std::chrono::milliseconds timeout);

  ~HttpClient();

  // TODO Avoid copy by using IOBuf for body
  folly::SemiFuture<std::unique_ptr<HttpResponse>> sendRequest(
      const proxygen::HTTPMessage& request,
      const std::string& body = "");

 private:
  folly::EventBase* eventBase_;
  const folly::SocketAddress address_;
  const folly::HHWheelTimer::UniquePtr timer_;
  std::unique_ptr<proxygen::SessionPool> sessionPool_;
};

class RequestBuilder {
 public:
  RequestBuilder() {
    headers_.setHTTPVersion(1, 1);
  }

  RequestBuilder& method(proxygen::HTTPMethod method) {
    headers_.setMethod(method);
    return *this;
  }

  RequestBuilder& url(const std::string& url) {
    headers_.setURL(url);
    return *this;
  }

  RequestBuilder& header(
      proxygen::HTTPHeaderCode code,
      const std::string& value) {
    headers_.getHeaders().set(code, value);
    return *this;
  }

  RequestBuilder& header(const std::string& header, const std::string& value) {
    headers_.getHeaders().set(header, value);
    return *this;
  }

  folly::SemiFuture<std::unique_ptr<HttpResponse>> send(
      HttpClient* client,
      const std::string& body = "") {
    header(proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
    headers_.ensureHostHeader();
    return client->sendRequest(headers_, body);
  }

 private:
  proxygen::HTTPMessage headers_;
};

} // namespace facebook::presto::http
