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
#include <velox/common/memory/MemoryPool.h>
#include "presto_cpp/main/http/HttpConstants.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::http {

/// NOTE: this class is not thread safe.
class HttpResponse {
 public:
  HttpResponse(
      std::unique_ptr<proxygen::HTTPMessage> headers,
      std::shared_ptr<velox::memory::MemoryPool> pool,
      uint64_t minResponseAllocBytes,
      uint64_t maxResponseAllocBytes);

  ~HttpResponse();

  proxygen::HTTPMessage* headers() {
    return headers_.get();
  }

  /// Appends payload to the body of this HttpResponse.
  void append(std::unique_ptr<folly::IOBuf>&& iobuf);

  /// Indicates if this http response has error occurred. If it has error, we
  /// will skip the rest of http response data processing.
  ///
  /// NOTE: a http client might append the payload more than once if the
  /// response payload is too big.
  bool hasError() const {
    return !error_.empty();
  }

  /// Returns http response error string if error occurred during the http
  /// response data processing.
  ///
  /// NOTE: the error is only set when append() fails to allocate memory.
  const std::string& error() const {
    return error_;
  }

  /// Returns true if the body of this HttpResponse is empty.
  bool empty() const {
    return bodyChain_.empty();
  }

  /// Consumes the response body. The caller is responsible for freeing the
  /// backed memory of this IOBuf from MappedMemory. Otherwise it could lead to
  /// memory leak.
  std::vector<std::unique_ptr<folly::IOBuf>> consumeBody() {
    VELOX_CHECK(!hasError());
    return std::move(bodyChain_);
  }

  /// Consumes the response body. The memory of body will be transferred to the
  /// memory to be allocated from 'pool'.
  std::unique_ptr<folly::IOBuf> consumeBody(velox::memory::MemoryPool* pool);

  void freeBuffers();

  std::string dumpBodyChain() const;

 private:
  // The append operation that copies the 'iobuf' to velox memory 'pool_' and
  // free 'iobuf' immediately.
  void appendWithCopy(std::unique_ptr<folly::IOBuf>&& iobuf);

  // Appends the 'iobuf' to 'bodyChain_', and copies them all once into a single
  // large buffer after receives the entire http response payload.
  void appendWithoutCopy(std::unique_ptr<folly::IOBuf>&& iobuf);

  // Invoked to set the error on the first encountered 'exception'.
  void setError(const std::exception& exception) {
    VELOX_CHECK(!hasError())
    error_ = exception.what();
    freeBuffers();
  }

  // Returns the next buffer allocation size given the new request 'dataLength'.
  FOLLY_ALWAYS_INLINE size_t nextAllocationSize(uint64_t dataLength) const;

  const std::unique_ptr<proxygen::HTTPMessage> headers_;
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  const uint64_t minResponseAllocBytes_;
  const uint64_t maxResponseAllocBytes_;

  std::string error_{};
  std::vector<std::unique_ptr<folly::IOBuf>> bodyChain_;
  size_t bodyChainBytes_{0};
};

// HttpClient uses proxygen::SessionPool which must be destructed on the
// EventBase thread. Hence, the destructor of HttpClient must run on the
// EventBase thread as well. Consider running HttpClient's destructor
// via EventBase::runOnDestruction.
class HttpClient : public std::enable_shared_from_this<HttpClient> {
 public:
  HttpClient(
      folly::EventBase* eventBase,
      proxygen::SessionPool* sessionPool,
      const folly::SocketAddress& address,
      std::chrono::milliseconds transactionTimeout,
      std::chrono::milliseconds connectTimeout,
      std::shared_ptr<velox::memory::MemoryPool> pool,
      const std::string& clientCertAndKeyPath = "",
      const std::string& ciphers = "",
      std::function<void(int)>&& reportOnBodyStatsFunc = nullptr);

  ~HttpClient();

  // TODO Avoid copy by using IOBuf for body
  folly::SemiFuture<std::unique_ptr<HttpResponse>> sendRequest(
      const proxygen::HTTPMessage& request,
      const std::string& body = "",
      int64_t delayMs = 0);

  const std::shared_ptr<velox::memory::MemoryPool>& memoryPool() {
    return pool_;
  }

 private:
  folly::EventBase* const eventBase_;
  const folly::SocketAddress address_;
  const proxygen::WheelTimerInstance transactionTimer_;
  const std::chrono::milliseconds connectTimeout_;
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  // clientCertAndKeyPath_ Points to a file (usually with pem extension) which
  // contains certificate and key concatenated together
  const std::string clientCertAndKeyPath_;
  // List of ciphers (comma separated) client can use. Note that, to communicate
  // successfully with server, client needs to have at least one cipher common
  // with server's cipher list
  const std::string ciphers_;
  const std::function<void(int)> reportOnBodyStatsFunc_;
  const uint64_t maxResponseAllocBytes_;
  proxygen::SessionPool* sessionPool_;
  // Create if sessionPool_ is not received from the contructor.
  std::unique_ptr<proxygen::SessionPool> sessionPoolHolder_;
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

  folly::SemiFuture<std::unique_ptr<HttpResponse>>
  send(HttpClient* client, const std::string& body = "", int64_t delayMs = 0) {
    addJwtIfConfigured();
    header(proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
    headers_.ensureHostHeader();
    return client->sendRequest(headers_, body, delayMs);
  }

 private:
  void addJwtIfConfigured();

  proxygen::HTTPMessage headers_;
};

} // namespace facebook::presto::http
