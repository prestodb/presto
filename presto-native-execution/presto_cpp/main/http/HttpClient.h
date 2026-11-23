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
#include <proxygen/lib/http/connpool/ServerIdleSessionController.h>
#include <proxygen/lib/http/connpool/SessionPool.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <velox/common/memory/MemoryPool.h>
#include <mutex>
#include <optional>
#include "presto_cpp/main/http/HttpClientOptions.h" // @manual
#include "presto_cpp/main/http/HttpConstants.h"
#include "presto_cpp/main/http/JwtOptions.h" // @manual
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::http {

/// NOTE: this class is not thread safe.
class HttpResponse {
 public:
  class BodyBuffer;
  using WeakBodyBuffer = std::optional<std::weak_ptr<BodyBuffer>>;

  HttpResponse(
      std::unique_ptr<proxygen::HTTPMessage> headers,
      std::shared_ptr<velox::memory::MemoryPool> pool,
      uint64_t maxResponseAllocBytes,
      WeakBodyBuffer bodyBuffer = std::nullopt);

  proxygen::HTTPMessage* headers() {
    return headers_.get();
  }

  /// Appends payload to the body of this HttpResponse.
  void append(std::unique_ptr<folly::IOBuf>&& iobuf);

  /// Claims the body of a buffered response after HTTP has finished writing
  /// it. Returns false if the buffer was closed or destroyed.
  bool takeBufferedBody();

  /// Indicates if this http response has error occurred. If it has error, we
  /// will skip the rest of http response data processing.
  ///
  /// NOTE: a http client might append the payload more than once if the
  /// response payload is too big.
  bool hasError() const {
    return body_ && !body_->error_.empty();
  }

  /// Returns http response error string if error occurred during the http
  /// response data processing.
  ///
  /// NOTE: the error is only set when append() fails to allocate memory.
  const std::string& error() const {
    return body_->error_;
  }

  /// Returns true if the body of this HttpResponse is empty.
  bool empty() const {
    return body_->chain_.empty();
  }

  /// Consumes the response body. The caller is responsible for freeing the
  /// backed memory of this IOBuf from MappedMemory. Otherwise it could lead to
  /// memory leak.
  std::vector<std::unique_ptr<folly::IOBuf>> consumeBody() {
    VELOX_CHECK(!hasError());
    return std::move(body_->chain_);
  }

  /// Consumes the response body. The memory of body will be transferred to the
  /// memory to be allocated from 'pool'.
  std::unique_ptr<folly::IOBuf> consumeBody(velox::memory::MemoryPool* pool);

  std::string dumpBodyChain() const;

  /// Pooled exchanges keep this buffer alive until the driver claims it.
  /// Closing it frees buffered data while any allocation already underway
  /// finishes on the HTTP thread.
  class BodyBuffer {
   public:
    BodyBuffer(
        std::shared_ptr<velox::memory::MemoryPool> pool,
        uint64_t maxResponseAllocBytes);
    BodyBuffer(BodyBuffer&& other);
    ~BodyBuffer();

    void close();

   private:
    friend class HttpResponse;

    void appendWithCopy(const folly::IOBuf& iobuf);
    void freeBuffers();
    size_t nextAllocationSize(uint64_t dataLength) const;

    std::mutex mutex_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::string error_;
    std::vector<std::unique_ptr<folly::IOBuf>> chain_;
    size_t chainBytes_{0};
    const uint64_t minResponseAllocBytes_;
    const uint64_t maxResponseAllocBytes_;
  };

 private:
  const std::unique_ptr<proxygen::HTTPMessage> headers_;
  std::optional<BodyBuffer> body_;
  const WeakBodyBuffer bodyBuffer_;
};

/// Connection pool shared by all the http clients.  It is held by presto server
/// and should outlive all the http clients, and destroyed before we join the
/// threads backing the event bases.
class HttpClientConnectionPool {
 public:
  ~HttpClientConnectionPool() {
    destroy();
  }

  /// Returns the session pool for a given endpoint and local event base.
  ///
  /// NOTE: this must be called from a thread context with local event base set.
  std::pair<proxygen::SessionPool*, proxygen::ServerIdleSessionController*>
  getSessionPool(const proxygen::Endpoint& endpoint);

  void destroy();

 private:
  // Session pools for a specific endpoint, grouped by their associated event
  // bases.  All the operations on the SessionPool must be performed on the
  // corresponding EventBase.
  struct SessionPools {
    proxygen::ServerIdleSessionController idleSessions;
    folly::Synchronized<folly::F14FastMap<
        folly::EventBase*,
        std::unique_ptr<proxygen::SessionPool>>>
        byEventBase;
  };

  std::pair<proxygen::SessionPool*, proxygen::ServerIdleSessionController*>
  getSessionPoolImpl(SessionPools& endpointPools);

  // The map from http end point to the corresponding session pools.
  folly::Synchronized<folly::F14FastMap<
      proxygen::Endpoint,
      std::unique_ptr<SessionPools>,
      proxygen::EndpointHash,
      proxygen::EndpointEqual>>
      pools_;
};

class ResponseHandler;

class HttpClient : public std::enable_shared_from_this<HttpClient> {
 public:
  HttpClient(
      folly::EventBase* eventBase,
      HttpClientConnectionPool* connPool,
      const proxygen::Endpoint& endpoint,
      const folly::SocketAddress& address,
      std::chrono::milliseconds transactionTimeout,
      std::chrono::milliseconds connectTimeout,
      std::shared_ptr<velox::memory::MemoryPool> pool,
      folly::SSLContextPtr sslContext,
      HttpClientOptions options = {},
      std::function<void(int)>&& reportOnBodyStatsFunc = nullptr);

  ~HttpClient();

  // TODO Avoid copy by using IOBuf for body
  // Responses that use responseBodyBuffer must claim it before reading the
  // body.
  folly::SemiFuture<std::unique_ptr<HttpResponse>> sendRequest(
      proxygen::HTTPMessage& request,
      const std::string& body = "",
      int64_t delayMs = 0,
      HttpResponse::WeakBodyBuffer responseBodyBuffer = std::nullopt);

  std::shared_ptr<HttpResponse::BodyBuffer> makeResponseBodyBuffer(
      const std::shared_ptr<velox::memory::MemoryPool>& pool) const;

  const std::shared_ptr<velox::memory::MemoryPool>& memoryPool() {
    return pool_;
  }

  bool connectionReuseCounterEnabled() const {
    return options_.connectionReuseCounterEnabled;
  }

  static int64_t numConnectionsCreated() {
    return numConnectionsCreated_;
  }

 private:
  void initSessionPool();

  folly::SemiFuture<proxygen::HTTPTransaction*> createTransaction(
      proxygen::HTTPTransactionHandler* handler);

  void sendRequest(std::shared_ptr<ResponseHandler> responseHandler);

  static inline std::atomic_int64_t numConnectionsCreated_ = 0;

  folly::EventBase* const eventBase_;
  HttpClientConnectionPool* const connPool_;
  const proxygen::Endpoint endpoint_;
  const folly::SocketAddress address_;
  const std::chrono::milliseconds transactionTimeout_;
  const std::chrono::milliseconds connectTimeout_;
  const HttpClientOptions options_;
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  const folly::SSLContextPtr sslContext_;
  const std::function<void(int)> reportOnBodyStatsFunc_;

  proxygen::SessionPool* sessionPool_ = nullptr;
  proxygen::ServerIdleSessionController* idleSessions_ = nullptr;

  // Create only if connPool_ is null (disabled).
  std::unique_ptr<proxygen::SessionPool> sessionPoolHolder_;
};

class RequestBuilder {
 public:
  RequestBuilder() {}

  RequestBuilder& jwtOptions(JwtOptions options) & {
    jwtOptions_ = std::move(options);
    return *this;
  }

  RequestBuilder&& jwtOptions(JwtOptions options) && {
    jwtOptions_ = std::move(options);
    return std::move(*this);
  }

  RequestBuilder& method(proxygen::HTTPMethod method) & {
    headers_.setMethod(method);
    return *this;
  }

  RequestBuilder&& method(proxygen::HTTPMethod method) && {
    headers_.setMethod(method);
    return std::move(*this);
  }

  RequestBuilder& url(const std::string& url) & {
    headers_.setURL(url);
    return *this;
  }

  RequestBuilder&& url(const std::string& url) && {
    headers_.setURL(url);
    return std::move(*this);
  }

  RequestBuilder& header(
      proxygen::HTTPHeaderCode code,
      const std::string& value) & {
    headers_.getHeaders().set(code, value);
    return *this;
  }

  RequestBuilder&& header(
      proxygen::HTTPHeaderCode code,
      const std::string& value) && {
    headers_.getHeaders().set(code, value);
    return std::move(*this);
  }

  RequestBuilder& header(
      const std::string& header,
      const std::string& value) & {
    headers_.getHeaders().set(header, value);
    return *this;
  }

  RequestBuilder&& header(
      const std::string& header,
      const std::string& value) && {
    headers_.getHeaders().set(header, value);
    return std::move(*this);
  }

  folly::SemiFuture<std::unique_ptr<HttpResponse>> send(
      HttpClient* client,
      const std::string& body = "",
      int64_t delayMs = 0,
      HttpResponse::WeakBodyBuffer responseBodyBuffer = std::nullopt) {
    addJwtIfConfigured();
    header(proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
    return client->sendRequest(
        headers_, body, delayMs, std::move(responseBodyBuffer));
  }

 private:
  void addJwtIfConfigured();

  JwtOptions jwtOptions_;
  proxygen::HTTPMessage headers_;
};

} // namespace facebook::presto::http
