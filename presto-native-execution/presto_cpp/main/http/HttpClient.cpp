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
#ifdef PRESTO_ENABLE_JWT
#include <folly/ssl/OpenSSLHash.h> // @manual
#include <jwt-cpp/jwt.h> // @manual
#include <jwt-cpp/traits/nlohmann-json/traits.h> //@manual
#endif // PRESTO_ENABLE_JWT
#include <folly/ScopeGuard.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/stop_watch.h>
#include <folly/synchronization/Latch.h>
#include <proxygen/lib/http/codec/CodecProtocol.h>
#include <velox/common/base/Exceptions.h>
#include <chrono>
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::presto::http {

HttpClient::HttpClient(
    folly::EventBase* eventBase,
    HttpClientConnectionPool* connPool,
    const proxygen::Endpoint& endpoint,
    const folly::SocketAddress& address,
    std::chrono::milliseconds transactionTimeout,
    std::chrono::milliseconds connectTimeout,
    std::shared_ptr<velox::memory::MemoryPool> pool,
    folly::SSLContextPtr sslContext,
    HttpClientOptions options,
    std::function<void(int)>&& reportOnBodyStatsFunc)
    : eventBase_(eventBase),
      connPool_(connPool),
      endpoint_(endpoint),
      address_(address),
      transactionTimeout_(transactionTimeout),
      connectTimeout_(connectTimeout),
      options_(std::move(options)),
      pool_(std::move(pool)),
      sslContext_(std::move(sslContext)),
      reportOnBodyStatsFunc_(std::move(reportOnBodyStatsFunc)) {}

HttpClient::~HttpClient() {
  if (sessionPoolHolder_) {
    eventBase_->runInEventBaseThread(
        [sessionPool = std::move(sessionPoolHolder_)] {});
  }
}

HttpResponse::HttpResponse(
    std::unique_ptr<proxygen::HTTPMessage> headers,
    std::shared_ptr<velox::memory::MemoryPool> pool,
    uint64_t maxResponseAllocBytes,
    WeakBodyBuffer bodyBuffer)
    : headers_(std::move(headers)), bodyBuffer_(std::move(bodyBuffer)) {
  if (!bodyBuffer_) {
    body_.emplace(std::move(pool), maxResponseAllocBytes);
  }
}

HttpResponse::BodyBuffer::BodyBuffer(
    std::shared_ptr<velox::memory::MemoryPool> pool,
    uint64_t maxResponseAllocBytes)
    : pool_(std::move(pool)),
      minResponseAllocBytes_(
          pool_ ? velox::memory::AllocationTraits::pageBytes(
                      pool_->sizeClasses().front())
                : 0),
      maxResponseAllocBytes_(
          std::max(minResponseAllocBytes_, maxResponseAllocBytes)) {}

HttpResponse::BodyBuffer::BodyBuffer(BodyBuffer&& other)
    : pool_(std::move(other.pool_)),
      error_(std::move(other.error_)),
      chain_(std::move(other.chain_)),
      chainBytes_(other.chainBytes_),
      minResponseAllocBytes_(other.minResponseAllocBytes_),
      maxResponseAllocBytes_(other.maxResponseAllocBytes_) {}

HttpResponse::BodyBuffer::~BodyBuffer() {
  freeBuffers();
}

void HttpResponse::BodyBuffer::appendWithCopy(const folly::IOBuf& iobuf) {
  std::unique_lock lock(mutex_);
  if (!pool_ || !error_.empty()) {
    return;
  }

  folly::ByteRange data(iobuf.data(), iobuf.length());
  if (!chain_.empty()) {
    auto& tail = chain_.back();
    const auto size = std::min(tail->tailroom(), data.size());
    ::memcpy(tail->writableTail(), data.data(), size);
    tail->append(size);
    data.advance(size);
  }
  if (data.empty()) {
    return;
  }

  // Allocation may enter arbitration. Let close() discard the buffered
  // chunks while this allocation is underway.
  auto pool = pool_;
  const auto capacity = nextAllocationSize(data.size());
  lock.unlock();

  void* memory;
  try {
    memory = pool->allocate(capacity);
    VELOX_CHECK_NOT_NULL(memory);
  } catch (const velox::VeloxException& ex) {
    lock.lock();
    if (pool_) {
      error_ = ex.what();
      freeBuffers();
    }
    return;
  }
  auto release = folly::makeGuard([&] { pool->free(memory, capacity); });
  ::memcpy(memory, data.data(), data.size());
  auto buffer = folly::IOBuf::wrapBuffer(memory, capacity);
  buffer->trimEnd(capacity - data.size());

  lock.lock();
  if (pool_) {
    chain_.push_back(std::move(buffer));
    chainBytes_ += capacity;
    release.dismiss();
  }
}

void HttpResponse::append(std::unique_ptr<folly::IOBuf>&& iobuf) {
  VELOX_CHECK(!iobuf->isChained());
  if (body_) {
    if (body_->pool_) {
      body_->appendWithCopy(*iobuf);
    } else {
      body_->chain_.push_back(std::move(iobuf));
    }
  } else if (auto buffer = bodyBuffer_->lock()) {
    buffer->appendWithCopy(*iobuf);
  }
}

bool HttpResponse::takeBufferedBody() {
  if (!body_) {
    if (auto buffer = bodyBuffer_->lock()) {
      std::lock_guard lock(buffer->mutex_);
      if (buffer->pool_) {
        body_.emplace(std::move(*buffer));
      }
    }
  }
  return body_.has_value();
}

void HttpResponse::BodyBuffer::close() {
  std::lock_guard lock(mutex_);
  freeBuffers();
  pool_.reset();
}

std::unique_ptr<folly::IOBuf> HttpResponse::consumeBody(
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NULL(body_->pool_);
  VELOX_CHECK(!hasError());
  uint64_t totalBytes{0};
  for (const auto& iobuf : body_->chain_) {
    VELOX_CHECK(!iobuf->isChained());
    totalBytes += iobuf->length();
  }
  void* newBuf = pool->allocate(totalBytes);
  void* curr = newBuf;
  for (auto& iobuf : body_->chain_) {
    const auto length = iobuf->length();
    ::memcpy(curr, iobuf->data(), length);
    curr = static_cast<char*>(curr) + length;
    iobuf.reset();
  }
  body_->chain_.clear();
  return folly::IOBuf::wrapBuffer(newBuf, totalBytes);
}

void HttpResponse::BodyBuffer::freeBuffers() {
  if (pool_) {
    for (const auto& iobuf : chain_) {
      if (iobuf != nullptr) {
        pool_->free(iobuf->writableData(), iobuf->capacity());
      }
    }
  }
  chain_.clear();
}

size_t HttpResponse::BodyBuffer::nextAllocationSize(uint64_t dataLength) const {
  const size_t minAllocSize = velox::bits::nextPowerOfTwo(
      velox::bits::roundUp(dataLength, minResponseAllocBytes_));
  return std::max<size_t>(
      minAllocSize,
      std::min<size_t>(
          maxResponseAllocBytes_,
          velox::bits::nextPowerOfTwo(
              velox::bits::roundUp(
                  dataLength + chainBytes_, minResponseAllocBytes_))));
}

std::string HttpResponse::dumpBodyChain() const {
  return empty() ? "Empty response" : util::extractMessageBody(body_->chain_);
}

class ResponseHandler : public proxygen::HTTPTransactionHandler {
 public:
  ResponseHandler(
      const proxygen::HTTPMessage& request,
      uint64_t maxResponseAllocBytes,
      const std::string& body,
      std::function<void(int)> reportOnBodyStatsFunc,
      std::shared_ptr<HttpClient> client,
      HttpResponse::WeakBodyBuffer responseBodyBuffer)
      : request_(request),
        body_(body),
        reportOnBodyStatsFunc_(std::move(reportOnBodyStatsFunc)),
        maxResponseAllocBytes_(maxResponseAllocBytes),
        client_(std::move(client)),
        responseBodyBuffer_(std::move(responseBodyBuffer)) {}

  folly::SemiFuture<std::unique_ptr<HttpResponse>> initialize(
      std::shared_ptr<ResponseHandler> self) {
    self_ = std::move(self);
    return promise_.getSemiFuture();
  }

  void setTransaction(proxygen::HTTPTransaction* txn) noexcept override {
    txn_ = CHECK_NOTNULL(txn);
    protocol_ = txn_->getTransport().getCodec().getProtocol();
    // Track connection usage via sequence number:
    // - seqNo == 0: First request on this connection
    // - seqNo > 0: Connection is being reused for subsequent requests
    // Reuse rate = connection_reuse / (connection_first_use + connection_reuse)
    if (client_->connectionReuseCounterEnabled()) {
      const uint32_t seqNo = txn_->getSequenceNumber();
      if (seqNo > 0) {
        RECORD_METRIC_VALUE(kCounterHttpClientConnectionReuse);
        VLOG(2) << "HttpClient reusing connection, seqNo=" << seqNo;
      } else {
        RECORD_METRIC_VALUE(kCounterHttpClientConnectionFirstUse);
      }
    }
  }

  void detachTransaction() noexcept override {
    txn_ = nullptr;
    self_.reset();
  }

  void onHeadersComplete(
      std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override {
    if (protocol_.has_value()) {
      VLOG(2) << "HttpClient received response of "
              << proxygen::getCodecProtocolString(protocol_.value());
    }
    response_ = std::make_unique<HttpResponse>(
        std::move(msg),
        client_->memoryPool(),
        maxResponseAllocBytes_,
        responseBodyBuffer_);
  }

  void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override {
    if ((chain != nullptr) && !response_->hasError()) {
      if (reportOnBodyStatsFunc_ != nullptr) {
        reportOnBodyStatsFunc_(chain->length());
      }
      response_->append(std::move(chain));
    }
  }

  void onTrailers(
      std::unique_ptr<proxygen::HTTPHeaders> /* trailers */) noexcept override {
  }

  void onEOM() noexcept override {
    if (!promise_.isFulfilled()) {
      promise_.setValue(std::move(response_));
    }
  }

  void onUpgrade(proxygen::UpgradeProtocol /* protocol*/) noexcept override {}

  void onError(const proxygen::HTTPException& error) noexcept override {
    if (!promise_.isFulfilled()) {
      if (responseBodyBuffer_) {
        if (auto buffer = responseBodyBuffer_->lock()) {
          buffer->close();
        }
      }
      promise_.setException(error);
    }
  }

  void onEgressPaused() noexcept override {}

  void onEgressResumed() noexcept override {}

  void connectError(const folly::AsyncSocketException& ex) {
    promise_.setException(ex);
    self_.reset();
  }

  void sendRequest() {
    if (txn_) {
      txn_->sendHeaders(request_);
      if (!body_.empty()) {
        txn_->sendBody(folly::IOBuf::wrapBuffer(body_.c_str(), body_.size()));
      }
      txn_->sendEOM();
    }
  }

 private:
  const proxygen::HTTPMessage request_;
  const std::string body_;
  const std::function<void(int)> reportOnBodyStatsFunc_;
  const uint64_t maxResponseAllocBytes_;
  std::unique_ptr<HttpResponse> response_;
  folly::Promise<std::unique_ptr<HttpResponse>> promise_;
  std::shared_ptr<ResponseHandler> self_;
  std::shared_ptr<HttpClient> client_;
  const HttpResponse::WeakBodyBuffer responseBodyBuffer_;
  std::optional<proxygen::CodecProtocol> protocol_;
  proxygen::HTTPTransaction* txn_{nullptr};
};

// Responsible for making an HTTP request. The request will be made in 2
// phases:
// 1. Connection establishment: An RTT(HTTP) or a series of RTTs(HTTPS) that
// establishes a connection with the server side. This phase will be timed out
// by 'connectTimeout_'
// 2. Transaction: The phase where actual application request is sent to server
// and response received from server. This phase will be timed out by
// 'transactionTimer_', also known as request timeout.
//
// NOTE: HTTP connection establishment on the proxygen::HTTPServer side can be
// handled directly in kernel. Whereas HTTPS connection establishment needs to
// be handled by proxygen::HTTPServer's request handling thread pool. So for
// HTTPS, connections will not be established if server's request handling
// thread pool is fully occupied, resulting in long connect time. This needs to
// be taken into consideration when setting timeouts.
class ConnectionHandler : public proxygen::HTTPConnector::Callback {
 public:
  ConnectionHandler(
      const std::shared_ptr<ResponseHandler>& responseHandler,
      proxygen::SessionPool* sessionPool,
      proxygen::WheelTimerInstance transactionTimeout,
      std::chrono::milliseconds connectTimeout,
      bool http2Enabled,
      uint32_t maxConcurrentStreams,
      uint32_t http2InitialStreamWindow,
      uint32_t http2StreamWindow,
      uint32_t http2SessionWindow,
      folly::EventBase* eventBase,
      const folly::SocketAddress& address,
      folly::SSLContextPtr sslContext)
      : responseHandler_(responseHandler),
        sessionPool_(sessionPool),
        transactionTimer_(std::move(transactionTimeout)),
        connectTimeout_(connectTimeout),
        http2Enabled_(http2Enabled),
        maxConcurrentStreams_(maxConcurrentStreams),
        http2InitialStreamWindow_(http2InitialStreamWindow),
        http2StreamWindow_(http2StreamWindow),
        http2SessionWindow_(http2SessionWindow),
        eventBase_(eventBase),
        address_(address),
        sslContext_(std::move(sslContext)) {}

  bool useHttps() const {
    return sslContext_ != nullptr;
  }

  void connect() {
    connector_ =
        std::make_unique<proxygen::HTTPConnector>(this, transactionTimer_);
    if (useHttps()) {
      connector_->connectSSL(
          eventBase_, address_, sslContext_, nullptr, connectTimeout_);
    } else {
      connector_->connect(eventBase_, address_, connectTimeout_);
    }
  }

  void connectSuccess(proxygen::HTTPUpstreamSession* session) override {
    if (http2Enabled_) {
      session->setFlowControl(
          http2InitialStreamWindow_, http2StreamWindow_, http2SessionWindow_);
      session->setMaxConcurrentOutgoingStreams(maxConcurrentStreams_);
    }
    session->newTransaction(responseHandler_.get());
    responseHandler_->sendRequest();

    sessionPool_->putSession(session);
    delete this;
  }

  void connectError(const folly::AsyncSocketException& ex) override {
    responseHandler_->connectError(ex);
    delete this;
  }

 private:
  const std::shared_ptr<ResponseHandler> responseHandler_;
  proxygen::SessionPool* const sessionPool_;
  // The request processing timer after the connection succeeds.
  const proxygen::WheelTimerInstance transactionTimer_;
  // The connect timeout used to timeout the duration from starting connection
  // to connect success
  const std::chrono::milliseconds connectTimeout_;
  const bool http2Enabled_;
  const uint32_t maxConcurrentStreams_;
  const uint32_t http2InitialStreamWindow_;
  const uint32_t http2StreamWindow_;
  const uint32_t http2SessionWindow_;
  folly::EventBase* const eventBase_;
  const folly::SocketAddress address_;
  const folly::SSLContextPtr sslContext_;
  std::unique_ptr<proxygen::HTTPConnector> connector_;
};

namespace {
// Same value as
// https://github.com/prestodb/presto/blob/831d5947b909fee0d5c0091a3246ddc5b31b2731/presto-main/src/main/java/com/facebook/presto/server/ServerMainModule.java#L547
constexpr int kMaxConnectionsPerServer = 250;
} // namespace

std::pair<proxygen::SessionPool*, proxygen::ServerIdleSessionController*>
HttpClientConnectionPool::getSessionPoolImpl(SessionPools& endpointPools) {
  auto* evb = folly::EventBaseManager::get()->getExistingEventBase();
  VELOX_CHECK_NOT_NULL(evb);
  {
    auto rlock = endpointPools.byEventBase.rlock();
    auto it = rlock->find(evb);
    if (it != rlock->end()) {
      return {it->second.get(), &endpointPools.idleSessions};
    }
  }
  // NOTE: local event base is unique so only 1 thread can access the same key
  // entry.
  auto wlock = endpointPools.byEventBase.wlock();
  auto& pool = (*wlock)[evb];
  VELOX_CHECK_NULL(pool);
  pool = std::make_unique<proxygen::SessionPool>(
      nullptr,
      kMaxConnectionsPerServer,
      std::chrono::seconds(30),
      std::chrono::milliseconds(0),
      nullptr,
      &endpointPools.idleSessions);
  return {pool.get(), &endpointPools.idleSessions};
}

std::pair<proxygen::SessionPool*, proxygen::ServerIdleSessionController*>
HttpClientConnectionPool::getSessionPool(const proxygen::Endpoint& endpoint) {
  {
    auto rlock = pools_.rlock();
    auto it = rlock->find(endpoint);
    if (it != rlock->end()) {
      return getSessionPoolImpl(*it->second);
    }
  }
  auto wlock = pools_.wlock();
  auto it = wlock->find(endpoint);
  if (it != wlock->end()) {
    auto rlock = wlock.moveFromWriteToRead();
    return getSessionPoolImpl(*it->second);
  }
  auto& endpointPools = (*wlock)[endpoint];
  VELOX_CHECK_NULL(endpointPools);
  endpointPools = std::make_unique<SessionPools>();
  endpointPools->idleSessions.setMaxIdleCount(kMaxConnectionsPerServer);
  auto rlock = wlock.moveFromWriteToRead();
  return getSessionPoolImpl(*endpointPools);
}

void HttpClientConnectionPool::destroy() {
  pools_.withWLock([](auto& pools) {
    for (auto& [_, endpointPools] : pools) {
      endpointPools->idleSessions.markForDeath();
      endpointPools->byEventBase.withWLock([](auto& byEventBase) {
        folly::Latch latch(byEventBase.size());
        for (auto& [evb, sessionPool] : byEventBase) {
          evb->runInEventBaseThread(
              [&, sessionPool = std::move(sessionPool)]() mutable {
                sessionPool.reset();
                latch.count_down();
              });
        }
        latch.wait();
      });
    }
    pools.clear();
  });
}

void HttpClient::initSessionPool() {
  eventBase_->dcheckIsInEventBaseThread();
  if (sessionPool_) {
    return;
  }
  if (connPool_) {
    std::tie(sessionPool_, idleSessions_) =
        connPool_->getSessionPool(endpoint_);
    return;
  }
  sessionPoolHolder_ = std::make_unique<proxygen::SessionPool>();
  sessionPool_ = sessionPoolHolder_.get();
  idleSessions_ = nullptr;
}

folly::SemiFuture<proxygen::HTTPTransaction*> HttpClient::createTransaction(
    proxygen::HTTPTransactionHandler* handler) {
  eventBase_->dcheckIsInEventBaseThread();
  if (auto* txn = sessionPool_->getTransaction(handler)) {
    VLOG(3) << "Reuse same thread connection to " << address_.describe();
    return folly::makeSemiFuture(txn);
  }
  if (!idleSessions_) {
    return folly::makeSemiFuture<proxygen::HTTPTransaction*>(nullptr);
  }
  auto idleSessionFuture = idleSessions_->getIdleSession();
  auto getFromIdleSession =
      [self = shared_from_this(),
       this,
       handler,
       watch = folly::stop_watch<std::chrono::milliseconds>()](
          proxygen::HTTPSessionBase* session) -> proxygen::HTTPTransaction* {
    if (!session) {
      return nullptr;
    }
    VLOG(3) << "Reuse idle connection from different thread to "
            << address_.describe();
// Skip this check for GCC to prevent SIGSEGV as described in the issue:
// https://github.com/prestodb/presto/issues/22995
#if defined(__clang__)
    auto* evb = session->getEventBase();
    // The event base from idle session should not be the current event base,
    // otherwise we should have already got it from the local session pool.
    VELOX_CHECK(!evb || !evb->isInEventBaseThread());
#endif
    session->attachThreadLocals(
        eventBase_,
        sslContext_,
        proxygen::WheelTimerInstance(transactionTimeout_, eventBase_),
        nullptr,
        [](auto*) {},
        nullptr,
        nullptr);
    sessionPool_->putSession(session);
    RECORD_HISTOGRAM_METRIC_VALUE(
        kCounterHTTPClientTransactionCreateDelay, watch.elapsed().count());
    return sessionPool_->getTransaction(handler);
  };
  if (idleSessionFuture.isReady()) {
    return getFromIdleSession(idleSessionFuture.value());
  }
  return idleSessionFuture.via(eventBase_)
      .thenValue(std::move(getFromIdleSession))
      .semi();
}

void HttpClient::sendRequest(std::shared_ptr<ResponseHandler> responseHandler) {
  initSessionPool();
  auto txnFuture = createTransaction(responseHandler.get());
  auto doSend = [this, responseHandler = std::move(responseHandler)](
                    proxygen::HTTPTransaction* txn) {
    if (txn) {
      // txn may no longer be valid, ::onError and ::detachTransaction would
      // have been called. In such case, ::sendRequest will be no-op
      responseHandler->sendRequest();
      return;
    }
    VLOG(2) << "Create new connection to " << address_.describe();
    ++numConnectionsCreated_;
    // NOTE: the connection handler object will be self-deleted when the
    // connection succeeds or fails,
    auto connectionHandler = new ConnectionHandler(
        responseHandler,
        sessionPool_,
        proxygen::WheelTimerInstance(transactionTimeout_, eventBase_),
        connectTimeout_,
        options_.http2Enabled,
        options_.http2MaxStreamsPerConnection,
        options_.http2InitialStreamWindow,
        options_.http2StreamWindow,
        options_.http2SessionWindow,
        eventBase_,
        address_,
        sslContext_);
    connectionHandler->connect();
  };
  if (txnFuture.isReady()) {
    doSend(txnFuture.value());
    return;
  }
  std::move(txnFuture).via(eventBase_).thenValue(std::move(doSend));
}

folly::SemiFuture<std::unique_ptr<HttpResponse>> HttpClient::sendRequest(
    proxygen::HTTPMessage& request,
    const std::string& body,
    int64_t delayMs,
    HttpResponse::WeakBodyBuffer responseBodyBuffer) {
  request.setDstAddress(this->address_);
  request.ensureHostHeader();
  auto responseHandler = std::make_shared<ResponseHandler>(
      request,
      options_.maxAllocateBytes,
      body,
      reportOnBodyStatsFunc_,
      shared_from_this(),
      std::move(responseBodyBuffer));
  auto future = responseHandler->initialize(responseHandler);

  auto sendCb = [this, responseHandler]() mutable {
    sendRequest(std::move(responseHandler));
  };

  if (eventBase_ != nullptr) {
    if (delayMs > 0) {
      // schedule() is expected to be run in the event base thread
      eventBase_->runInEventBaseThread([=, this]() {
        eventBase_->schedule(sendCb, std::chrono::milliseconds(delayMs));
      });
    } else {
      eventBase_->runInEventBaseThreadAlwaysEnqueue(sendCb);
    }
  }

  return future;
}

std::shared_ptr<HttpResponse::BodyBuffer> HttpClient::makeResponseBodyBuffer(
    const std::shared_ptr<velox::memory::MemoryPool>& pool) const {
  return std::make_shared<HttpResponse::BodyBuffer>(
      pool, options_.maxAllocateBytes);
}

void RequestBuilder::addJwtIfConfigured() {
#ifdef PRESTO_ENABLE_JWT
  if (jwtOptions_.jwtEnabled) {
    // If JWT was enabled the secret cannot be empty.
    auto secretHash = std::vector<uint8_t>(SHA256_DIGEST_LENGTH);
    folly::ssl::OpenSSLHash::sha256(
        folly::range(secretHash),
        folly::ByteRange(folly::StringPiece(jwtOptions_.sharedSecret)));

    const auto time = std::chrono::system_clock::now();
    const auto token =
        jwt::create<jwt::traits::nlohmann_json>()
            .set_subject(jwtOptions_.nodeId)
            .set_issued_at(time)
            .set_expires_at(
                time + std::chrono::seconds{jwtOptions_.jwtExpirationSeconds})
            .sign(
                jwt::algorithm::hs256{std::string(
                    reinterpret_cast<char*>(secretHash.data()),
                    secretHash.size())});
    header(kPrestoInternalBearer, token);
  }
#endif // PRESTO_ENABLE_JWT
}

} // namespace facebook::presto::http
