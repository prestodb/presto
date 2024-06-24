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
#include <folly/io/async/EventBaseManager.h>
#include <folly/synchronization/Latch.h>
#include <velox/common/base/Exceptions.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"

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
    std::function<void(int)>&& reportOnBodyStatsFunc)
    : eventBase_(eventBase),
      connPool_(connPool),
      endpoint_(endpoint),
      address_(address),
      transactionTimeout_(transactionTimeout),
      connectTimeout_(connectTimeout),
      pool_(std::move(pool)),
      sslContext_(sslContext),
      reportOnBodyStatsFunc_(std::move(reportOnBodyStatsFunc)),
      maxResponseAllocBytes_(SystemConfig::instance()->httpMaxAllocateBytes()) {
}

HttpClient::~HttpClient() {
  if (sessionPoolHolder_) {
    eventBase_->runInEventBaseThread(
        [sessionPool = std::move(sessionPoolHolder_)] {});
  }
}

HttpResponse::HttpResponse(
    std::unique_ptr<proxygen::HTTPMessage> headers,
    std::shared_ptr<velox::memory::MemoryPool> pool,
    uint64_t minResponseAllocBytes,
    uint64_t maxResponseAllocBytes)
    : headers_(std::move(headers)),
      pool_(std::move(pool)),
      minResponseAllocBytes_(minResponseAllocBytes),
      maxResponseAllocBytes_(maxResponseAllocBytes) {}

HttpResponse::~HttpResponse() {
  // Clear out any leftover iobufs if not consumed.
  freeBuffers();
}

void HttpResponse::appendWithCopy(std::unique_ptr<folly::IOBuf>&& iobuf) {
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK(!iobuf->isChained());
  VELOX_CHECK(!hasError());

  uint64_t dataLength = iobuf->length();
  auto dataStart = iobuf->data();
  if (!bodyChain_.empty()) {
    auto tail = bodyChain_.back().get();
    auto space = tail->tailroom();
    auto copySize = std::min<size_t>(space, dataLength);
    ::memcpy(tail->writableTail(), dataStart, copySize);
    tail->append(copySize);
    dataLength -= copySize;
    if (dataLength == 0) {
      return;
    }
    dataStart += copySize;
  }

  const size_t roundedSize = nextAllocationSize(dataLength);
  void* newBuf{nullptr};
  try {
    newBuf = pool_->allocate(roundedSize);
  } catch (const velox::VeloxException& ex) {
    // NOTE: we need to catch exception and process it later in driver execution
    // context when processing the data response. Otherwise, the presto server
    // process will die.
    setError(ex);
    return;
  }
  VELOX_CHECK_NOT_NULL(newBuf);
  bodyChainBytes_ += roundedSize;
  ::memcpy(newBuf, dataStart, dataLength);
  bodyChain_.emplace_back(folly::IOBuf::wrapBuffer(newBuf, roundedSize));
  bodyChain_.back()->trimEnd(roundedSize - dataLength);
}

void HttpResponse::appendWithoutCopy(std::unique_ptr<folly::IOBuf>&& iobuf) {
  VELOX_CHECK_NULL(pool_);
  VELOX_CHECK(!iobuf->isChained());
  VELOX_CHECK(!hasError());
  bodyChainBytes_ += iobuf->length();
  bodyChain_.emplace_back(std::move(iobuf));
}

void HttpResponse::append(std::unique_ptr<folly::IOBuf>&& iobuf) {
  if (pool_ == nullptr) {
    appendWithoutCopy(std::move(iobuf));
  } else {
    appendWithCopy(std::move(iobuf));
  }
}

std::unique_ptr<folly::IOBuf> HttpResponse::consumeBody(
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NULL(pool_);
  VELOX_CHECK(!hasError());
  uint64_t totalBytes{0};
  for (const auto& iobuf : bodyChain_) {
    VELOX_CHECK(!iobuf->isChained());
    totalBytes += iobuf->length();
  }
  void* newBuf = pool->allocate(totalBytes);
  void* curr = newBuf;
  for (auto& iobuf : bodyChain_) {
    const auto length = iobuf->length();
    ::memcpy(curr, iobuf->data(), length);
    curr = (char*)curr + length;
    iobuf.reset();
  }
  bodyChain_.clear();
  return folly::IOBuf::wrapBuffer(newBuf, totalBytes);
}

void HttpResponse::freeBuffers() {
  if (pool_ != nullptr) {
    for (auto& iobuf : bodyChain_) {
      if (iobuf != nullptr) {
        pool_->free(iobuf->writableData(), iobuf->capacity());
      }
    }
  }
  bodyChain_.clear();
}

FOLLY_ALWAYS_INLINE size_t
HttpResponse::nextAllocationSize(uint64_t dataLength) const {
  const size_t minAllocSize = velox::bits::nextPowerOfTwo(
      velox::bits::roundUp(dataLength, minResponseAllocBytes_));
  return std::max<size_t>(
      minAllocSize,
      std::min<size_t>(
          maxResponseAllocBytes_,
          velox::bits::nextPowerOfTwo(velox::bits::roundUp(
              dataLength + bodyChainBytes_, minResponseAllocBytes_))));
}

std::string HttpResponse::dumpBodyChain() const {
  std::string responseBody;
  if (!bodyChain_.empty()) {
    std::ostringstream oss;
    for (auto& buf : bodyChain_) {
      oss << std::string((const char*)buf->data(), buf->length());
    }
    responseBody = oss.str();
  }
  return responseBody;
}

class ResponseHandler : public proxygen::HTTPTransactionHandler {
 public:
  ResponseHandler(
      const proxygen::HTTPMessage& request,
      uint64_t maxResponseAllocBytes,
      const std::string& body,
      std::function<void(int)> reportOnBodyStatsFunc,
      std::shared_ptr<HttpClient> client)
      : request_(request),
        body_(body),
        reportOnBodyStatsFunc_(std::move(reportOnBodyStatsFunc)),
        minResponseAllocBytes_(
            client->memoryPool() == nullptr
                ? 0
                : velox::memory::AllocationTraits::pageBytes(
                      client->memoryPool()->sizeClasses().front())),
        maxResponseAllocBytes_(
            std::max(minResponseAllocBytes_, maxResponseAllocBytes)),
        client_(std::move(client)) {}

  folly::SemiFuture<std::unique_ptr<HttpResponse>> initialize(
      std::shared_ptr<ResponseHandler> self) {
    self_ = self;
    auto [promise, future] =
        folly::makePromiseContract<std::unique_ptr<HttpResponse>>();
    promise_ = std::move(promise);
    return std::move(future);
  }

  void setTransaction(proxygen::HTTPTransaction* /* txn */) noexcept override {}
  void detachTransaction() noexcept override {
    self_.reset();
  }

  void onHeadersComplete(
      std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override {
    response_ = std::make_unique<HttpResponse>(
        std::move(msg),
        client_->memoryPool(),
        minResponseAllocBytes_,
        maxResponseAllocBytes_);
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
    promise_.setValue(std::move(response_));
  }

  void onUpgrade(proxygen::UpgradeProtocol /* protocol*/) noexcept override {}

  void onError(const proxygen::HTTPException& error) noexcept override {
    promise_.setException(error);
  }

  void onEgressPaused() noexcept override {}

  void onEgressResumed() noexcept override {}

  void onPushedTransaction(
      proxygen::HTTPTransaction* /* pushedTxn */) noexcept override {}

  void connectError(const folly::AsyncSocketException& ex) {
    promise_.setException(ex);
    self_.reset();
  }

  void sendRequest(proxygen::HTTPTransaction* txn) {
    txn->sendHeaders(request_);
    if (!body_.empty()) {
      txn->sendBody(folly::IOBuf::wrapBuffer(body_.c_str(), body_.size()));
    }
    txn->sendEOM();
  }

 private:
  const proxygen::HTTPMessage request_;
  const std::string body_;
  const std::function<void(int)> reportOnBodyStatsFunc_;
  const uint64_t minResponseAllocBytes_;
  const uint64_t maxResponseAllocBytes_;
  std::unique_ptr<HttpResponse> response_;
  folly::Promise<std::unique_ptr<HttpResponse>> promise_;
  std::shared_ptr<ResponseHandler> self_;
  std::shared_ptr<HttpClient> client_;
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
      folly::EventBase* eventBase,
      const folly::SocketAddress& address,
      folly::SSLContextPtr sslContext)
      : responseHandler_(responseHandler),
        sessionPool_(sessionPool),
        transactionTimer_(transactionTimeout),
        connectTimeout_(connectTimeout),
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
    auto txn = session->newTransaction(responseHandler_.get());
    if (txn) {
      responseHandler_->sendRequest(txn);
    }

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
      [self = shared_from_this(), this, handler](
          proxygen::HTTPSessionBase* session) -> proxygen::HTTPTransaction* {
    if (!session) {
      return nullptr;
    }
    VLOG(3) << "Reuse idle connection from different thread to "
            << address_.describe();
    session->attachThreadLocals(
        eventBase_,
        sslContext_,
        proxygen::WheelTimerInstance(transactionTimeout_, eventBase_),
        nullptr,
        [](auto*) {},
        nullptr,
        nullptr);
    sessionPool_->putSession(session);
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
      responseHandler->sendRequest(txn);
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
    const proxygen::HTTPMessage& request,
    const std::string& body,
    int64_t delayMs) {
  auto responseHandler = std::make_shared<ResponseHandler>(
      request,
      maxResponseAllocBytes_,
      body,
      reportOnBodyStatsFunc_,
      shared_from_this());
  auto future = responseHandler->initialize(responseHandler);

  auto sendCb = [this, responseHandler]() mutable {
    sendRequest(std::move(responseHandler));
  };

  if (eventBase_ != nullptr) {
    if (delayMs > 0) {
      // schedule() is expected to be run in the event base thread
      eventBase_->runInEventBaseThread([=]() {
        eventBase_->schedule(sendCb, std::chrono::milliseconds(delayMs));
      });
    } else {
      eventBase_->runInEventBaseThreadAlwaysEnqueue(sendCb);
    }
  }

  return future;
}

void RequestBuilder::addJwtIfConfigured() {
#ifdef PRESTO_ENABLE_JWT
  if (SystemConfig::instance()->internalCommunicationJwtEnabled()) {
    // If JWT was enabled the secret cannot be empty.
    auto secretHash = std::vector<uint8_t>(SHA256_DIGEST_LENGTH);
    folly::ssl::OpenSSLHash::sha256(
        folly::range(secretHash),
        folly::ByteRange(folly::StringPiece(
            SystemConfig::instance()->internalCommunicationSharedSecret())));

    const auto time = std::chrono::system_clock::now();
    const auto token =
        jwt::create<jwt::traits::nlohmann_json>()
            .set_subject(NodeConfig::instance()->nodeId())
            .set_issued_at(time)
            .set_expires_at(
                time +
                std::chrono::seconds{
                    SystemConfig::instance()
                        ->internalCommunicationJwtExpirationSeconds()})
            .sign(jwt::algorithm::hs256{std::string(
                reinterpret_cast<char*>(secretHash.data()),
                secretHash.size())});
    header(kPrestoInternalBearer, token);
  }
#endif // PRESTO_ENABLE_JWT
}

} // namespace facebook::presto::http
