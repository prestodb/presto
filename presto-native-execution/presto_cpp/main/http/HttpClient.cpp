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
#include <velox/common/base/Exceptions.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"

namespace facebook::presto::http {
HttpClient::HttpClient(
    folly::EventBase* eventBase,
    const folly::SocketAddress& address,
    std::chrono::milliseconds timeout,
    const std::string& clientCertAndKeyPath,
    const std::string& ciphers,
    std::function<void(int)>&& reportOnBodyStatsFunc)
    : eventBase_(eventBase),
      address_(address),
      timer_(folly::HHWheelTimer::newTimer(
          eventBase_,
          std::chrono::milliseconds(folly::HHWheelTimer::DEFAULT_TICK_INTERVAL),
          folly::AsyncTimeout::InternalEnum::NORMAL,
          timeout)),
      clientCertAndKeyPath_(clientCertAndKeyPath),
      ciphers_(ciphers),
      reportOnBodyStatsFunc_(std::move(reportOnBodyStatsFunc)),
      maxResponseAllocBytes_(SystemConfig::instance()->httpMaxAllocateBytes()) {
  sessionPool_ = std::make_unique<proxygen::SessionPool>(nullptr, 10);
  // clientCertAndKeyPath_ and ciphers_ both needed to be set for https. For
  // http, both need to be unset. One set and another is not set is not a valid
  // configuration.
  VELOX_CHECK_EQ(clientCertAndKeyPath_.empty(), ciphers_.empty());
}

HttpClient::~HttpClient() {
  if (sessionPool_) {
    // make sure to destroy SessionPool on the EventBase thread
    eventBase_->runImmediatelyOrRunInEventBaseThreadAndWait(
        [this] { sessionPool_.reset(); });
  }
}

HttpResponse::HttpResponse(
    std::unique_ptr<proxygen::HTTPMessage> headers,
    velox::memory::MemoryPool* pool,
    uint64_t minResponseAllocBytes,
    uint64_t maxResponseAllocBytes)
    : headers_(std::move(headers)),
      pool_(pool),
      minResponseAllocBytes_(minResponseAllocBytes),
      maxResponseAllocBytes_(maxResponseAllocBytes) {
  VELOX_CHECK_NOT_NULL(pool_);
}

HttpResponse::~HttpResponse() {
  // Clear out any leftover iobufs if not consumed.
  freeBuffers();
}

void HttpResponse::append(std::unique_ptr<folly::IOBuf>&& iobuf) {
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

void HttpResponse::freeBuffers() {
  for (auto& iobuf : bodyChain_) {
    if (iobuf != nullptr) {
      pool_->free(iobuf->writableData(), iobuf->capacity());
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
      velox::memory::MemoryPool* pool,
      uint64_t maxResponseAllocBytes,
      const std::string& body,
      std::function<void(int)> reportOnBodyStatsFunc,
      std::shared_ptr<HttpClient> client)
      : request_(request),
        body_(body),
        reportOnBodyStatsFunc_(std::move(reportOnBodyStatsFunc)),
        pool_(pool),
        minResponseAllocBytes_(velox::memory::AllocationTraits::pageBytes(
            pool_->sizeClasses().front())),
        maxResponseAllocBytes_(
            std::max(minResponseAllocBytes_, maxResponseAllocBytes)),
        client_(std::move(client)) {
    VELOX_CHECK_NOT_NULL(pool_);
  }

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
        std::move(msg), pool_, minResponseAllocBytes_, maxResponseAllocBytes_);
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
  velox::memory::MemoryPool* const pool_;
  const uint64_t minResponseAllocBytes_;
  const uint64_t maxResponseAllocBytes_;
  std::unique_ptr<HttpResponse> response_;
  folly::Promise<std::unique_ptr<HttpResponse>> promise_;
  std::shared_ptr<ResponseHandler> self_;
  std::shared_ptr<HttpClient> client_;
};

class ConnectionHandler : public proxygen::HTTPConnector::Callback {
 public:
  ConnectionHandler(
      const std::shared_ptr<ResponseHandler>& responseHandler,
      proxygen::SessionPool* sessionPool,
      folly::HHWheelTimer* timer,
      folly::EventBase* eventBase,
      const folly::SocketAddress& address,
      const std::string& clientCertAndKeyPath,
      const std::string& ciphers)
      : responseHandler_(responseHandler),
        sessionPool_(sessionPool),
        timer_(timer),
        eventBase_(eventBase),
        address_(address),
        clientCertAndKeyPath_(clientCertAndKeyPath),
        ciphers_(ciphers) {}

  bool useHttps() {
    return !(clientCertAndKeyPath_.empty() && ciphers_.empty());
  }

  void connect() {
    connector_ = std::make_unique<proxygen::HTTPConnector>(this, timer_);
    if (useHttps()) {
      auto context = std::make_shared<folly::SSLContext>();
      context->loadCertKeyPairFromFiles(
          clientCertAndKeyPath_.c_str(), clientCertAndKeyPath_.c_str());
      context->setCiphersOrThrow(ciphers_);
      connector_->connectSSL(eventBase_, address_, context);
    } else {
      connector_->connect(eventBase_, address_);
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
  std::shared_ptr<ResponseHandler> responseHandler_;
  proxygen::SessionPool* sessionPool_;
  std::unique_ptr<proxygen::HTTPConnector> connector_;
  folly::HHWheelTimer* timer_;
  folly::EventBase* eventBase_;
  const folly::SocketAddress address_;
  const std::string clientCertAndKeyPath_;
  const std::string ciphers_;
};

folly::SemiFuture<std::unique_ptr<HttpResponse>> HttpClient::sendRequest(
    const proxygen::HTTPMessage& request,
    velox::memory::MemoryPool* pool,
    const std::string& body) {
  auto responseHandler = std::make_shared<ResponseHandler>(
      request,
      pool,
      maxResponseAllocBytes_,
      body,
      reportOnBodyStatsFunc_,
      shared_from_this());
  auto future = responseHandler->initialize(responseHandler);

  eventBase_->runInEventBaseThreadAlwaysEnqueue([this, responseHandler]() {
    auto txn = sessionPool_->getTransaction(responseHandler.get());
    if (txn) {
      responseHandler->sendRequest(txn);
      return;
    }

    auto connectionHandler = new ConnectionHandler(
        responseHandler,
        sessionPool_.get(),
        timer_.get(),
        eventBase_,
        address_,
        clientCertAndKeyPath_,
        ciphers_);

    connectionHandler->connect();
  });

  return future;
}

} // namespace facebook::presto::http
