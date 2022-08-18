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
#include "presto_cpp/main/http/HttpClient.h"
#include <velox/common/base/Exceptions.h>

namespace facebook::presto::http {
HttpClient::HttpClient(
    folly::EventBase* eventBase,
    const folly::SocketAddress& address,
    std::chrono::milliseconds timeout)
    : eventBase_(eventBase),
      address_(address),
      timer_(folly::HHWheelTimer::newTimer(
          eventBase_,
          std::chrono::milliseconds(folly::HHWheelTimer::DEFAULT_TICK_INTERVAL),
          folly::AsyncTimeout::InternalEnum::NORMAL,
          timeout)) {
  sessionPool_ = std::make_unique<proxygen::SessionPool>(nullptr, 10);
}

HttpClient::~HttpClient() {
  if (sessionPool_) {
    // make sure to destroy SessionPool on the EventBase thread
    eventBase_->runImmediatelyOrRunInEventBaseThreadAndWait(
        [this] { sessionPool_.reset(); });
  }
}

HttpResponse::~HttpResponse() {
  // Clear out any leftover iobufs if not consumed.
  for (auto& buf : bodyChain_) {
    if (buf) {
      mappedMemory_->freeBytes(buf->writableData(), buf->length());
    }
  }
}

void HttpResponse::append(std::unique_ptr<folly::IOBuf>&& iobuf) {
  VELOX_CHECK(!iobuf->isChained());
  uint64_t dataLength = iobuf->length();

  void* buf = mappedMemory_->allocateBytes(dataLength);
  if (buf == nullptr) {
    VELOX_FAIL(
        "Cannot spare enough system memory to receive more HTTP response.");
  }
  memcpy(buf, iobuf->data(), dataLength);
  bodyChain_.emplace_back(folly::IOBuf::wrapBuffer(buf, dataLength));
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
  ResponseHandler(const proxygen::HTTPMessage& request, const std::string& body)
      : request_(request), body_(body) {}

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
    response_ = std::make_unique<HttpResponse>(std::move(msg));
  }

  void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override {
    if (chain) {
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
  std::unique_ptr<HttpResponse> response_;
  folly::Promise<std::unique_ptr<HttpResponse>> promise_;
  std::shared_ptr<ResponseHandler> self_;
};

class ConnectionHandler : public proxygen::HTTPConnector::Callback {
 public:
  ConnectionHandler(
      const std::shared_ptr<ResponseHandler>& responseHandler,
      proxygen::SessionPool* sessionPool,
      folly::HHWheelTimer* timer,
      folly::EventBase* eventBase,
      const folly::SocketAddress address)
      : responseHandler_(responseHandler),
        sessionPool_(sessionPool),
        timer_(timer),
        eventBase_(eventBase),
        address_(address) {}

  void connect() {
    connector_ = std::make_unique<proxygen::HTTPConnector>(this, timer_);
    connector_->connect(eventBase_, address_);
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
};

folly::SemiFuture<std::unique_ptr<HttpResponse>> HttpClient::sendRequest(
    const proxygen::HTTPMessage& request,
    const std::string& body) {
  auto responseHandler = std::make_shared<ResponseHandler>(request, body);
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
        address_);

    connectionHandler->connect();
  });

  return future;
}

} // namespace facebook::presto::http
