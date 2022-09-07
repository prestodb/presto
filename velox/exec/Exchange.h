/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <velox/common/memory/MappedMemory.h>
#include <velox/common/memory/Memory.h>
#include <memory>
#include "velox/common/memory/ByteStream.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

// Corresponds to Presto SerializedPage, i.e. a container for
// serialize vectors in Presto wire format.
class SerializedPage {
 public:
  static constexpr int kSerializedPageOwner = -11;

  // Construct from IOBuf chain. The external memory usage of 'iobuf' will be
  // tracked if 'pool' is not null.
  //
  // TODO: consider to enforce setting memory pool if possible.
  explicit SerializedPage(
      std::unique_ptr<folly::IOBuf> iobuf,
      memory::MemoryPool* FOLLY_NULLABLE pool = nullptr,
      std::function<void(folly::IOBuf&)> onDestructionCb = nullptr);

  ~SerializedPage();

  // Returns the size of the serialized data in bytes.
  uint64_t size() const {
    return iobufBytes_;
  }

  // Makes 'input' ready for deserializing 'this' with
  // VectorStreamGroup::read().
  void prepareStreamForDeserialize(ByteStream* FOLLY_NONNULL input);

  std::unique_ptr<folly::IOBuf> getIOBuf() const {
    return iobuf_->clone();
  }

 private:
  static int64_t chainBytes(folly::IOBuf& iobuf) {
    int64_t size = 0;
    for (auto& range : iobuf) {
      size += range.size();
    }
    return size;
  }

  // Buffers containing the serialized data. The memory is owned by 'iobuf_'.
  std::vector<ByteRange> ranges_;

  // IOBuf holding the data in 'ranges_.
  std::unique_ptr<folly::IOBuf> iobuf_;

  // Number of payload bytes in 'iobuf_'.
  const int64_t iobufBytes_;
  memory::MemoryPool* FOLLY_NULLABLE pool_;

  // Callback that will be called on destruction of the SerializedPage,
  // primarily used to free externally allocated memory backing folly::IOBuf
  // from caller. Caller is responsible to pass in proper cleanup logic to
  // prevent any memory leak.
  std::function<void(folly::IOBuf&)> onDestructionCb_;
};

// Queue of results retrieved from source. Owned by shared_ptr by
// Exchange and client threads and registered callbacks waiting
// for input.
class ExchangeQueue {
 public:
  explicit ExchangeQueue(int64_t minBytes) : minBytes_(minBytes) {}

  ~ExchangeQueue() {
    std::lock_guard<std::mutex> l(mutex_);
    clearAllPromises();
  }

  std::mutex& mutex() {
    return mutex_;
  }

  bool empty() const {
    return queue_.empty();
  }

  void enqueue(std::unique_ptr<SerializedPage>&& page) {
    if (!page) {
      ++numCompleted_;
      checkComplete();
      return;
    }
    totalBytes_ += page->size();
    queue_.push_back(std::move(page));
    if (!promises_.empty()) {
      // Resume one of the waiting drivers.
      promises_.back().setValue();
      promises_.pop_back();
    }
  }

  // If data is permanently not available, e.g. the source cannot be
  // contacted, this registers an error message and causes the reading
  // Exchanges to throw with the message
  void setErrorLocked(const std::string& error) {
    if (!error_.empty()) {
      return;
    }
    error_ = error;
    atEnd_ = true;
    clearAllPromises();
  }

  std::unique_ptr<SerializedPage> dequeue(
      bool* FOLLY_NONNULL atEnd,
      ContinueFuture* FOLLY_NONNULL future) {
    VELOX_CHECK(future);
    if (!error_.empty()) {
      *atEnd = true;
      throw std::runtime_error(error_);
    }
    if (queue_.empty()) {
      if (atEnd_) {
        *atEnd = true;
      } else {
        promises_.emplace_back("ExchangeQueue::dequeue");
        *future = promises_.back().getSemiFuture();
        *atEnd = false;
      }
      return nullptr;
    }
    auto page = std::move(queue_.front());
    queue_.pop_front();
    *atEnd = false;
    totalBytes_ -= page->size();
    return page;
  }

  // Returns the total bytes held by SerializedPages in 'this'.
  uint64_t totalBytes() const {
    return totalBytes_;
  }

  // Returns the target size for totalBytes(). An exchange client
  // should not fetch more data until the queue totalBytes() is below
  // minBytes().
  uint64_t minBytes() const {
    return minBytes_;
  }

  void addSource() {
    VELOX_CHECK(!noMoreSources_, "addSource called after noMoreSources");
    numSources_++;
  }

  void noMoreSources() {
    noMoreSources_ = true;
    checkComplete();
  }

 private:
  void checkComplete() {
    if (noMoreSources_ && numCompleted_ == numSources_) {
      atEnd_ = true;
      clearAllPromises();
    }
  }

  void clearAllPromises() {
    for (auto& promise : promises_) {
      promise.setValue();
    }
    promises_.clear();
  }

  int numCompleted_ = 0;
  int numSources_ = 0;
  bool noMoreSources_ = false;
  bool atEnd_ = false;
  std::mutex mutex_;
  std::deque<std::unique_ptr<SerializedPage>> queue_;
  std::vector<ContinuePromise> promises_;
  // When set, all promises will be realized and the next dequeue will
  // throw an exception with this message.
  std::string error_;
  // Total size of SerializedPages in queue.
  uint64_t totalBytes_{0};

  // If 'totalBytes_' < 'minBytes_', an exchange should request more data from
  // producers.
  uint64_t minBytes_;
};

class ExchangeSource : public std::enable_shared_from_this<ExchangeSource> {
 public:
  using Factory = std::function<std::shared_ptr<ExchangeSource>(
      const std::string& taskId,
      int destination,
      std::shared_ptr<ExchangeQueue> queue)>;

  ExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<ExchangeQueue> queue)
      : taskId_(taskId), destination_(destination), queue_(std::move(queue)) {}

  virtual ~ExchangeSource() = default;

  static std::shared_ptr<ExchangeSource> create(
      const std::string& taskId,
      int destination,
      std::shared_ptr<ExchangeQueue> queue);

  // Returns true if there is no request to the source pending or if
  // this should be retried. If true, the caller is expected to call
  // request(). This is expected to be called while holding lock over
  // queue_.mutex(). This sets the status of 'this' to be pending. The
  // caller is thus expected to call request() without holding a lock over
  // queue_.mutex(). This pattern prevents multiple exchange consumer
  // threads from issuing the same request.
  virtual bool shouldRequestLocked() = 0;

  // Requests the producer to generate more data. Call only if shouldRequest()
  // was true. The object handles its own lifetime by acquiring a
  // shared_from_this() pointer if needed.
  virtual void request() = 0;

  // Close the exchange source. May be called before all data
  // has been received and proessed. This can happen in case
  // of an error or an operator like Limit aborting the query
  // once it received enough data.
  virtual void close() = 0;

  virtual std::string toString() {
    std::stringstream out;
    out << "[ExchangeSource " << taskId_ << ":" << destination_
        << (requestPending_ ? " pending " : "") << (atEnd_ ? " at end" : "");
    return out.str();
  }

  static void registerFactory();

  static bool registerFactory(Factory factory) {
    factories().push_back(factory);
    return true;
  }

  static std::vector<Factory>& factories();

  void setMemoryPool(memory::MemoryPool* FOLLY_NULLABLE pool);
  // ID of the task producing data
  const std::string taskId_;
  // Destination number of 'this' on producer
  const int destination_;
  int64_t sequence_ = 0;
  std::shared_ptr<ExchangeQueue> queue_;
  bool requestPending_ = false;
  bool atEnd_ = false;

 protected:
  memory::MemoryPool* FOLLY_NULLABLE pool_{nullptr};
};

struct RemoteConnectorSplit : public connector::ConnectorSplit {
  const std::string taskId;

  explicit RemoteConnectorSplit(const std::string& t, int32_t groupId = -1)
      : ConnectorSplit(""), taskId(t) {}
};

// Handle for a set of producers. This may be shared by multiple Exchanges, one
// per consumer thread.
class ExchangeClient {
 public:
  static constexpr int32_t kDefaultMinSize = 32 << 20; // 32 MB.

  explicit ExchangeClient(int destination, int64_t minSize = kDefaultMinSize)
      : destination_(destination),
        queue_(std::make_shared<ExchangeQueue>(minSize)) {
    VELOX_CHECK(
        destination >= 0,
        "Exchange client destination must be greater than zero, got {}",
        destination);
  }

  ~ExchangeClient();

  memory::MemoryPool* FOLLY_NULLABLE pool() const {
    return pool_;
  }

  void maybeSetMemoryPool(memory::MemoryPool* FOLLY_NONNULL pool);

  // Creates an exchange source and starts fetching data from the specified
  // upstream task. If 'close' has been called already, creates an exchange
  // source and immediately closes it to notify the upstream task that data is
  // no longer needed. Repeated calls with the same 'taskId' are ignored.
  void addRemoteTaskId(const std::string& taskId);

  void noMoreRemoteTasks();

  // Closes exchange sources.
  void close();

  std::shared_ptr<ExchangeQueue> queue() const {
    return queue_;
  }

  std::unique_ptr<SerializedPage> next(
      bool* FOLLY_NONNULL atEnd,
      ContinueFuture* FOLLY_NONNULL future);

  std::string toString();

 private:
  const int destination_;
  std::shared_ptr<ExchangeQueue> queue_;
  std::unordered_set<std::string> taskIds_;
  std::vector<std::shared_ptr<ExchangeSource>> sources_;
  memory::MemoryPool* FOLLY_NULLABLE pool_{nullptr};
  bool closed_{false};
};

class Exchange : public SourceOperator {
 public:
  Exchange(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const core::ExchangeNode>& exchangeNode,
      std::shared_ptr<ExchangeClient> exchangeClient)
      : SourceOperator(
            ctx,
            exchangeNode->outputType(),
            operatorId,
            exchangeNode->id(),
            "Exchange"),
        planNodeId_(exchangeNode->id()),
        exchangeClient_(std::move(exchangeClient)) {
    exchangeClient_->maybeSetMemoryPool(operatorCtx_->pool());
  }

  ~Exchange() override {
    close();
  }

  RowVectorPtr getOutput() override;

  void close() override {
    SourceOperator::close();
    currentPage_ = nullptr;
    result_ = nullptr;
    if (exchangeClient_) {
      exchangeClient_->close();
    }
    exchangeClient_ = nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* FOLLY_NONNULL future) override;

  bool isFinished() override;

 private:
  /// Fetches splits from the task until there are no more splits or task
  /// returns a future that will be complete when more splits arrive. Adds
  /// splits to exchangeClient_. Returns true if received a future from the task
  /// and sets the 'future' parameter. Returns false if fetched all splits or if
  /// this operator is not the first operator in the pipeline and therefore is
  /// not responsible for fetching splits and adding them to the
  /// exchangeClient_.
  bool getSplits(ContinueFuture* FOLLY_NONNULL future);

  const core::PlanNodeId planNodeId_;
  bool noMoreSplits_ = false;

  /// A future received from Task::getSplitOrFuture(). It will be complete when
  /// there are more splits available or no-more-splits signal has arrived.
  ContinueFuture splitFuture_{ContinueFuture::makeEmpty()};

  RowVectorPtr result_;
  std::shared_ptr<ExchangeClient> exchangeClient_;
  std::unique_ptr<SerializedPage> currentPage_;
  std::unique_ptr<ByteStream> inputStream_;
  bool atEnd_{false};
};

} // namespace facebook::velox::exec

#define VELOX_REGISTER_EXCHANGE_SOURCE_METHOD_DEFINITION(class, function) \
  void class ::registerFactory() {                                        \
    facebook::velox::exec::ExchangeSource::registerFactory((function));   \
  }
