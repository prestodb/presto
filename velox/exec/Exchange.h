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

#include <memory>
#include "velox/common/memory/ByteStream.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

// Corresponds to Presto SerializedPage, i.e. a container for
// serialize vectors in Presto wire format.
class SerializedPage {
 public:
  static constexpr int kSerializedPageOwner = -11;

  // Construct from a bounded stream
  SerializedPage(
      std::istream* stream,
      uint64_t size,
      memory::MappedMemory* memory);

  ~SerializedPage() = default;

  uint64_t byteSize() const {
    return allocation_.byteSize();
  }

  // Makes 'input' ready for deserializing 'this' with
  // VectorStreamGroup::read().
  void prepareStreamForDeserialize(ByteStream* input);

  static std::unique_ptr<SerializedPage> fromVectorStreamGroup(
      VectorStreamGroup* group) {
    std::stringstream out;
    OutputStreamListener listener;
    OStreamOutputStream outputStream(&out, &listener);
    group->flush(&outputStream);
    return std::make_unique<SerializedPage>(
        &out, out.tellp(), group->mappedMemory());
  }

 private:
  memory::MappedMemory::Allocation allocation_;
  std::vector<ByteRange> ranges_;
};

// Queue of results retrieved from source. Owned by shared_ptr by
// Exchange and client threads and registered callbacks waiting
// for input.
class ExchangeQueue {
 public:
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
    queue_.push_back(std::move(page));
    if (!promises_.empty()) {
      // Resume one of the waiting drivers.
      promises_.back().setValue(true);
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

  std::unique_ptr<SerializedPage> dequeue(bool* atEnd, ContinueFuture* future) {
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
    return page;
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
      promise.setValue(true);
    }
    promises_.clear();
  }

  int numCompleted_ = 0;
  int numSources_ = 0;
  bool noMoreSources_ = false;
  bool atEnd_ = false;
  std::mutex mutex_;
  std::deque<std::unique_ptr<SerializedPage>> queue_;
  std::vector<VeloxPromise<bool>> promises_;
  // When set, all promises will be realized and the next dequeue will
  // throw an exception with this message.
  std::string error_;
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

  // ID of the task producing data
  const std::string taskId_;
  // Destination number of 'this' on producer
  const int destination_;
  int64_t sequence_ = 0;
  std::shared_ptr<ExchangeQueue> queue_;
  bool requestPending_ = false;
  bool atEnd_ = false;
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
  explicit ExchangeClient(int destination)
      : destination_(destination), queue_(std::make_shared<ExchangeQueue>()) {
    VELOX_CHECK(
        destination >= 0,
        "Exchange client destination must be greater than zero, got {}",
        destination);
  }

  ~ExchangeClient();

  void addRemoteTaskId(const std::string& taskId);

  void noMoreRemoteTasks();

  std::shared_ptr<ExchangeQueue> queue() const {
    return queue_;
  }

  std::unique_ptr<SerializedPage> next(bool* atEnd, ContinueFuture* future);

  std::string toString();

 private:
  const int destination_;
  std::shared_ptr<ExchangeQueue> queue_;
  std::unordered_set<std::string> taskIds_;
  std::vector<std::shared_ptr<ExchangeSource>> sources_;
};

class Exchange : public SourceOperator {
 public:
  Exchange(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::ExchangeNode>& exchangeNode,
      std::shared_ptr<ExchangeClient> exchangeClient)
      : SourceOperator(
            ctx,
            exchangeNode->outputType(),
            operatorId,
            exchangeNode->id(),
            "Exchange"),
        planNodeId_(exchangeNode->id()),
        exchangeClient_(std::move(exchangeClient)) {}

  ~Exchange() override {
    close();
  }

  RowVectorPtr getOutput() override;

  void close() override {
    currentPage_ = nullptr;
    result_ = nullptr;
    exchangeClient_ = nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

 private:
  /// Fetches splits from the task until there are no more splits or task
  /// returns a future that will be complete when more splits arrive. Adds
  /// splits to exchangeClient_. Returns true if received a future from the task
  /// and sets the 'future' parameter. Returns false if fetched all splits or if
  /// this operator is not the first operator in the pipeline and therefore is
  /// not responsible for fetching splits and adding them to the
  /// exchangeClient_.
  bool getSplits(ContinueFuture* future);

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
  size_t numSplits_{0}; // Number of splits we took to process so far.
};

} // namespace facebook::velox::exec

#define VELOX_REGISTER_EXCHANGE_SOURCE_METHOD_DEFINITION(class, function) \
  void class ::registerFactory() {                                        \
    facebook::velox::exec::ExchangeSource::registerFactory((function));   \
  }
