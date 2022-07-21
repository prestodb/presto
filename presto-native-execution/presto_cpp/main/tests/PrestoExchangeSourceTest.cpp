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
#include <folly/executors/ThreadedExecutor.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/tests/HttpServerWrapper.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"

using namespace facebook::presto;
using namespace facebook::velox;

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

class Producer {
 public:
  void registerEndpoints(http::HttpServer* server) {
    server->registerGet(
        R"(/v1/task/(.*)/results/([0-9]+)/([0-9]+))",
        [this](
            proxygen::HTTPMessage* message,
            const std::vector<std::string>& pathMatch) {
          return getResults(message, pathMatch);
        });
    server->registerGet(
        R"(/v1/task/(.+)/results/([0-9]+)/([0-9]+)/acknowledge)",
        [this](
            proxygen::HTTPMessage* message,
            const std::vector<std::string>& pathMatch) {
          return acknowledgeResults(message, pathMatch);
        });
    server->registerDelete(
        R"(/v1/task/(.+)/results/([0-9]+))",
        [this](
            proxygen::HTTPMessage* message,
            const std::vector<std::string>& pathMatch) {
          return deleteResults(message, pathMatch);
        });
  }

  proxygen::RequestHandler* getResults(
      proxygen::HTTPMessage* /*message*/,
      const std::vector<std::string>& pathMatch) {
    protocol::TaskId taskId = pathMatch[1];
    long sequence = std::stol(pathMatch[3]);

    return new http::CallbackRequestHandler(
        [this, taskId, sequence](
            proxygen::HTTPMessage* /*message*/,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream) {
          auto [data, noMoreData] = getData(sequence);
          if (!data.empty() || noMoreData) {
            sendResponse(downstream, taskId, sequence, data, noMoreData);
          } else {
            auto [promise, future] = folly::makePromiseContract<bool>();

            std::move(future)
                .via(folly::EventBaseManager().getEventBase())
                .thenValue([this, downstream, taskId, sequence](
                               bool /*value*/) {
                  auto [data, noMoreData] = getData(sequence);
                  VELOX_CHECK(!data.empty() || noMoreData);
                  sendResponse(downstream, taskId, sequence, data, noMoreData);
                });

            promise_ = std::move(promise);
          }
        });
  }

  proxygen::RequestHandler* acknowledgeResults(
      proxygen::HTTPMessage* /*message*/,
      const std::vector<std::string>& pathMatch) {
    long sequence = std::stol(pathMatch[3]);
    return new http::CallbackRequestHandler(
        [this, sequence](
            proxygen::HTTPMessage* /*message*/,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream) {
          auto lastAckPromise = folly::Promise<bool>::makeEmpty();
          {
            std::lock_guard<std::mutex> l(mutex_);
            if (sequence > startSequence_) {
              for (int i = startSequence_; i < sequence && !queue_.empty();
                   ++i) {
                queue_.pop_front();
              }
              startSequence_ = sequence;
            }

            if (queue_.empty() && noMoreData_) {
              lastAckPromise = std::move(deleteResultsPromise_);
            }
          }

          proxygen::ResponseBuilder(downstream)
              .status(http::kHttpOk, "OK")
              .sendWithEOM();

          if (lastAckPromise.valid()) {
            lastAckPromise.setValue(true);
          }
        });
  }

  proxygen::RequestHandler* deleteResults(
      proxygen::HTTPMessage* /*message*/,
      const std::vector<std::string>& /*pathMatch*/) {
    return new http::CallbackRequestHandler(
        [this](
            proxygen::HTTPMessage* /*message*/,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream) {
          auto deleteResultsPromise = folly::Promise<bool>::makeEmpty();
          {
            std::lock_guard<std::mutex> l(mutex_);
            queue_.clear();
            receivedDeleteResults_ = true;
            deleteResultsPromise = std::move(deleteResultsPromise_);
          }

          proxygen::ResponseBuilder(downstream)
              .status(http::kHttpOk, "OK")
              .sendWithEOM();

          if (deleteResultsPromise.valid()) {
            deleteResultsPromise.setValue(true);
          }
        });
  }

  void enqueue(const std::string& data) {
    auto promise = folly::Promise<bool>::makeEmpty();
    {
      std::lock_guard<std::mutex> l(mutex_);
      queue_.emplace_back(data);

      if (promise_.valid()) {
        promise = std::move(promise_);
      }
    }

    if (promise.valid()) {
      promise.setValue(true);
    }
  }

  void noMoreData() {
    std::lock_guard<std::mutex> l(mutex_);
    noMoreData_ = true;
  }

  void waitForDeleteResults() {
    folly::SemiFuture<bool> future(false);
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (receivedDeleteResults_) {
        return;
      }

      auto [p, f] = folly::makePromiseContract<bool>();
      deleteResultsPromise_ = std::move(p);
      future = std::move(f);
    }

    std::move(future).get();
  }

 private:
  std::tuple<std::string, bool> getData(int64_t sequence) {
    std::string data;
    bool noMoreData = false;
    {
      std::lock_guard<std::mutex> l(mutex_);
      auto index = sequence - startSequence_;
      VELOX_CHECK_GE(index, 0);
      if (queue_.size() > index) {
        data = queue_[index];
      } else {
        noMoreData = noMoreData_;
      }
    }
    return std::make_tuple(std::move(data), noMoreData);
  }

  void sendResponse(
      proxygen::ResponseHandler* downstream,
      const protocol::TaskId& taskId,
      int64_t sequence,
      const std::string& data,
      bool complete) {
    proxygen::ResponseBuilder builder(downstream);
    builder.status(http::kHttpOk, "OK")
        .header(protocol::PRESTO_TASK_INSTANCE_ID_HEADER, taskId)
        .header(protocol::PRESTO_PAGE_TOKEN_HEADER, std::to_string(sequence))
        .header(
            protocol::PRESTO_PAGE_NEXT_TOKEN_HEADER,
            std::to_string(sequence + 1))
        .header(
            protocol::PRESTO_BUFFER_COMPLETE_HEADER,
            complete ? "true" : "false");
    if (!data.empty()) {
      auto buffer = folly::IOBuf::create(4 + data.size());
      int32_t dataSize = data.size();
      memcpy(
          buffer->writableData(), reinterpret_cast<const char*>(&dataSize), 4);
      memcpy(buffer->writableData() + 4, data.data(), dataSize);
      buffer->append(4 + dataSize);
      builder
          .header(
              proxygen::HTTP_HEADER_CONTENT_TYPE,
              protocol::PRESTO_PAGES_MIME_TYPE)
          .body(std::move(buffer));
    }
    builder.sendWithEOM();
  }

  std::deque<std::string> queue_;
  std::mutex mutex_;
  bool noMoreData_ = false;
  int startSequence_ = 0;
  folly::Promise<bool> promise_ = folly::Promise<bool>::makeEmpty();
  folly::Promise<bool> deleteResultsPromise_ =
      folly::Promise<bool>::makeEmpty();
  bool receivedDeleteResults_ = false;
};

std::string toString(exec::SerializedPage* page) {
  ByteStream input;
  page->prepareStreamForDeserialize(&input);

  auto numBytes = input.read<int32_t>();
  char data[numBytes + 1];
  input.readBytes(data, numBytes);
  data[numBytes] = '\0';
  return std::string(data);
}

std::unique_ptr<exec::SerializedPage> waitForNextPage(
    const std::shared_ptr<exec::ExchangeQueue>& queue) {
  bool atEnd;
  facebook::velox::ContinueFuture future;
  auto page = queue->dequeue(&atEnd, &future);
  EXPECT_FALSE(atEnd);
  if (page == nullptr) {
    std::move(future).get();
    page = queue->dequeue(&atEnd, &future);
    EXPECT_TRUE(page != nullptr);
  }
  return page;
}

void waitForEndMarker(const std::shared_ptr<exec::ExchangeQueue>& queue) {
  bool atEnd;
  facebook::velox::ContinueFuture future;
  auto page = queue->dequeue(&atEnd, &future);
  ASSERT_TRUE(page == nullptr);
  if (!atEnd) {
    std::move(future).get();
    page = queue->dequeue(&atEnd, &future);
    ASSERT_TRUE(page == nullptr);
    ASSERT_TRUE(atEnd);
  }
}

folly::Uri makeProducerUri(const folly::SocketAddress& address) {
  return folly::Uri(fmt::format(
      "http://{}:{}/v1/task/20201007_190402_00000_r5erw.1.0.0/results/3",
      address.getAddressStr(),
      address.getPort()));
}

class PrestoExchangeSourceTest : public testing::Test {
 public:
  void SetUp() override {
    auto& defaultManager =
        memory::MemoryManager<memory::MemoryAllocator, memory::kNoAlignment>::
            getProcessDefaultManager();
    auto& pool =
        dynamic_cast<memory::MemoryPoolImpl<memory::MemoryAllocator, 16>&>(
            defaultManager.getRoot());
    pool_ = &pool;
  }

  void requestNextPage(
      const std::shared_ptr<exec::ExchangeQueue>& queue,
      const std::shared_ptr<exec::ExchangeSource>& exchangeSource) {
    {
      std::lock_guard<std::mutex> l(queue->mutex());
      ASSERT_TRUE(exchangeSource->shouldRequestLocked());
    }
    exchangeSource->request();
  }

  memory::MemoryPool* pool_;
};

TEST_F(PrestoExchangeSourceTest, basic) {
  // Test both with memory pool and without memory pool conditions.
  for (const bool withPool : {true, false}) {
    memory::MemoryPool* pool = withPool ? pool_ : nullptr;

    std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
    auto producer = std::make_unique<Producer>();
    for (auto& page : pages) {
      producer->enqueue(page);
    }
    producer->noMoreData();

    auto producerServer = std::make_unique<http::HttpServer>(
        folly::SocketAddress("127.0.0.1", 0));
    producer->registerEndpoints(producerServer.get());

    test::HttpServerWrapper serverWrapper(std::move(producerServer));
    auto producerAddress = serverWrapper.start().get();
    auto producerUri = makeProducerUri(producerAddress);

    auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
    queue->addSource();
    queue->noMoreSources();

    auto exchangeSource =
        std::make_shared<PrestoExchangeSource>(producerUri, 3, queue);
    exchangeSource->setMemoryPool(pool);

    size_t beforePoolSize = pool_->getCurrentBytes();
    size_t beforeQueueSize = queue->totalBytes();
    requestNextPage(queue, exchangeSource);
    for (int i = 0; i < pages.size(); i++) {
      auto page = waitForNextPage(queue);
      ASSERT_EQ(toString(page.get()), pages[i]) << "at " << i;
      requestNextPage(queue, exchangeSource);
    }
    waitForEndMarker(queue);

    size_t deltaPool = pool_->getCurrentBytes() - beforePoolSize;
    size_t deltaQueue = queue->totalBytes() - beforeQueueSize;
    if (withPool) {
      EXPECT_EQ(deltaPool, deltaQueue);
    } else {
      EXPECT_EQ(deltaPool, 0);
    }

    producer->waitForDeleteResults();
    serverWrapper.stop();
    EXPECT_EQ(pool_->getCurrentBytes(), 0);
  }
}

TEST_F(PrestoExchangeSourceTest, slowProducer) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  auto producer = std::make_unique<Producer>();

  auto producerServer =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSource();
  queue->noMoreSources();
  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue);
  exchangeSource->setMemoryPool(pool_);

  size_t beforePoolSize = pool_->getCurrentBytes();
  size_t beforeQueueSize = queue->totalBytes();
  requestNextPage(queue, exchangeSource);
  for (int i = 0; i < pages.size(); i++) {
    producer->enqueue(pages[i]);
    auto page = waitForNextPage(queue);
    ASSERT_EQ(toString(page.get()), pages[i]) << "at " << i;
    requestNextPage(queue, exchangeSource);
  }
  producer->noMoreData();
  waitForEndMarker(queue);

  size_t deltaPool = pool_->getCurrentBytes() - beforePoolSize;
  size_t deltaQueue = queue->totalBytes() - beforeQueueSize;
  EXPECT_EQ(deltaPool, deltaQueue);

  producer->waitForDeleteResults();
  serverWrapper.stop();
  EXPECT_EQ(pool_->getCurrentBytes(), 0);
}

TEST_F(PrestoExchangeSourceTest, failedProducer) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  auto producer = std::make_unique<Producer>();

  auto producerServer =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = std::make_shared<exec::ExchangeQueue>(1 << 20);
  queue->addSource();
  queue->noMoreSources();
  auto exchangeSource = std::make_shared<PrestoExchangeSource>(
      makeProducerUri(producerAddress), 3, queue);

  requestNextPage(queue, exchangeSource);
  producer->enqueue(pages[0]);

  // Stop server to simulate failed connection.
  serverWrapper.stop();

  EXPECT_THROW(waitForNextPage(queue), std::runtime_error);
}
