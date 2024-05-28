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
#include <folly/init/Init.h>
#include <folly/portability/GMock.h>
#include <gtest/gtest.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include "folly/experimental/EventCount.h"
#include "presto_cpp/main/PrestoExchangeSource.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/tests/HttpServerWrapper.h"
#include "presto_cpp/main/tests/MultableConfigs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/ExchangeQueue.h"

DECLARE_bool(velox_memory_leak_check_enabled);

namespace fs = boost::filesystem;
using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::memory;
using namespace facebook::velox::common::testutil;
using namespace testing;

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv};
  FLAGS_velox_memory_leak_check_enabled = true;
  return RUN_ALL_TESTS();
}

namespace {
std::string getCertsPath(const std::string& fileName) {
  std::string currentPath = fs::current_path().c_str();
  if (boost::algorithm::ends_with(currentPath, "fbcode")) {
    return currentPath +
        "/github/presto-trunk/presto-native-execution/presto_cpp/main/tests/certs/" +
        fileName;
  }

  // CLion runs the tests from cmake-build-release/ or cmake-build-debug/
  // directory. Hard-coded json files are not copied there and test fails with
  // file not found. Fixing the path so that we can trigger these tests from
  // CLion.
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");

  return currentPath + "/certs/" + fileName;
}

class Producer {
 public:
  explicit Producer(
      std::function<bool(bool)> shouldFail = [](bool) { return false; })
      : shouldFail_(std::move(shouldFail)) {}

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
            proxygen::HTTPMessage* message,
            const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
            proxygen::ResponseHandler* downstream) {
          if (shouldFail_(false)) {
            return sendErrorResponse(
                downstream, "ERR\nConnection reset by peer", 500);
          }
          if (sequence < this->startSequence_) {
            return sendResponse(downstream, taskId, sequence, "", false);
          }
          auto [data, noMoreData] = getData(sequence);
          if (!data.empty() || noMoreData) {
            sendResponse(downstream, taskId, sequence, data, noMoreData);
          } else {
            auto [promise, future] = folly::makePromiseContract<bool>();

            std::move(future)
                .via(folly::EventBaseManager::get()->getEventBase())
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
          if (shouldFail_(true)) {
            return sendErrorResponse(
                downstream, "ERR\nConnection reset by peer", 500);
          }
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

  folly::Promise<bool>& promise() {
    return promise_;
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

  void sendErrorResponse(
      proxygen::ResponseHandler* downstream,
      const std::string& error,
      uint16_t status) {
    proxygen::ResponseBuilder(downstream)
        .status(status, "ERR")
        .body(error)
        .sendWithEOM();
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
  std::function<bool(bool)> shouldFail_;
};

std::string toString(exec::SerializedPage* page) {
  auto input = page->prepareStreamForDeserialize();

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
  auto pages = queue->dequeueLocked(1, &atEnd, &future);
  EXPECT_LE(pages.size(), 1);
  EXPECT_FALSE(atEnd);
  if (pages.empty()) {
    std::move(future).get();
    pages = queue->dequeueLocked(1, &atEnd, &future);
    EXPECT_EQ(pages.size(), 1);
  }
  return std::move(pages.front());
}

void waitForEndMarker(const std::shared_ptr<exec::ExchangeQueue>& queue) {
  bool atEnd;
  facebook::velox::ContinueFuture future;
  auto pages = queue->dequeueLocked(1, &atEnd, &future);
  ASSERT_TRUE(pages.empty());
  if (!atEnd) {
    std::move(future).get();
    pages = queue->dequeueLocked(1, &atEnd, &future);
    ASSERT_TRUE(pages.empty());
    ASSERT_TRUE(atEnd);
  }
}

static std::unique_ptr<http::HttpServer> createHttpServer(
    bool useHttps,
    std::shared_ptr<folly::IOThreadPoolExecutor> ioPool =
        std::make_shared<folly::IOThreadPoolExecutor>(8)) {
  if (useHttps) {
    std::string certPath = getCertsPath("test_cert1.pem");
    std::string keyPath = getCertsPath("test_key1.pem");
    std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
    auto httpsConfig = std::make_unique<http::HttpsConfig>(
        folly::SocketAddress("127.0.0.1", 0), certPath, keyPath, ciphers);
    return std::make_unique<http::HttpServer>(
        ioPool, nullptr, std::move(httpsConfig));
  } else {
    return std::make_unique<http::HttpServer>(
        ioPool,
        std::make_unique<http::HttpConfig>(
            folly::SocketAddress("127.0.0.1", 0)));
  }
}

std::string makeProducerUri(
    const folly::SocketAddress& address,
    bool useHttps) {
  std::string protocol = useHttps ? "https" : "http";
  return fmt::format(
      "{}://{}:{}/v1/task/20201007_190402_00000_r5erw.1.0.0/results/3",
      protocol,
      address.getAddressStr(),
      address.getPort());
}

struct Params {
  bool useHttps;
  bool immediateBufferTransfer;
  int exchangeCpuThreadPoolSize;
  int exchangeIoThreadPoolSize;
};

} // namespace

class PrestoExchangeSourceTest : public ::testing::TestWithParam<Params> {
 public:
  static void SetUpTestCase() {
    MemoryManagerOptions options;
    options.allocatorCapacity = 1L << 30;
    options.useMmapAllocator = true;
    MemoryManager::testingSetInstance(options);
  }

  void SetUp() override {
    pool_ = memory::deprecatedAddDefaultLeafMemoryPool();

    exchangeCpuExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
        GetParam().exchangeCpuThreadPoolSize);
    exchangeIoExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(
        GetParam().exchangeIoThreadPoolSize);
    TestValue::enable();

    filesystems::registerLocalFileSystem();
    test::setupMutableSystemConfig();
    SystemConfig::instance()->setValue(
        std::string(SystemConfig::kExchangeImmediateBufferTransfer),
        GetParam().immediateBufferTransfer ? "true" : "false");
    const std::string keyPath = getCertsPath("client_ca.pem");
    const std::string ciphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
    sslContext_ = facebook::presto::util::createSSLContext(keyPath, ciphers);
  }

  void TearDown() override {
    TestValue::disable();
  }

  std::shared_ptr<exec::ExchangeQueue> makeSingleSourceQueue() {
    auto queue = std::make_shared<exec::ExchangeQueue>();
    queue->addSourceLocked();
    queue->noMoreSources();
    return queue;
  }

  std::shared_ptr<PrestoExchangeSource> makeExchangeSource(
      const folly::SocketAddress& producerAddress,
      bool useHttps,
      int destination,
      const std::shared_ptr<exec::ExchangeQueue>& queue,
      memory::MemoryPool* pool = nullptr) {
    return PrestoExchangeSource::create(
        makeProducerUri(producerAddress, useHttps),
        destination,
        queue,
        pool != nullptr ? pool : pool_.get(),
        exchangeCpuExecutor_.get(),
        exchangeIoExecutor_.get(),
        &connectionPools_,
        useHttps ? sslContext_ : nullptr);
  }

  void requestNextPage(
      const std::shared_ptr<exec::ExchangeQueue>& queue,
      const std::shared_ptr<exec::ExchangeSource>& exchangeSource) {
    {
      std::lock_guard<std::mutex> l(queue->mutex());
      ASSERT_TRUE(exchangeSource->shouldRequestLocked());
    }
    exchangeSource->request(1 << 20, std::chrono::seconds(2));
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> exchangeCpuExecutor_;
  std::shared_ptr<folly::IOThreadPoolExecutor> exchangeIoExecutor_;
  ConnectionPools connectionPools_;
  folly::SSLContextPtr sslContext_;
};

int64_t totalBytes(const std::vector<std::string>& pages) {
  int64_t totalBytes = 0;
  for (const auto& page : pages) {
    totalBytes += 4 + page.size();
  }
  return totalBytes;
}

TEST_P(PrestoExchangeSourceTest, basic) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  const auto useHttps = GetParam().useHttps;
  auto producer = std::make_unique<Producer>();

  for (const auto& page : pages) {
    producer->enqueue(page);
  }
  producer->noMoreData();

  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = makeSingleSourceQueue();

  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);

  size_t beforePoolSize = pool_->usedBytes();
  size_t beforeQueueSize = queue->totalBytes();
  requestNextPage(queue, exchangeSource);
  for (int i = 0; i < pages.size(); i++) {
    auto page = waitForNextPage(queue);
    ASSERT_EQ(toString(page.get()), pages[i]) << "at " << i;
    requestNextPage(queue, exchangeSource);
  }
  waitForEndMarker(queue);

  size_t deltaPoolBytes = pool_->usedBytes() - beforePoolSize;
  size_t deltaQueueBytes = queue->totalBytes() - beforeQueueSize;
  EXPECT_EQ(deltaPoolBytes, deltaQueueBytes);

  producer->waitForDeleteResults();
  exchangeCpuExecutor_->stop();
  serverWrapper.stop();
  EXPECT_EQ(pool_->usedBytes(), 0);

  const auto stats = exchangeSource->stats();
  ASSERT_EQ(stats.size(), 2);
  ASSERT_EQ(stats.at("prestoExchangeSource.numPages"), pages.size());
  ASSERT_EQ(stats.at("prestoExchangeSource.totalBytes"), totalBytes(pages));
}

TEST_P(PrestoExchangeSourceTest, retryState) {
  PrestoExchangeSource::RetryState state(1000);
  ASSERT_FALSE(state.isExhausted());
  ASSERT_EQ(state.nextDelayMs(), 0);
  ASSERT_LT(state.nextDelayMs(), 200);
  ASSERT_FALSE(state.isExhausted());
  for (int i = 0; i < 10; ++i) {
    ASSERT_LE(state.nextDelayMs(), 10000);
  }
  ASSERT_FALSE(state.isExhausted());
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  ASSERT_TRUE(state.isExhausted());
}

TEST_P(PrestoExchangeSourceTest, retries) {
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kExchangeRequestTimeout), "1s");
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kExchangeMaxErrorDuration), "3s");
  std::vector<std::string> pages = {"page1 - xx"};
  const auto useHttps = GetParam().useHttps;
  std::atomic<int> numRequestTries(0);
  std::atomic<int> numAbortTries(0);

  auto shouldFail = [&](bool abort) {
    auto& numTries = abort ? numAbortTries : numRequestTries;
    ++numTries;
    // On the third try, simulate network delay by sleeping for longer than the
    // request timeout
    if (numTries == 3) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1'100));
      return true;
    }
    // Fail for the first two times
    return numTries <= 2;
  };
  auto producer = std::make_unique<Producer>(shouldFail);
  for (const auto& page : pages) {
    producer->enqueue(page);
  }
  producer->noMoreData();

  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = makeSingleSourceQueue();

  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);

  requestNextPage(queue, exchangeSource);
  {
    auto page = waitForNextPage(queue);
    ASSERT_EQ(toString(page.get()), pages[0]) << "at " << 0;
    ASSERT_EQ(exchangeSource->testingFailedAttempts(), 3);
    requestNextPage(queue, exchangeSource);
  }

  waitForEndMarker(queue);
  producer->waitForDeleteResults();
  serverWrapper.stop();
}

TEST_P(PrestoExchangeSourceTest, alwaysFail) {
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kExchangeRequestTimeout), "1s");
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kExchangeMaxErrorDuration), "3s");
  std::vector<std::string> pages = {"page1 - xx"};
  const auto useHttps = GetParam().useHttps;

  auto producer = std::make_unique<Producer>([&](bool) { return true; });
  for (const auto& page : pages) {
    producer->enqueue(page);
  }
  producer->noMoreData();

  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = makeSingleSourceQueue();

  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);

  requestNextPage(queue, exchangeSource);
  EXPECT_THAT(
      [&]() { waitForNextPage(queue); },
      ThrowsMessage<std::exception>(HasSubstr("Connection reset by peer")));
}

TEST_P(PrestoExchangeSourceTest, earlyTerminatingConsumer) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  const bool useHttps = GetParam().useHttps;

  auto producer = std::make_unique<Producer>();
  for (const auto& page : pages) {
    producer->enqueue(page);
  }
  producer->noMoreData();

  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = makeSingleSourceQueue();

  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);
  exchangeSource->close();

  producer->waitForDeleteResults();
  serverWrapper.stop();
  EXPECT_EQ(pool_->usedBytes(), 0);

  const auto stats = exchangeSource->stats();
  ASSERT_EQ(stats.size(), 2);
  ASSERT_EQ(stats.at("prestoExchangeSource.numPages"), 0);
  ASSERT_EQ(stats.at("prestoExchangeSource.totalBytes"), 0);
}

TEST_P(PrestoExchangeSourceTest, slowProducer) {
  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  const bool useHttps = GetParam().useHttps;

  auto producer = std::make_unique<Producer>();

  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = makeSingleSourceQueue();
  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);

  const size_t beforePoolSize = pool_->usedBytes();
  const size_t beforeQueueSize = queue->totalBytes();
  requestNextPage(queue, exchangeSource);

  for (int i = 0; i < pages.size(); i++) {
    producer->enqueue(pages[i]);
    auto page = waitForNextPage(queue);
    ASSERT_EQ(toString(page.get()), pages[i]) << "at " << i;
    requestNextPage(queue, exchangeSource);
  }
  producer->noMoreData();
  waitForEndMarker(queue);

  const size_t deltaPoolBytes = pool_->usedBytes() - beforePoolSize;
  const size_t deltaQueueBytes = queue->totalBytes() - beforeQueueSize;
  EXPECT_EQ(deltaPoolBytes, deltaQueueBytes);

  producer->waitForDeleteResults();
  serverWrapper.stop();
  EXPECT_EQ(pool_->usedBytes(), 0);

  const auto stats = exchangeSource->stats();
  ASSERT_EQ(stats.size(), 2);
  ASSERT_EQ(stats.at("prestoExchangeSource.numPages"), pages.size());
  ASSERT_EQ(stats.at("prestoExchangeSource.totalBytes"), totalBytes(pages));
}

DEBUG_ONLY_TEST_P(
    PrestoExchangeSourceTest,
    slowProducerAndEarlyTerminatingConsumer) {
  const bool useHttps = GetParam().useHttps;
  std::atomic_bool codePointHit{false};
  folly::EventCount closeWait;
  std::atomic_bool allCloseCheckPassed{false};
  SCOPED_TESTVALUE_SET(
      "facebook::presto::PrestoExchangeSource::doRequest",
      std::function<void(const PrestoExchangeSource*)>(
          ([&](const auto* prestoExchangeSource) {
            allCloseCheckPassed = true;
          })));
  SCOPED_TESTVALUE_SET(
      "facebook::presto::PrestoExchangeSource::handleDataResponse",
      std::function<void(const PrestoExchangeSource*)>(
          ([&](const auto* prestoExchangeSource) { codePointHit = true; })));

  auto producer = std::make_unique<Producer>();
  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());
  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = makeSingleSourceQueue();
  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);
  requestNextPage(queue, exchangeSource);

  closeWait.await([&]() { return allCloseCheckPassed.load(); });

  // Simulation of an early destruction of 'Task' will release following
  // resources, including pool_
  exchangeSource->close();
  queue->close();
  exchangeSource.reset();
  queue.reset();
  pool_.reset();

  // We want to wait a bit on the promise state to be valid. That way we are
  // sure getResults() on the server side (producer) goes into empty data
  // condition because we did not enqueue any data in producer yet. This allows
  // us to have full control to simulate a super late response return by
  // enqueuing a result afterwards.
  while (!producer->promise().valid()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  // We shall not crash here when response comes back super late.
  producer->enqueue("I'm a super slow response");

  // We need to wait a bit for response handling mechanism to happen in the
  // background. There is no way to know where we are for response handling as
  // all resources have been cleaned up, so explicitly waiting is the only way
  // to allow the execution of background processing. We expect the test to not
  // crash.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_TRUE(codePointHit);
  serverWrapper.stop();
}

TEST_P(PrestoExchangeSourceTest, failedProducer) {
  SystemConfig::instance()->setValue(
      std::string(SystemConfig::kExchangeMaxErrorDuration), "3s");

  std::vector<std::string> pages = {"page1 - xx", "page2 - xxxxx"};
  const bool useHttps = GetParam().useHttps;
  auto producer = std::make_unique<Producer>();

  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());

  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();

  auto queue = makeSingleSourceQueue();
  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);

  requestNextPage(queue, exchangeSource);
  producer->enqueue(pages[0]);

  // Stop server to simulate failed connection.
  serverWrapper.stop();

  EXPECT_THROW(waitForNextPage(queue), std::exception);
}

DEBUG_ONLY_TEST_P(
    PrestoExchangeSourceTest,
    exceedingMemoryCapacityForHttpResponse) {
  const int64_t memoryCapBytes = 1L << 30;
  const bool useHttps = GetParam().useHttps;

  for (bool persistentError : {false, true}) {
    SCOPED_TRACE(fmt::format("persistentError: {}", persistentError));

    auto rootPool = memoryManager()->addRootPool("", memoryCapBytes);
    const std::string leafPoolName("exceedingMemoryCapacityForHttpResponse");
    auto leafPool = rootPool->addLeafChild(leafPoolName);

    // Setup to allow exchange source sufficient time to retry.
    SystemConfig::instance()->setValue(
        std::string(SystemConfig::kExchangeMaxErrorDuration), "3s");

    const std::string injectedErrorMessage{"Inject allocation error"};
    std::atomic<int> numAllocations{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
        std::function<void(MemoryPool*)>(([&](MemoryPool* pool) {
          if (pool->name().compare(leafPoolName) != 0) {
            return;
          }
          // For non-consistent error, inject memory allocation failure once.
          ++numAllocations;
          if (numAllocations > 1 && !persistentError) {
            return;
          }
          VELOX_FAIL(injectedErrorMessage);
        })));

    auto producer = std::make_unique<Producer>();

    auto producerServer = createHttpServer(useHttps);
    producer->registerEndpoints(producerServer.get());

    test::HttpServerWrapper serverWrapper(std::move(producerServer));
    auto producerAddress = serverWrapper.start().get();

    auto queue = makeSingleSourceQueue();
    auto exchangeSource =
        makeExchangeSource(producerAddress, useHttps, 3, queue, leafPool.get());

    requestNextPage(queue, exchangeSource);
    const std::string payload(1 << 20, 'L');
    producer->enqueue(payload);

    if (persistentError) {
      VELOX_ASSERT_THROW(waitForNextPage(queue), "Failed to fetch data from");
    } else {
      const auto receivedPage = waitForNextPage(queue);
      ASSERT_EQ(toString(receivedPage.get()), payload);
    }
    producer->noMoreData();
    if (GetParam().immediateBufferTransfer) {
      // Verify that we have retried on memory allocation failure of the http
      // response data other than just failing the query.
      ASSERT_GE(exchangeSource->testingFailedAttempts(), 1);
    }
    ASSERT_EQ(leafPool->usedBytes(), 0);
  }
}

TEST_P(PrestoExchangeSourceTest, memoryAllocationAndUsageCheck) {
  const bool immediateBufferTransfer = GetParam().immediateBufferTransfer;
  std::vector<bool> resetPeaks = {false, true};
  for (const auto resetPeak : resetPeaks) {
    SCOPED_TRACE(fmt::format("resetPeak {}", resetPeak));

    PrestoExchangeSource::testingClearMemoryUsage();
    auto rootPool = memory::MemoryManager::getInstance()->addRootPool();
    auto leafPool = rootPool->addLeafChild("memoryAllocationAndUsageCheck");

    const bool useHttps = GetParam().useHttps;

    auto producer = std::make_unique<Producer>();

    auto producerServer = createHttpServer(useHttps);
    producer->registerEndpoints(producerServer.get());

    test::HttpServerWrapper serverWrapper(std::move(producerServer));
    auto producerAddress = serverWrapper.start().get();

    auto queue = makeSingleSourceQueue();
    auto exchangeSource =
        makeExchangeSource(producerAddress, useHttps, 3, queue, leafPool.get());

    const std::string smallPayload(7 << 10, 'L');
    producer->enqueue(smallPayload);
    requestNextPage(queue, exchangeSource);
    auto smallPage = waitForNextPage(queue);
    if (immediateBufferTransfer) {
      ASSERT_EQ(leafPool->stats().numAllocs, 2);
    }
    int64_t currMemoryBytes;
    int64_t peakMemoryBytes;
    PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
    ASSERT_EQ(
        immediateBufferTransfer ? memory::AllocationTraits::pageBytes(
                                      pool_->sizeClasses().front()) *
                (1 + 2)
                                : smallPayload.size() + 4,
        currMemoryBytes);

    ASSERT_EQ(
        immediateBufferTransfer ? memory::AllocationTraits::pageBytes(
                                      pool_->sizeClasses().front()) *
                (1 + 2)
                                : smallPayload.size() + 4,
        peakMemoryBytes);
    int64_t oldCurrMemoryBytes = currMemoryBytes;

    if (resetPeak) {
      PrestoExchangeSource::resetPeakMemoryUsage();
      PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, currMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, peakMemoryBytes);
    }

    smallPage.reset();
    PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
    ASSERT_EQ(0, currMemoryBytes);

    if (!resetPeak) {
      ASSERT_EQ(
          immediateBufferTransfer ? memory::AllocationTraits::pageBytes(
                                        pool_->sizeClasses().front()) *
                  (1 + 2)
                                  : smallPayload.size() + 4,
          peakMemoryBytes);
    } else {
      ASSERT_EQ(peakMemoryBytes, oldCurrMemoryBytes);
      oldCurrMemoryBytes = currMemoryBytes;
      PrestoExchangeSource::resetPeakMemoryUsage();
      PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, currMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, peakMemoryBytes);
    }

    const std::string largePayload(128 << 10, 'L');
    producer->enqueue(largePayload);
    requestNextPage(queue, exchangeSource);
    auto largePage = waitForNextPage(queue);
    producer->noMoreData();

    PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);

    ASSERT_EQ(
        immediateBufferTransfer ? memory::AllocationTraits::pageBytes(
                                      pool_->sizeClasses().front()) *
                (1 + 2 + 4 + 8 + 16 + 16)
                                : largePayload.size() + 4,
        currMemoryBytes);
    ASSERT_EQ(
        immediateBufferTransfer ? memory::AllocationTraits::pageBytes(
                                      pool_->sizeClasses().front()) *
                (1 + 2 + 4 + 8 + 16 + 16)
                                : largePayload.size() + 4,
        peakMemoryBytes);
    oldCurrMemoryBytes = currMemoryBytes;

    if (resetPeak) {
      PrestoExchangeSource::resetPeakMemoryUsage();
      PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, currMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, peakMemoryBytes);
    }

    largePage.reset();
    PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
    ASSERT_EQ(0, currMemoryBytes);
    ASSERT_EQ(
        immediateBufferTransfer ? memory::AllocationTraits::pageBytes(
                                      pool_->sizeClasses().front()) *
                (1 + 2 + 4 + 8 + 16 + 16)
                                : largePayload.size() + 4,
        peakMemoryBytes);

    if (!resetPeak) {
      ASSERT_EQ(
          immediateBufferTransfer ? memory::AllocationTraits::pageBytes(
                                        pool_->sizeClasses().front()) *
                  (1 + 2 + 4 + 8 + 16 + 16)
                                  : largePayload.size() + 4,
          peakMemoryBytes);
    } else {
      ASSERT_EQ(peakMemoryBytes, oldCurrMemoryBytes);
      oldCurrMemoryBytes = currMemoryBytes;
      PrestoExchangeSource::resetPeakMemoryUsage();
      PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, currMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, peakMemoryBytes);
    }

    requestNextPage(queue, exchangeSource);
    waitForEndMarker(queue);
    producer->waitForDeleteResults();
    serverWrapper.stop();
    PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
    ASSERT_EQ(0, currMemoryBytes);
    if (!resetPeak) {
      ASSERT_EQ(
          immediateBufferTransfer ? 192512 : largePayload.size() + 4,
          peakMemoryBytes);
    } else {
      ASSERT_EQ(peakMemoryBytes, oldCurrMemoryBytes);
      oldCurrMemoryBytes = currMemoryBytes;
      PrestoExchangeSource::resetPeakMemoryUsage();
      PrestoExchangeSource::getMemoryUsage(currMemoryBytes, peakMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, currMemoryBytes);
      ASSERT_EQ(oldCurrMemoryBytes, peakMemoryBytes);
      ASSERT_EQ(peakMemoryBytes, 0);
    }
  }
}

DEBUG_ONLY_TEST_P(PrestoExchangeSourceTest, closeRaceCondition) {
  const auto useHttps = GetParam().useHttps;
  auto producer = std::make_unique<Producer>();
  producer->enqueue("one pager");
  producer->noMoreData();

  auto producerServer = createHttpServer(useHttps);
  producer->registerEndpoints(producerServer.get());
  test::HttpServerWrapper serverWrapper(std::move(producerServer));
  auto producerAddress = serverWrapper.start().get();
  auto queue = makeSingleSourceQueue();

  SCOPED_TESTVALUE_SET(
      "facebook::presto::PrestoExchangeSource::request",
      std::function<void(PrestoExchangeSource*)>((
          [&](auto* prestoExchangeSource) { prestoExchangeSource->close(); })));
  auto exchangeSource = makeExchangeSource(producerAddress, useHttps, 3, queue);
  {
    std::lock_guard<std::mutex> l(queue->mutex());
    ASSERT_TRUE(exchangeSource->shouldRequestLocked());
  }
  auto future = exchangeSource->request(1 << 20, std::chrono::seconds(2));
  ASSERT_TRUE(future.isReady());
  auto response = std::move(future).get();
  ASSERT_EQ(response.bytes, 0);
  ASSERT_FALSE(response.atEnd);
  exchangeCpuExecutor_->stop();
  serverWrapper.stop();
}

INSTANTIATE_TEST_CASE_P(
    PrestoExchangeSourceTest,
    PrestoExchangeSourceTest,
    ::testing::Values(
        Params{true, true, 1, 1},
        Params{true, false, 1, 1},
        Params{false, true, 1, 1},
        Params{false, false, 1, 1},
        Params{true, true, 2, 10},
        Params{true, false, 2, 10},
        Params{false, true, 2, 10},
        Params{false, false, 2, 10}));
