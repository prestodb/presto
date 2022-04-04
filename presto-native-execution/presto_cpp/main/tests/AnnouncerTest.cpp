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
#include "presto_cpp/main/Announcer.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/tests/HttpServerWrapper.h"

using namespace facebook::presto;

namespace {
template <typename T>
struct PromiseHolder {
  explicit PromiseHolder(folly::Promise<T> promise)
      : promise_(std::move(promise)) {}

  folly::Promise<T>& get() {
    return promise_;
  }

 private:
  folly::Promise<T> promise_;
};

std::unique_ptr<facebook::presto::test::HttpServerWrapper> makeDiscoveryServer(
    std::function<void()> onAnnouncement) {
  auto httpServer =
      std::make_unique<http::HttpServer>(folly::SocketAddress("127.0.0.1", 0));
  httpServer->registerPut(
      R"(/v1/announcement/(.+))",
      [onAnnouncement](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) mutable {
        onAnnouncement();
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpAccepted, "Accepted")
            .sendWithEOM();
      });
  return std::make_unique<facebook::presto::test::HttpServerWrapper>(
      std::move(httpServer));
}

} // namespace

TEST(AnnouncerTest, basic) {
  auto [promise, future] = folly::makePromiseContract<bool>();

  std::atomic_int announcementCnt(0);
  auto onAnnouncement = [&announcementCnt,
                         promiseHolder = std::make_shared<PromiseHolder<bool>>(
                             std::move(promise))]() {
    if (++announcementCnt == 5) {
      promiseHolder->get().setValue(true);
    }
  };

  auto discoveryServer = makeDiscoveryServer(onAnnouncement);
  auto serverAddress = discoveryServer->start().get();

  std::atomic_int addressLookupCnt(0);
  auto addressLookup = [&]() mutable {
    auto prevCnt = addressLookupCnt++;
    if (prevCnt < 3) {
      return serverAddress;
    }
    if (prevCnt == 3) {
      // Simulate failure to reach discovery server
      discoveryServer->stop();
      return serverAddress;
    }
    if (prevCnt < 6) {
      // Simulate failure to get the discovery server address
      throw std::runtime_error("Server is down");
    }
    if (prevCnt == 6) {
      discoveryServer = makeDiscoveryServer(onAnnouncement);
      serverAddress = discoveryServer->start().get();
    }
    return serverAddress;
  };

  Announcer announcer(
      "127.0.0.1",
      1234,
      addressLookup,
      "testversion",
      "testing",
      "test-node",
      "test-node-location",
      {"hive", "tpch"},
      100 /*milliseconds*/);

  announcer.start();
  ASSERT_TRUE(std::move(future).getTry().hasValue());
  ASSERT_GE(addressLookupCnt, 8);
  announcer.stop();
}
