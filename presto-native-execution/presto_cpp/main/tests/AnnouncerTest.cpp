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
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/tests/HttpServerWrapper.h"

DECLARE_bool(velox_memory_leak_check_enabled);

namespace fs = boost::filesystem;
using namespace facebook::presto;

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

std::unique_ptr<facebook::presto::test::HttpServerWrapper> makeDiscoveryServer(
    std::function<void()> onAnnouncement,
    bool useHttps) {
  auto httpServer = createHttpServer(useHttps);

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

// makeDiscoveryServer variant that captures the Host header from the first
// announcement so the test can assert it was set correctly.
std::unique_ptr<facebook::presto::test::HttpServerWrapper>
makeDiscoveryServerCapturingHost(
    std::function<void(const std::string&)> onAnnouncement,
    bool useHttps) {
  auto httpServer = createHttpServer(useHttps);

  httpServer->registerPut(
      R"(/v1/announcement/(.+))",
      [onAnnouncement](
          proxygen::HTTPMessage* message,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) mutable {
        std::string host =
            message->getHeaders().getSingleOrEmpty(proxygen::HTTP_HEADER_HOST);
        onAnnouncement(host);
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpAccepted, "Accepted")
            .header(
                proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeTextPlain)
            .sendWithEOM();
      });
  return std::make_unique<facebook::presto::test::HttpServerWrapper>(
      std::move(httpServer));
}

} // namespace

class AnnouncerTestSuite : public ::testing::TestWithParam<bool> {
  void SetUp() override {
    FLAGS_velox_memory_leak_check_enabled = true;

    std::string keyPath = getCertsPath("client_ca.pem");
    std::string ciphers = "ECDHE-ECDSA-AES256-GCM-SHA384,AES256-GCM-SHA384";
    sslContext_ = util::createSSLContext(keyPath, ciphers, false);
  }

 protected:
  folly::SSLContextPtr sslContext_;
};

class TestCoordinatorDiscoverer : public CoordinatorDiscoverer {
 public:
  TestCoordinatorDiscoverer(
      folly::Promise<bool> announcementPromise,
      bool useHttps)
      : announcementCnt(0), addressLookupCnt(0), useHttps(useHttps) {
    onAnnouncement = [this,
                      promiseHolder = std::make_shared<PromiseHolder<bool>>(
                          std::move(announcementPromise))]() {
      if (++announcementCnt == 5) {
        promiseHolder->get().setValue(true);
      }
    };
    discoveryServer = makeDiscoveryServer(onAnnouncement, useHttps);
    serverAddress = discoveryServer->start().get();
  }

  folly::SocketAddress updateAddress() override {
    auto prevCnt = addressLookupCnt++;
    if (prevCnt < 3) {
      updateHostnameSafe(
          fmt::format(
              "{}:{}", serverAddress.getAddressStr(), serverAddress.getPort()));
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
      discoveryServer = makeDiscoveryServer(onAnnouncement, useHttps);
      serverAddress = discoveryServer->start().get();
      updateHostnameSafe(
          fmt::format(
              "{}:{}", serverAddress.getAddressStr(), serverAddress.getPort()));
    }
    return serverAddress;
  }

  std::unique_ptr<test::HttpServerWrapper> discoveryServer;
  folly::SocketAddress serverAddress;
  std::function<void()> onAnnouncement;
  std::atomic_int announcementCnt;
  std::atomic_int addressLookupCnt;
  bool useHttps;
};

// CoordinatorDiscoverer that exposes the server address as a stable hostname
// and captures the Host header sent by the first announcement.
class HostHeaderCapturingDiscoverer : public CoordinatorDiscoverer {
 public:
  explicit HostHeaderCapturingDiscoverer(bool useHttps) : useHttps_(useHttps) {
    auto [p, f] = folly::makePromiseContract<std::string>();
    hostHeaderPromise_ = std::move(p);
    hostHeaderFuture_ = std::move(f);

    discoveryServer_ = makeDiscoveryServerCapturingHost(
        [this](const std::string& host) {
          if (!captured_) {
            captured_ = true;
            hostHeaderPromise_.setValue(host);
          }
        },
        useHttps_);
    serverAddress_ = discoveryServer_->start().get();
    // Pre-populate hostname so sendRequest() sets Host on the very first call.
    updateHostnameSafe(
        fmt::format(
            "{}:{}", serverAddress_.getAddressStr(), serverAddress_.getPort()));
  }

  folly::SocketAddress updateAddress() override {
    return serverAddress_;
  }

  std::string waitForHostHeader() {
    return std::move(hostHeaderFuture_).get();
  }

  std::unique_ptr<test::HttpServerWrapper> discoveryServer_;
  folly::SocketAddress serverAddress_;

 private:
  bool useHttps_;
  bool captured_{false};
  folly::Promise<std::string> hostHeaderPromise_;
  folly::SemiFuture<std::string> hostHeaderFuture_{
      folly::SemiFuture<std::string>::makeEmpty()};
};

TEST_P(AnnouncerTestSuite, basic) {
  const bool useHttps = GetParam();
  auto [promise, future] = folly::makePromiseContract<bool>();

  auto coordinatorDiscoverer =
      std::make_shared<TestCoordinatorDiscoverer>(std::move(promise), useHttps);

  Announcer announcer(
      "127.0.0.1",
      useHttps,
      1234,
      coordinatorDiscoverer,
      "testversion",
      "testing",
      "test-node",
      "test-node-location",
      "DEFAULT",
      true,
      {"hive", "tpch"},
      500 /*milliseconds*/,
      useHttps ? sslContext_ : nullptr);

  announcer.start();
  ASSERT_TRUE(std::move(future).getTry().hasValue());
  ASSERT_GE(coordinatorDiscoverer->addressLookupCnt, 8);
  announcer.stop();
}

// Verify that PeriodicServiceInventoryManager::sendRequest() sets the HTTP
// Host header from CoordinatorDiscoverer::getHostname() when it is non-empty,
// so that the coordinator's Jetty SNI host check passes.
TEST_P(AnnouncerTestSuite, hostHeaderIsSetFromHostname) {
  const bool useHttps = GetParam();

  auto coordinatorDiscoverer =
      std::make_shared<HostHeaderCapturingDiscoverer>(useHttps);

  const std::string expectedHost = fmt::format(
      "{}:{}",
      coordinatorDiscoverer->serverAddress_.getAddressStr(),
      coordinatorDiscoverer->serverAddress_.getPort());

  Announcer announcer(
      "127.0.0.1",
      useHttps,
      1234,
      coordinatorDiscoverer,
      "testversion",
      "testing",
      "test-node",
      "test-node-location",
      "DEFAULT",
      true,
      {"hive", "tpch"},
      100 /*milliseconds*/,
      useHttps ? sslContext_ : nullptr);

  announcer.start();
  std::string receivedHost = coordinatorDiscoverer->waitForHostHeader();
  announcer.stop();

  EXPECT_EQ(receivedHost, expectedHost);
}

INSTANTIATE_TEST_CASE_P(
    AnnouncerTest,
    AnnouncerTestSuite,
    ::testing::Values(true, false));
