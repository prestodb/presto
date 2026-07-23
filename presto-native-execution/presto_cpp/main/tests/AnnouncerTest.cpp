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
#include <gtest/gtest.h>
#include <algorithm>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/http/HttpConstants.h"
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

INSTANTIATE_TEST_CASE_P(
    AnnouncerTest,
    AnnouncerTestSuite,
    ::testing::Values(true, false));

#ifdef PRESTO_ENABLE_JWT
namespace {

// Discovery server that reports the cluster-internal bearer of every
// announcement it receives, empty when the header is absent.
// makeDiscoveryServer above ignores the request entirely, which is why an
// announcement missing the header went unnoticed.
std::unique_ptr<facebook::presto::test::HttpServerWrapper>
makeBearerCapturingDiscoveryServer(
    std::function<void(const std::string&)> onAnnouncement) {
  auto httpServer = createHttpServer(/*useHttps=*/false);

  httpServer->registerPut(
      R"(/v1/announcement/(.+))",
      [onAnnouncement = std::move(onAnnouncement)](
          proxygen::HTTPMessage* message,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) mutable {
        onAnnouncement(message->getHeaders().getSingleOrEmpty(
            http::kPrestoInternalBearer));
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpAccepted, "Accepted")
            .sendWithEOM();
      });
  return std::make_unique<facebook::presto::test::HttpServerWrapper>(
      std::move(httpServer));
}

// Fires its promise with the bearer of the first announcement, so the test does
// not have to wait for the announcer's steady state.
class BearerCapturingDiscoverer : public CoordinatorDiscoverer {
 public:
  explicit BearerCapturingDiscoverer(
      folly::Promise<std::string> bearerPromise) {
    auto onAnnouncement =
        [promiseHolder = std::make_shared<PromiseHolder<std::string>>(
             std::move(bearerPromise))](const std::string& bearer) mutable {
          if (!promiseHolder->get().isFulfilled()) {
            promiseHolder->get().setValue(bearer);
          }
        };
    discoveryServer_ =
        makeBearerCapturingDiscoveryServer(std::move(onAnnouncement));
    serverAddress_ = discoveryServer_->start().get();
  }

  folly::SocketAddress updateAddress() override {
    return serverAddress_;
  }

 private:
  std::unique_ptr<test::HttpServerWrapper> discoveryServer_;
  folly::SocketAddress serverAddress_;
};

std::unique_ptr<facebook::velox::config::ConfigBase> makeConfig(
    std::unordered_map<std::string, std::string> values) {
  return std::make_unique<facebook::velox::config::ConfigBase>(
      std::move(values), /*_mutable=*/true);
}

// Announces once and returns whatever arrived in X-Presto-Internal-Bearer.
// The options are snapshotted in PeriodicServiceInventoryManager's constructor,
// so the config has to be installed before the Announcer is constructed, not
// merely before start().
std::string announceAndCaptureBearer(bool jwtEnabled) {
  SystemConfig::instance()->initialize(makeConfig(
      {{std::string(SystemConfig::kMutableConfig), "true"},
       {std::string(SystemConfig::kInternalCommunicationJwtEnabled),
        jwtEnabled ? "true" : "false"},
       {std::string(SystemConfig::kInternalCommunicationSharedSecret),
        "announcer-test-secret"}}));

  NodeConfig::instance()->initialize(makeConfig(
      {{std::string(NodeConfig::kMutableConfig), "true"},
       {std::string(NodeConfig::kNodeId), "announcer-test-node"}}));

  auto [promise, future] = folly::makePromiseContract<std::string>();
  auto discoverer =
      std::make_shared<BearerCapturingDiscoverer>(std::move(promise));

  Announcer announcer(
      "127.0.0.1",
      /*useHttps=*/false,
      1234,
      discoverer,
      "testversion",
      "testing",
      "announcer-test-node",
      "test-node-location",
      "DEFAULT",
      true,
      {"hive"},
      50 /*milliseconds*/,
      /*sslContext=*/nullptr);

  announcer.start();
  auto bearer = std::move(future).get();
  announcer.stop();
  return bearer;
}

} // namespace

// SystemConfig and NodeConfig are process-global and presto_server_test runs
// many suites in one process, so restore both to a default-valued config after
// each case rather than leaving this test's JWT settings installed.
class AnnouncerJwtTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_velox_memory_leak_check_enabled = true;
  }

  void TearDown() override {
    SystemConfig::instance()->initialize(makeConfig({}));
    NodeConfig::instance()->initialize(makeConfig({}));
  }
};

// The announcement is built by hand in Announcer rather than through
// RequestBuilder, so nothing else guards that it carries the internal JWT: a
// coordinator that authenticates the announcement endpoint 401s an unsigned
// announcement and the node never registers.
TEST_F(AnnouncerJwtTest, carriesInternalJwtWhenEnabled) {
  const auto bearer = announceAndCaptureBearer(/*jwtEnabled=*/true);
  ASSERT_FALSE(bearer.empty())
      << "announcement carried no " << http::kPrestoInternalBearer;
  // A signed JWS is three dot-separated segments.
  EXPECT_EQ(std::count(bearer.begin(), bearer.end(), '.'), 2);
}

TEST_F(AnnouncerJwtTest, omitsInternalJwtWhenDisabled) {
  EXPECT_TRUE(announceAndCaptureBearer(/*jwtEnabled=*/false).empty());
}
#endif // PRESTO_ENABLE_JWT
