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
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/common/tests/MutableConfigs.h"
#include "presto_cpp/main/tests/HttpServerWrapper.h"
#include "velox/common/file/FileSystems.h"

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

// ============================================================================
// PeriodicServiceInventoryManager SSL-by-scheme tests
//
// The logic under test is in PeriodicServiceInventoryManager::sendRequest():
//
//   bool useSSL = false;
//   if (sslContext_ != nullptr) {
//     auto discoveryUri = systemConfig->discoveryUri();
//     if (discoveryUri.has_value()) {
//       useSSL = (folly::Uri(discoveryUri.value()).scheme() == "https");
//     }
//   }
//   client_ = std::make_shared<http::HttpClient>(...,
//       useSSL ? sslContext_ : nullptr, ...);
//
// Two cases:
//   sslScheme: discovery.uri = "https://..." + sslContext provided
//              → useSSL=true  → HTTPS client → announcements reach HTTPS server
//   httpScheme: discovery.uri = "http://..."  + sslContext provided
//              → useSSL=false → plain-HTTP client → announcements reach HTTP
//              server
//
// In both cases the Announcer is given a non-null sslContext_ to confirm that
// the scheme, not the mere presence of an sslContext, drives the choice.
// ============================================================================

// Simple one-shot discoverer that returns a fixed address.
class FixedAddressDiscoverer : public CoordinatorDiscoverer {
 public:
  explicit FixedAddressDiscoverer(folly::SocketAddress address)
      : address_(std::move(address)) {}

  folly::SocketAddress updateAddress() override {
    return address_;
  }

 private:
  folly::SocketAddress address_;
};

class PeriodicServiceInventoryManagerSslBySchemeTest : public ::testing::Test {
 public:
  void SetUp() override {
    FLAGS_velox_memory_leak_check_enabled = true;
    facebook::velox::filesystems::registerLocalFileSystem();
    // setupMutableSystemConfig() writes a config.properties file and calls
    // SystemConfig::instance()->initialize(), making setValue() available.
    test::setupMutableSystemConfig();

    std::string keyPath = getCertsPath("client_ca.pem");
    std::string ciphers = "ECDHE-ECDSA-AES256-GCM-SHA384,AES256-GCM-SHA384";
    sslContext_ = util::createSSLContext(keyPath, ciphers, false);
  }

 protected:
  // Run an Announcer against a test server of the given transport type
  // and assert that at least one announcement is delivered successfully.
  void runAndExpectAnnouncements(
      bool serverUsesHttps,
      const std::string& discoveryUriScheme) {
    auto [promise, future] = folly::makePromiseContract<bool>();

    // Counter shared between the server handler and the promise fulfiller.
    std::atomic<int> announcementCnt{0};
    auto onAnnouncement = [&announcementCnt,
                           holder = std::make_shared<PromiseHolder<bool>>(
                               std::move(promise))]() mutable {
      if (++announcementCnt == 3) {
        holder->get().setValue(true);
      }
    };

    auto discoveryServer = makeDiscoveryServer(onAnnouncement, serverUsesHttps);
    folly::SocketAddress serverAddress = discoveryServer->start().get();

    // Set discovery.uri to the chosen scheme so PeriodicServiceInventoryManager
    // picks the right transport when it creates the HttpClient.
    SystemConfig::instance()->setValue(
        std::string(SystemConfig::kDiscoveryUri),
        fmt::format(
            "{}://{}:{}",
            discoveryUriScheme,
            serverAddress.getAddressStr(),
            serverAddress.getPort()));

    auto discoverer = std::make_shared<FixedAddressDiscoverer>(serverAddress);

    // Always pass sslContext_ to confirm that the scheme (not the mere presence
    // of an sslContext) determines whether TLS is used.
    Announcer announcer(
        "127.0.0.1",
        serverUsesHttps,
        1234,
        discoverer,
        "testversion",
        "testing",
        "test-node",
        "test-node-location",
        "DEFAULT",
        /*sidecar=*/false,
        {"tpch"},
        200 /*frequencyMs*/,
        sslContext_);

    announcer.start();
    ASSERT_TRUE(std::move(future).getTry().hasValue());
    announcer.stop();
  }

  folly::SSLContextPtr sslContext_;
};

// discovery.uri uses https:// → sslContext is passed to the HttpClient → TLS
// handshake succeeds against the HTTPS test server.
TEST_F(
    PeriodicServiceInventoryManagerSslBySchemeTest,
    sslUsedWhenSchemeIsHttps) {
  runAndExpectAnnouncements(/*serverUsesHttps=*/true,
                            /*discoveryUriScheme=*/"https");
}

// discovery.uri uses http:// → sslContext is withheld from the HttpClient →
// plain-text connection succeeds against the HTTP test server, even though a
// non-null sslContext_ was supplied to the Announcer constructor.
TEST_F(
    PeriodicServiceInventoryManagerSslBySchemeTest,
    sslSkippedWhenSchemeIsHttp) {
  runAndExpectAnnouncements(/*serverUsesHttps=*/false,
                            /*discoveryUriScheme=*/"http");
}
