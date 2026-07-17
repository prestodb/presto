#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::presto::functions::remote::rest;
namespace fs = boost::filesystem;

namespace facebook::presto::functions::remote::rest::test {
namespace {

void setConfig(const std::unordered_map<std::string, std::string>& props) {
  std::unordered_map<std::string, std::string> values(props);
  SystemConfig::instance()->initialize(
      std::make_unique<velox::config::ConfigBase>(std::move(values)));
}

// Certs live under presto_cpp/main/http/tests/certs/.
std::string getCertsPath(const std::string& fileName) {
  std::string currentPath = fs::current_path().c_str();
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");
  boost::algorithm::replace_all(currentPath, "_build/debug/", "");
  boost::algorithm::replace_all(currentPath, "_build/release/", "");
  return currentPath + "/presto_cpp/main/http/tests/certs/" + fileName;
}

class RestRemoteClientTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
  }
};

TEST_F(RestRemoteClientTest, constructsSuccessfullyOverHttp) {
  setConfig({{"http-server.https.enabled", "false"}});
  EXPECT_NO_THROW(RestRemoteClient client("http://127.0.0.1:9999"));
}

TEST_F(RestRemoteClientTest, throwsWhenHttpsEnabledButCiphersEmpty) {
  setConfig({
      {"http-server.https.enabled", "true"},
      {"https-supported-ciphers", ""},
      {"https-client-cert-key-path", "/some/path/client.pem"},
  });
  VELOX_ASSERT_THROW(
      RestRemoteClient("https://127.0.0.1:9999"),
      "HTTPS is enabled for remote function server but ciphers are not configured");
}

TEST_F(RestRemoteClientTest, throwsWhenHttpsEnabledButClientCertMissing) {
  setConfig({
      {"http-server.https.enabled", "true"},
      {"https-supported-ciphers", "ECDHE-RSA-AES128-GCM-SHA256"},
  });
  VELOX_ASSERT_THROW(
      RestRemoteClient("https://127.0.0.1:9999"),
      "HTTPS client certificates are not configured correctly");
}

// HTTPS success with valid ciphers + real cert
TEST_F(RestRemoteClientTest, constructsSuccessfullyOverHttpsWithValidConfig) {
  setConfig({
      {"http-server.https.enabled", "true"},
      {"https-supported-ciphers", "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384"},
      {"https-client-cert-key-path", getCertsPath("client_ca.pem")},
  });
  // Gets past config checks and createSSLContext(); does not open a socket yet.
  EXPECT_NO_THROW(RestRemoteClient client("https://127.0.0.1:9999"));
}

// Invalid cert path
TEST_F(RestRemoteClientTest, fatalsWhenClientCertPathDoesNotExist) {
  setConfig({
      {"http-server.https.enabled", "true"},
      {"https-supported-ciphers", "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384"},
      {"https-client-cert-key-path", "/tmp/does-not-exist-client.pem"},
  });
  EXPECT_DEATH(
      { RestRemoteClient("https://127.0.0.1:9999"); },
      "Unable to load certificate or key");
}

// Unreadable cert path
TEST_F(RestRemoteClientTest, fatalsWhenClientCertIsNotValidPem) {
  const std::string badPem = "/tmp/rest_remote_client_bad.pem";
  {
    std::ofstream out(badPem);
    out << "this is not a PEM certificate\n";
  }
  setConfig({
      {"http-server.https.enabled", "true"},
      {"https-supported-ciphers", "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384"},
      {"https-client-cert-key-path", badPem},
  });
  EXPECT_DEATH(
      { RestRemoteClient("https://127.0.0.1:9999"); },
      "Unable to load certificate or key");
}

} // namespace
} // namespace facebook::presto::functions::remote::rest::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}