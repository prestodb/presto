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

#include <cstdio>

#include <boost/asio.hpp>
#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/functions/remote/RestRemoteFunction.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestService.h"
#include "presto_cpp/main/functions/remote/tests/server/RestFunctionRegistry.h"
#include "presto_cpp/main/functions/remote/tests/server/examples/RemoteDoubleDivHandler.h"
#include "presto_cpp/main/functions/remote/tests/server/examples/RemoteFibonacciHandler.h"
#include "presto_cpp/main/functions/remote/tests/server/examples/RemoteInverseCdfHandler.h"
#include "presto_cpp/main/functions/remote/tests/server/examples/RemoteRemoveCharHandler.h"
#include "presto_cpp/main/functions/remote/tests/server/examples/RemoteStrLenHandler.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PortUtil.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/fbhive/HiveTypeParser.h"

using ::facebook::velox::test::assertEqualVectors;
using namespace facebook::velox;
namespace facebook::presto::functions::remote::rest::test {
namespace {

class RemoteFunctionRestTest
    : public velox::functions::test::FunctionBaseTest,
      public testing::WithParamInterface<velox::functions::remote::PageFormat> {
 public:
  void SetUp() override {
    auto servicePort = exec::test::getFreePort();
    location_ = fmt::format(kHostAddressTemplate_, servicePort);
    wrongLocation_ =
        fmt::format(kHostAddressTemplate_, exec::test::getFreePort());

    restClient_ = std::make_shared<RestRemoteClient>(location_);
    wrongRestClient_ = std::make_shared<RestRemoteClient>(wrongLocation_);

    initializeServer(servicePort);
    registerRemoteFunctions();
  }

  ~RemoteFunctionRestTest() override {
    if (serverThread_ && serverThread_->joinable()) {
      ioc_.stop();
      serverThread_->join();
    }
  }

 private:
  // Registers a remote function by creating a handler instance and registering
  // it on both the server and client side. The handler provides its own input
  // and output types, eliminating the need for manual type specification.
  template <typename Handler>
  void registerRemoteFunctionHelper(
      const std::string& functionName,
      const std::string& baseLocation,
      RestRemoteClientPtr client) const {
    auto handler = std::make_shared<Handler>();

    auto inputTypes = handler->getInputTypes();
    auto outputType = handler->getOutputType();

    auto signatureBuilder = exec::FunctionSignatureBuilder();
    signatureBuilder.returnType(outputType->toString());
    for (const auto& childType : inputTypes->children()) {
      signatureBuilder.argumentType(childType->toString());
    }

    RestFunctionRegistry::getInstance().registerFunction(functionName, handler);

    VeloxRemoteFunctionMetadata metadata;
    metadata.serdeFormat = GetParam();
    metadata.location = baseLocation + "/" + functionName;
    registerVeloxRemoteFunction(
        functionName, {signatureBuilder.build()}, metadata, client);
  }

  void registerRemoteFunctions() const {
    registerRemoteFunctionHelper<RemoteFibonacciHandler>(
        "remote_fibonacci", location_, restClient_);
    registerRemoteFunctionHelper<RemoteStrLenHandler>(
        "remote_strlen", location_, restClient_);
    registerRemoteFunctionHelper<RemoteRemoveCharHandler>(
        "remote_remove_char", location_, restClient_);
    registerRemoteFunctionHelper<RemoteInverseCdfHandler>(
        "remote_inverse_cdf", location_, restClient_);
    registerRemoteFunctionHelper<RemoteDoubleDivHandler>(
        "remote_divide", location_, restClient_);
    registerRemoteFunctionHelper<RemoteDoubleDivHandler>(
        "remote_wrong_port", wrongLocation_, wrongRestClient_);

    // Register a fake function handler whose logic is intentionally not
    // implemented in the server. This simulates a failure scenario for testing
    // purposes.
    auto roundSignatures = {exec::FunctionSignatureBuilder()
                                .returnType("integer")
                                .argumentType("integer")
                                .build()};
    VeloxRemoteFunctionMetadata metadata;
    metadata.serdeFormat = GetParam();
    metadata.location = location_ + "/remote_round";
    registerVeloxRemoteFunction(
        "remote_round", roundSignatures, metadata, restClient_);
  }

  void initializeServer(uint16_t servicePort) {
    serverThread_ = std::make_unique<std::thread>([this, servicePort]() {
      std::make_shared<RestListener>(
          ioc_,
          boost::asio::ip::tcp::endpoint(
              boost::asio::ip::make_address(kServiceHost), servicePort))
          ->run();
      ioc_.run();
    });

    VELOX_CHECK(
        waitForRunning(servicePort), "Unable to initialize HTTP server.");
  }

  bool waitForRunning(uint16_t servicePort) const {
    for (size_t i = 0; i < 100; ++i) {
      using boost::asio::ip::tcp;
      boost::asio::io_context io_context;

      tcp::socket socket(io_context);
      tcp::resolver resolver(io_context);

      try {
        boost::asio::connect(
            socket,
            resolver.resolve(kServiceHost, std::to_string(servicePort)));
        return true;
      } catch (std::exception& e) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
    return false;
  }

  std::unique_ptr<std::thread> serverThread_;
  boost::asio::io_context ioc_{1};
  static constexpr auto kHostAddressTemplate_ = "http://127.0.0.1:{}";
  static constexpr auto kServiceHost = "127.0.0.1";

  std::string location_;
  std::string wrongLocation_;
  RestRemoteClientPtr restClient_;
  RestRemoteClientPtr wrongRestClient_;
};

TEST_P(RemoteFunctionRestTest, connectionError) {
  auto numeratorVector = makeFlatVector<double>({0, 1, 4, 9, 16, 25, -25});
  auto denominatorVector = makeFlatVector<double>({0, 1, 2, 3, 4, 0, 2});
  auto data = makeRowVector({numeratorVector, denominatorVector});
  VELOX_ASSERT_THROW(
      evaluate<SimpleVector<double>>("remote_wrong_port(c0,c1)", data),
      "HTTP invocation failed for URL");
}

TEST_P(RemoteFunctionRestTest, fibonacci) {
  auto inputVector = makeFlatVector<int64_t>({10, 20});
  auto results = evaluate<SimpleVector<int64_t>>(
      "remote_fibonacci(c0)", makeRowVector({inputVector}));

  auto expected = makeFlatVector<int64_t>({55, 6765});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, stringLength) {
  auto inputVector =
      makeFlatVector<StringView>({"hello", "from", "remote", "server"});
  auto results = evaluate<SimpleVector<int32_t>>(
      "remote_strlen(c0)", makeRowVector({inputVector}));

  auto expected = makeFlatVector<int32_t>({5, 4, 6, 6});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, removeChar) {
  auto input = makeFlatVector<StringView>(
      {"hello from remote server",
       "testing remote server",
       "My file, named 'data_report#2.csv', is located in the folder: C:\\Users\\User\\Documents!  It's quite large (~1.5GB)."});
  auto charToRemove = makeFlatVector<StringView>({"o", "e", "c"});
  auto results = evaluate<SimpleVector<StringView>>(
      "remote_remove_char(c0,c1)", makeRowVector({input, charToRemove}));

  auto expected = makeFlatVector<StringView>(
      {"hell frm remte server",
       "tsting rmot srvr",
       "My file, named 'data_report#2.sv', is loated in the folder: C:\\Users\\User\\Douments!  It's quite large (~1.5GB)."});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, tryException) {
  // remote_divide throws if denominator is 0.
  auto numeratorVector = makeFlatVector<double>({0, 1, 4, 9, 16, 25, -25});
  auto denominatorVector = makeFlatVector<double>({0, 1, 2, 3, 4, 0, 2});
  auto data = makeRowVector({numeratorVector, denominatorVector});
  auto results = evaluate<SimpleVector<double>>("remote_divide(c0, c1)", data);

  ASSERT_EQ(results->size(), 7);
  auto expected = makeFlatVector<double>({0, 1, 2, 3, 4, 0, -12.5});
  expected->setNull(0, true);
  expected->setNull(5, true);

  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, inverseCdf) {
  auto pVector = makeFlatVector<double>({0.95, 0.95, 0.50, 0.10});
  auto nuVector = makeFlatVector<double>({4, 1, 10, 2});
  auto data = makeRowVector({pVector, nuVector});
  auto results =
      evaluate<SimpleVector<double>>("remote_inverse_cdf(c0, c1)", data);

  ASSERT_EQ(results->size(), 4);
  auto expected = makeFlatVector<double>({9.49, 3.84, 9.34, 0.21});

  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionRestTest, inverseCdfException) {
  // p < 0, p > 1, nu <= 0 - these should throw exceptions
  auto pVector = makeFlatVector<double>({-0.1});
  auto nuVector = makeFlatVector<double>({4});
  auto data = makeRowVector({pVector, nuVector});

  VELOX_ASSERT_THROW(
      evaluate<SimpleVector<double>>("remote_inverse_cdf(c0, c1)", data),
      "inverse_chi_squared_cdf: p must be in (0,1)");

  // Test p > 1
  pVector = makeFlatVector<double>({1.1});
  nuVector = makeFlatVector<double>({1});
  data = makeRowVector({pVector, nuVector});

  VELOX_ASSERT_THROW(
      evaluate<SimpleVector<double>>("remote_inverse_cdf(c0, c1)", data),
      "inverse_chi_squared_cdf: p must be in (0,1)");

  // Test nu <= 0
  pVector = makeFlatVector<double>({0.5});
  nuVector = makeFlatVector<double>({0});
  data = makeRowVector({pVector, nuVector});

  VELOX_ASSERT_THROW(
      evaluate<SimpleVector<double>>("remote_inverse_cdf(c0, c1)", data),
      "inverse_chi_squared_cdf: degrees of freedom must be > 0");

  // Test nu < 0
  pVector = makeFlatVector<double>({0.95});
  nuVector = makeFlatVector<double>({-2});
  data = makeRowVector({pVector, nuVector});

  VELOX_ASSERT_THROW(
      evaluate<SimpleVector<double>>("remote_inverse_cdf(c0, c1)", data),
      "inverse_chi_squared_cdf: degrees of freedom must be > 0");
}

TEST_P(RemoteFunctionRestTest, functionNotAvailable) {
  auto inputVector = makeFlatVector<int32_t>({-10, -20});
  VELOX_ASSERT_THROW(
      evaluate<SimpleVector<int32_t>>(
          "remote_round(c0)", makeRowVector({inputVector})),
      "Server responded with status 400. Body: 'Function 'remote_round' is not available.'");
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    RemoteFunctionRestTestFixture,
    RemoteFunctionRestTest,
    ::testing::Values(
        velox::functions::remote::PageFormat::PRESTO_PAGE,
        velox::functions::remote::PageFormat::SPARK_UNSAFE_ROW));

} // namespace
} // namespace facebook::presto::functions::remote::rest::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
