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

#include <folly/SocketAddress.h>
#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/CheckedArithmetic.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/remote/client/Remote.h"
#include "velox/functions/remote/if/gen-cpp2/RemoteFunctionService.h"
#include "velox/functions/remote/server/RemoteFunctionService.h"

using ::apache::thrift::ThriftServer;
using ::facebook::velox::test::assertEqualVectors;

namespace facebook::velox::functions {
namespace {

// Parametrize in the serialization format so we can test both presto page and
// unsafe row.
class RemoteFunctionTest
    : public functions::test::FunctionBaseTest,
      public ::testing::WithParamInterface<remote::PageFormat> {
 public:
  void SetUp() override {
    initializeServer();
    registerRemoteFunctions();
  }

  // Registers a few remote functions to be used in this test.
  void registerRemoteFunctions() {
    RemoteVectorFunctionMetadata metadata;
    metadata.serdeFormat = GetParam();
    metadata.location = location_;

    // Register the remote adapter.
    auto plusSignatures = {exec::FunctionSignatureBuilder()
                               .returnType("bigint")
                               .argumentType("bigint")
                               .argumentType("bigint")
                               .build()};
    registerRemoteFunction("remote_plus", plusSignatures, metadata);

    RemoteVectorFunctionMetadata wrongMetadata = metadata;
    wrongMetadata.location = folly::SocketAddress(); // empty address.
    registerRemoteFunction("remote_wrong_port", plusSignatures, wrongMetadata);

    auto divSignatures = {exec::FunctionSignatureBuilder()
                              .returnType("double")
                              .argumentType("double")
                              .argumentType("double")
                              .build()};
    registerRemoteFunction("remote_divide", divSignatures, metadata);

    auto substrSignatures = {exec::FunctionSignatureBuilder()
                                 .returnType("varchar")
                                 .argumentType("varchar")
                                 .argumentType("integer")
                                 .build()};
    registerRemoteFunction("remote_substr", substrSignatures, metadata);

    // Registers the actual function under a different prefix. This is only
    // needed for tests since the thrift service runs in the same process.
    registerFunction<PlusFunction, int64_t, int64_t, int64_t>(
        {remotePrefix_ + ".remote_plus"});
    registerFunction<CheckedDivideFunction, double, double, double>(
        {remotePrefix_ + ".remote_divide"});
    registerFunction<SubstrFunction, Varchar, Varchar, int32_t>(
        {remotePrefix_ + ".remote_substr"});
  }

  void initializeServer() {
    auto handler =
        std::make_shared<RemoteFunctionServiceHandler>(remotePrefix_);
    server_ = std::make_shared<ThriftServer>();
    server_->setInterface(handler);
    server_->setAddress(location_);

    thread_ = std::make_unique<std::thread>([&] { server_->serve(); });
    VELOX_CHECK(waitForRunning(), "Unable to initialize thrift server.");
    LOG(INFO) << "Thrift server is up and running in local port " << location_;
  }

  ~RemoteFunctionTest() {
    server_->stop();
    thread_->join();
    LOG(INFO) << "Thrift server stopped.";
  }

 private:
  // Loop until the server is up and running.
  bool waitForRunning() {
    for (size_t i = 0; i < 100; ++i) {
      if (server_->getServerStatus() == ThriftServer::ServerStatus::RUNNING) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
  }

  std::shared_ptr<apache::thrift::ThriftServer> server_;
  std::unique_ptr<std::thread> thread_;

  // Creates a random temporary file name to use to communicate as a unix domain
  // socket.
  folly::SocketAddress location_ = []() {
    char name[] = "/tmp/socketXXXXXX";
    int fd = mkstemp(name);
    if (fd < 0) {
      throw std::runtime_error("Failed to create temporary file for socket");
    }
    close(fd);
    std::string socketPath(name);
    // Cleanup existing socket file if it exists.
    unlink(socketPath.c_str());
    return folly::SocketAddress::makeFromPath(socketPath);
  }();

  const std::string remotePrefix_{"remote"};
};

TEST_P(RemoteFunctionTest, simple) {
  auto inputVector = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  auto results = evaluate<SimpleVector<int64_t>>(
      "remote_plus(c0, c0)", makeRowVector({inputVector}));

  auto expected = makeFlatVector<int64_t>({2, 4, 6, 8, 10});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionTest, string) {
  auto inputVector =
      makeFlatVector<StringView>({"hello", "my", "remote", "world"});
  auto inputVector1 = makeFlatVector<int32_t>({2, 1, 3, 5});
  auto results = evaluate<SimpleVector<StringView>>(
      "remote_substr(c0, c1)", makeRowVector({inputVector, inputVector1}));

  auto expected = makeFlatVector<StringView>({"ello", "my", "mote", "d"});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionTest, connectionError) {
  auto inputVector = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  auto func = [&]() {
    evaluate<SimpleVector<int64_t>>(
        "remote_wrong_port(c0, c0)", makeRowVector({inputVector}));
  };

  // Check it throw and that the exception has the "connection refused"
  // substring.
  EXPECT_THROW(func(), VeloxRuntimeError);
  try {
    func();
  } catch (const VeloxRuntimeError& e) {
    EXPECT_THAT(e.message(), testing::HasSubstr("Channel is !good()"));
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    RemoteFunctionTestFixture,
    RemoteFunctionTest,
    ::testing::Values(
        remote::PageFormat::PRESTO_PAGE,
        remote::PageFormat::SPARK_UNSAFE_ROW));

} // namespace
} // namespace facebook::velox::functions

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
