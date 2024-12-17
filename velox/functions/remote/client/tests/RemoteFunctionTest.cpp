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
#include "velox/functions/prestosql/Fail.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/remote/client/Remote.h"
#include "velox/functions/remote/if/gen-cpp2/RemoteFunctionService.h"
#include "velox/functions/remote/server/RemoteFunctionService.h"
#include "velox/functions/remote/utils/RemoteFunctionServiceProvider.h"
#include "velox/serializers/PrestoSerializer.h"

using ::apache::thrift::ThriftServer;
using ::facebook::velox::test::assertEqualVectors;

namespace facebook::velox::functions {
namespace {

struct Foo {
  explicit Foo(int64_t id) : id_(id) {}

  int64_t id() const {
    return id_;
  }

  int64_t id_;

  static std::string serialize(const std::shared_ptr<Foo>& foo) {
    return std::to_string(foo->id_);
  }

  static std::shared_ptr<Foo> deserialize(const std::string& serialized) {
    return std::make_shared<Foo>(std::stoi(serialized));
  }
};

template <typename T>
struct OpaqueTypeFunction {
  template <typename TInput, typename TOutput>
  FOLLY_ALWAYS_INLINE void call(TOutput& result, const TInput& a) {
    LOG(INFO) << "OpaqueTypeFunction.value: " << a->id();
    result = a->id();
  }
};

// Parametrize in the serialization format so we can test both presto page and
// unsafe row.
class RemoteFunctionTest
    : public functions::test::FunctionBaseTest,
      public ::testing::WithParamInterface<remote::PageFormat> {
 public:
  void SetUp() override {
    auto params = startLocalThriftServiceAndGetParams();
    registerRemoteFunctions(params);
  }

  void TearDown() override {
    OpaqueType::clearSerializationRegistry();
  }

  // Registers a few remote functions to be used in this test.
  void registerRemoteFunctions(RemoteFunctionServiceParams params) {
    RemoteVectorFunctionMetadata metadata;
    metadata.serdeFormat = GetParam();
    metadata.location = params.serverAddress;

    // Register the remote adapter.
    auto plusSignatures = {exec::FunctionSignatureBuilder()
                               .returnType("bigint")
                               .argumentType("bigint")
                               .argumentType("bigint")
                               .build()};
    registerRemoteFunction("remote_plus", plusSignatures, metadata);

    auto failSignatures = {exec::FunctionSignatureBuilder()
                               .returnType("unknown")
                               .argumentType("integer")
                               .argumentType("varchar")
                               .build()};
    registerRemoteFunction("remote_fail", failSignatures, metadata);

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

    auto opaqueSignatures = {exec::FunctionSignatureBuilder()
                                 .returnType("bigint")
                                 .argumentType("opaque")
                                 .build()};
    registerRemoteFunction("remote_opaque", opaqueSignatures, metadata);

    // Registers the actual function under a different prefix. This is only
    // needed for tests since the thrift service runs in the same process.
    registerFunction<PlusFunction, int64_t, int64_t, int64_t>(
        {params.functionPrefix + ".remote_plus"});
    registerFunction<FailFunction, UnknownValue, int32_t, Varchar>(
        {params.functionPrefix + ".remote_fail"});
    registerFunction<CheckedDivideFunction, double, double, double>(
        {params.functionPrefix + ".remote_divide"});
    registerFunction<SubstrFunction, Varchar, Varchar, int32_t>(
        {params.functionPrefix + ".remote_substr"});
    registerFunction<OpaqueTypeFunction, int64_t, std::shared_ptr<Foo>>(
        {params.functionPrefix + ".remote_opaque"});

    registerOpaqueType<Foo>("Foo");
    OpaqueType::registerSerialization<Foo>(
        "Foo", Foo::serialize, Foo::deserialize);
  }
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

TEST_P(RemoteFunctionTest, tryException) {
  // remote_divide throws if denominator is 0.
  auto numeratorVector = makeFlatVector<double>({0, 1, 4, 9, 16});
  auto denominatorVector = makeFlatVector<double>({0, 1, 2, 3, 4});
  auto data = makeRowVector({numeratorVector, denominatorVector});
  auto results =
      evaluate<SimpleVector<double>>("TRY(remote_divide(c0, c1))", data);

  ASSERT_EQ(results->size(), 5);
  auto expected = makeFlatVector<double>({0 /* doesn't matter*/, 1, 2, 3, 4});
  expected->setNull(0, true);

  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionTest, conditionalConjunction) {
  // conditional conjunction disables throwing on error.
  auto inputVector0 = makeFlatVector<bool>({true, true});
  auto inputVector1 = makeFlatVector<int32_t>({1, 2});
  auto data = makeRowVector({inputVector0, inputVector1});
  auto results = evaluate<SimpleVector<StringView>>(
      "case when (c0 OR remote_fail(c1, 'error')) then 'hello' else 'world' end",
      data);

  ASSERT_EQ(results->size(), 2);
  auto expected = makeFlatVector<StringView>({"hello", "hello"});
  assertEqualVectors(expected, results);
}

TEST_P(RemoteFunctionTest, tryErrorCode) {
  // remote_fail doesn't throw, but returns error code.
  auto errorCodesVector = makeFlatVector<int32_t>({1, 2});
  auto errorMessagesVector =
      makeFlatVector<StringView>({"failed 1", "failed 2"});
  auto data = makeRowVector({errorCodesVector, errorMessagesVector});
  exec::ExprSet exprSet(
      {makeTypedExpr("TRY(remote_fail(c0, c1))", asRowType(data->type()))},
      &execCtx_);
  std::optional<SelectivityVector> rows;
  exec::EvalCtx context(&execCtx_, &exprSet, data.get());
  std::vector<VectorPtr> results(1);
  SelectivityVector defaultRows(data->size());
  exprSet.eval(defaultRows, context, results);

  ASSERT_EQ(results[0]->size(), 2);
}

TEST_P(RemoteFunctionTest, opaque) {
  // TODO: Support opaque type serialization in SPARK_UNSAFE_ROW
  if (GetParam() == remote::PageFormat::SPARK_UNSAFE_ROW) {
    GTEST_SKIP()
        << "opaque type serialization not supported in SPARK_UNSAFE_ROW";
  }
  auto inputVector = makeFlatVector<std::shared_ptr<void>>(
      2,
      [](vector_size_t row) { return std::make_shared<Foo>(row + 10); },
      /*isNullAt=*/nullptr,
      velox::OPAQUE<Foo>());
  auto data = makeRowVector({inputVector});
  LOG(INFO) << "type of data = " << data->type()->toString();

  auto results = evaluate<SimpleVector<int64_t>>(
      "remote_opaque(c0)", makeRowVector({inputVector}));

  auto expected = makeFlatVector<int64_t>({10, 11});
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
