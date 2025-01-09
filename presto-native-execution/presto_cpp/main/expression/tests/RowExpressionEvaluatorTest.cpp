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
#include "presto_cpp/main/expression/RowExpressionEvaluator.h"
#include <folly/SocketAddress.h>
#include <folly/Uri.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace {
std::string getDataPath(const std::string& fileName) {
  std::string currentPath = fs::current_path().c_str();

  if (boost::algorithm::ends_with(currentPath, "fbcode")) {
    return currentPath +
        "/github/presto-trunk/presto-native-execution/presto_cpp/main/expression/tests/data/" +
        fileName;
  }

  if (boost::algorithm::ends_with(currentPath, "fbsource")) {
    return currentPath + "/third-party/presto_cpp/main/expression/tests/data/" +
        fileName;
  }

  // CLion runs the tests from cmake-build-release/ or cmake-build-debug/
  // directory. Hard-coded json files are not copied there and test fails with
  // file not found. Fixing the path so that we can trigger these tests from
  // CLion.
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");

  return currentPath + "/data/" + fileName;
}
} // namespace

// RowExpressionEvaluatorTest only tests basic expression evaluation via the
// 'v1/expressions' endpoint. End to end tests for different expression types
// can be found in TestDelegatingExpressionOptimizer.java, in the module
// presto-native-sidecar-plugin.
class RowExpressionEvaluatorTest
    : public ::testing::Test,
      public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    parse::registerTypeResolver();
    functions::prestosql::registerAllScalarFunctions("presto.default.");
    exec::registerFunctionCallToSpecialForms();

    auto httpServer = std::make_unique<http::HttpServer>(
        httpSrvIOExecutor_,
        std::make_unique<http::HttpConfig>(
            folly::SocketAddress("127.0.0.1", 0)));
    driverExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
    rowExpressionEvaluator_ =
        std::make_unique<expression::RowExpressionEvaluator>();
    httpServer->registerPost(
        "/v1/expressions",
        [&](proxygen::HTTPMessage* message,
            const std::vector<std::unique_ptr<folly::IOBuf>>& body,
            proxygen::ResponseHandler* downstream) {
          return rowExpressionEvaluator_->evaluate(message, body, downstream);
        });
    httpServerWrapper_ =
        std::make_unique<HttpServerWrapper>(std::move(httpServer));
    auto address = httpServerWrapper_->start().get();
    client_ = clientFactory_.newClient(
        address,
        std::chrono::milliseconds(100'000),
        std::chrono::milliseconds(0),
        false,
        pool_);
  }

  void TearDown() override {
    if (httpServerWrapper_) {
      httpServerWrapper_->stop();
    }
  }

  static std::string getHttpBody(
      const std::unique_ptr<http::HttpResponse>& response) {
    std::ostringstream oss;
    auto iobufs = response->consumeBody();
    for (auto& body : iobufs) {
      oss << std::string((const char*)body->data(), body->length());
    }
    return oss.str();
  }

  void validateHttpResponse(
      const std::string& inputStr,
      const std::string& expectedStr) {
    http::RequestBuilder()
        .method(proxygen::HTTPMethod::POST)
        .url("/v1/expressions")
        .send(client_.get(), inputStr)
        .via(driverExecutor_.get())
        .thenValue([expectedStr](std::unique_ptr<http::HttpResponse> response) {
          VELOX_USER_CHECK_EQ(
              response->headers()->getStatusCode(), http::kHttpOk);
          if (response->hasError()) {
            VELOX_USER_FAIL(
                "Expression evaluation failed: {}", response->error());
          }

          auto resStr = getHttpBody(response);
          auto resJson = json::parse(resStr);
          ASSERT_TRUE(resJson.is_array());
          auto expectedJson = json::parse(expectedStr);
          ASSERT_TRUE(expectedJson.is_array());
          EXPECT_EQ(expectedJson.size(), resJson.size());
          auto size = resJson.size();
          for (auto i = 0; i < size; i++) {
            EXPECT_EQ(resJson[i], expectedJson[i]);
          }
        })
        .thenError(
            folly::tag_t<std::exception>{}, [&](const std::exception& e) {
              VLOG(1) << "Expression evaluation failed: " << e.what();
            });
  }

  void testFile(const std::string& prefix) {
    std::string input = slurp(getDataPath(fmt::format("{}Input.json", prefix)));
    auto inputExpressions = json::parse(input);
    std::string output =
        slurp(getDataPath(fmt::format("{}Expected.json", prefix)));
    auto expectedExpressions = json::parse(output);

    validateHttpResponse(inputExpressions.dump(), expectedExpressions.dump());
  }

  std::unique_ptr<expression::RowExpressionEvaluator> rowExpressionEvaluator_;
  std::unique_ptr<HttpServerWrapper> httpServerWrapper_;
  HttpClientFactory clientFactory_;
  std::shared_ptr<http::HttpClient> client_;
  std::shared_ptr<folly::IOThreadPoolExecutor> httpSrvIOExecutor_{
      std::make_shared<folly::IOThreadPoolExecutor>(8)};
  std::unique_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool("RowExpressionEvaluatorTest")};
};

TEST_F(RowExpressionEvaluatorTest, simple) {
  // File SimpleExpressions{Input|Expected}.json contain the input and expected
  // JSON representing the RowExpressions resulting from the following queries:
  // select 1 + 2;
  // select abs(-11) + ceil(cast(3.4 as double)) + floor(cast(5.6 as double));
  // select 2 between 1 and 3;
  // Simple expression evaluation with constant folding is verified here.
  testFile("SimpleExpressions");
}

TEST_F(RowExpressionEvaluatorTest, specialFormRewrites) {
  // File SpecialExpressions{Input|Expected}.json contain the input and expected
  // JSON representing the RowExpressions resulting from the following queries:
  // select if(1 < 2, 2, 3);
  // select (1 < 2) and (2 < 3);
  // select (1 < 2) or (2 < 3);
  // Special form expression rewrites are verified here.
  testFile("SpecialForm");
}
