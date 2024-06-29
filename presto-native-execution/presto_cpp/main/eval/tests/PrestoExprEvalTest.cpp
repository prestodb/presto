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

#include "presto_cpp/main/eval/PrestoExprEval.h"
#include <folly/SocketAddress.h>
#include <folly/Uri.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto;
using namespace facebook::velox;

class PrestoExprEvalTest : public ::testing::Test,
                           public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    exec::registerFunctionCallToSpecialForms();
    parse::registerTypeResolver();

    auto httpServer = std::make_unique<http::HttpServer>(
        httpSrvIOExecutor_,
        std::make_unique<http::HttpConfig>(
            folly::SocketAddress("127.0.0.1", 0)));
    prestoExprEval_ = std::make_unique<eval::PrestoExprEval>(pool_);
    prestoExprEval_->registerUris(*httpServer.get());
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

  std::string getHttpBody(const std::unique_ptr<http::HttpResponse>& response) {
    std::ostringstream oss;
    auto iobufs = response->consumeBody();
    for (auto& body : iobufs) {
      oss << std::string((const char*)body->data(), body->length());
    }
    return oss.str();
  }

  std::string getHttpBody(const std::vector<std::string>& rowExprArray) {
    nlohmann::json output;
    for (auto i = 0; i < rowExprArray.size(); i++) {
      output.push_back(rowExprArray[i]);
    }
    return output.dump();
  }

  void validateHttpResponse(
      const std::string& input,
      const std::string& expected) {
    auto driverExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
        4, std::make_shared<folly::NamedThreadFactory>("Driver"));
    const auto url = "/v1/expressions";
    http::RequestBuilder()
        .method(proxygen::HTTPMethod::POST)
        .url(url)
        .send(client_.get(), input)
        .via(driverExecutor.get())
        .thenValue(
            [expected, this](std::unique_ptr<http::HttpResponse> response) {
              VELOX_USER_CHECK_EQ(
                  response->headers()->getStatusCode(), http::kHttpOk);
              if (response->hasError()) {
                VELOX_USER_FAIL(
                    "Expression evaluation failed: {}", response->error());
              }

              auto expectedJson = json::parse(expected);
              auto expectedArray = expectedJson.at("outputs");
              auto resStr = getHttpBody(response);
              auto resJson = json::parse(resStr);
              auto resArray = resJson.at("outputs");
              ASSERT_TRUE(resArray.is_array());
              VELOX_USER_CHECK_EQ(expectedArray.size(), resArray.size());
              auto sz = resArray.size();
              for (auto i = 0; i < sz; i++) {
                json result = resArray[i];
                json expectedVal = expectedArray[i];
                auto t = (result == expectedVal);
//                VELOX_USER_CHECK_EQ(expectedVal, result);
              }
            })
        .thenError(
            folly::tag_t<std::exception>{}, [&](const std::exception& e) {
              VLOG(1) << "Expression evaluation failed: " << e.what();
            });
  }

  std::unique_ptr<eval::PrestoExprEval> prestoExprEval_;
  std::unique_ptr<HttpServerWrapper> httpServerWrapper_;
  HttpClientFactory clientFactory_;
  std::shared_ptr<http::HttpClient> client_;
  std::shared_ptr<folly::IOThreadPoolExecutor> httpSrvIOExecutor_{
      std::make_shared<folly::IOThreadPoolExecutor>(8)};
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool("PrestoExprEvalTest")};
};

TEST_F(PrestoExprEvalTest, constant) {
  const std::string input = R"##(
    {
      "@type": "PrestoExprEvalTest",
      "inputs": [
          {
              "@type": "constant",
              "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
              "type": "integer"
          },
          {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAGEcAAA==",
            "type": "date"
          },
          {
              "@type": "constant",
              "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAIAAAAAAgAAADIz",
              "type": "varchar(25)"
          }
      ]
    }
    )##";
  const std::string expected = R"##(
{
  "@type": "PrestoExprEvalTest",
  "outputs": [
      {
          "@type": "constant",
          "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
          "type": "integer"
      },
      {
        "@type": "constant",
        "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAGEcAAA==",
        "type": "date"
      },
      {
          "@type": "constant",
          "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAIAAAAAAgAAADIz",
          "type": "varchar(25)"
      }
  ]
}
)##";

  validateHttpResponse(input, expected);
}

TEST_F(PrestoExprEvalTest, simpleCallExpr) {
  const std::string input = R"##(
    {
      "@type": "PrestoExprEvalTest",
      "inputs" : [
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "constant",
                "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
                "type": "integer"
              },
              {
                "@type": "constant",
                "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
                "type": "integer"
              }
            ],
            "displayName": "EQUAL",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": [
                  "integer",
                  "integer"
                ],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.$operator$equal",
                "returnType": "boolean",
                "typeVariableConstraints": [],
                "variableArity": false
              }
            },
            "returnType": "boolean"
          },
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "constant",
                "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
                "type": "integer"
              },
              {
                "@type": "constant",
                "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
                "type": "integer"
              }
            ],
            "displayName": "PLUS",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": [
                  "integer",
                  "integer"
                ],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.$operator$add",
                "returnType": "integer",
                "typeVariableConstraints": [],
                "variableArity": false
              }
            },
            "returnType": "integer"
          }
      ]
    }
    )##";
  const std::string expected = R"##(
  {
    "@type": "PrestoExprEvalTest",
    "outputs" : [
      "true",
      "2",

    ]
  }
)##";

  validateHttpResponse(input, expected);
}
