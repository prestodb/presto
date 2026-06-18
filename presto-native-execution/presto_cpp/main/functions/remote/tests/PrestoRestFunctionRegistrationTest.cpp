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

#include "presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"

using namespace facebook::presto;

namespace facebook::presto::functions::remote::rest {

class PrestoRestFunctionRegistrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto systemConfig = SystemConfig::instance();
    std::unordered_map<std::string, std::string> config{
        {std::string(SystemConfig::kRemoteFunctionServerSerde),
         std::string("presto_page")},
        {std::string(SystemConfig::kRemoteFunctionServerRestURL),
         std::string("http://localhost:8080")}};
    systemConfig->initialize(
        std::make_unique<velox::config::ConfigBase>(std::move(config), true));
  }

  std::shared_ptr<protocol::RestFunctionHandle> parseRestFunctionHandle(
      const std::string& jsonStr) {
    const json j = json::parse(jsonStr);
    return j;
  }
};

TEST_F(
    PrestoRestFunctionRegistrationTest,
    getRemoteFunctionServerUrlWithExecutionEndpoint) {
  // Test when executionEndpoint is provided
  const std::string jsonStr = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.testFunction;BIGINT",
      "version": "v1",
      "signature": {
        "name": "testFunction",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": "http://custom-server:8080"
    }
  )JSON";

  auto handle = parseRestFunctionHandle(jsonStr);
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(*handle->executionEndpoint, "http://custom-server:8080");

  EXPECT_EQ(handle->signature.name, "testFunction");
  EXPECT_EQ(handle->signature.kind, protocol::FunctionKind::SCALAR);
  EXPECT_EQ(handle->signature.returnType, "bigint");
  ASSERT_EQ(handle->signature.argumentTypes.size(), 1);
  EXPECT_EQ(handle->signature.argumentTypes[0], "bigint");
  EXPECT_FALSE(handle->signature.variableArity);

  std::string result =
      PrestoRestFunctionRegistration::getInstance().getRemoteFunctionServerUrl(
          *handle);

  EXPECT_EQ(result, "http://custom-server:8080");
}

TEST_F(
    PrestoRestFunctionRegistrationTest,
    getRemoteFunctionServerUrlWithEmptyExecutionEndpoint) {
  // Test when executionEndpoint is provided but empty
  const std::string jsonStr = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.testFunction;BIGINT",
      "version": "v1",
      "signature": {
        "name": "testFunction",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": ""
    }
  )JSON";

  auto handle = parseRestFunctionHandle(jsonStr);
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(*handle->executionEndpoint, "");

  EXPECT_EQ(handle->signature.name, "testFunction");
  EXPECT_EQ(handle->signature.kind, protocol::FunctionKind::SCALAR);
  EXPECT_EQ(handle->signature.returnType, "bigint");
  ASSERT_EQ(handle->signature.argumentTypes.size(), 1);
  EXPECT_EQ(handle->signature.argumentTypes[0], "bigint");
  EXPECT_FALSE(handle->signature.variableArity);

  auto& instance = PrestoRestFunctionRegistration::getInstance();
  std::string result = instance.getRemoteFunctionServerUrl(*handle);

  // Should fall back to default URL (kRemoteFunctionServerRestURL_)
  EXPECT_EQ(result, "http://localhost:8080");
}

TEST_F(
    PrestoRestFunctionRegistrationTest,
    getRemoteFunctionServerUrlWithoutExecutionEndpoint) {
  // Test when executionEndpoint is not provided (nullopt)
  const std::string jsonStr = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.testFunction;BIGINT",
      "version": "v1",
      "signature": {
        "name": "testFunction",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      }
    }
  )JSON";

  auto handle = parseRestFunctionHandle(jsonStr);
  ASSERT_NE(handle, nullptr);

  EXPECT_EQ(handle->signature.name, "testFunction");
  EXPECT_EQ(handle->signature.kind, protocol::FunctionKind::SCALAR);
  EXPECT_EQ(handle->signature.returnType, "bigint");
  ASSERT_EQ(handle->signature.argumentTypes.size(), 1);
  EXPECT_EQ(handle->signature.argumentTypes[0], "bigint");
  EXPECT_FALSE(handle->signature.variableArity);

  auto& instance = PrestoRestFunctionRegistration::getInstance();
  std::string result = instance.getRemoteFunctionServerUrl(*handle);

  // Should fall back to default URL (kRemoteFunctionServerRestURL_)
  EXPECT_EQ(result, "http://localhost:8080");
}

TEST_F(
    PrestoRestFunctionRegistrationTest,
    getRemoteFunctionServerUrlConsistency) {
  // Test that the same input produces the same output
  const std::string jsonStr1 = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.function1;BIGINT",
      "version": "v1",
      "signature": {
        "name": "function1",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": "http://server1:9090"
    }
  )JSON";

  const std::string jsonStr2 = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.function2;BIGINT",
      "version": "v1",
      "signature": {
        "name": "function2",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": "http://server2:9091"
    }
  )JSON";

  const std::string jsonStr3 = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.function3;BIGINT",
      "version": "v1",
      "signature": {
        "name": "function3",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": "http://server1:9090"
    }
  )JSON";

  auto handle1 = parseRestFunctionHandle(jsonStr1);
  auto handle2 = parseRestFunctionHandle(jsonStr2);
  auto handle3 = parseRestFunctionHandle(jsonStr3);

  EXPECT_EQ(handle1->signature.name, "function1");
  EXPECT_EQ(handle1->signature.kind, protocol::FunctionKind::SCALAR);

  EXPECT_EQ(handle2->signature.name, "function2");
  EXPECT_EQ(handle2->signature.kind, protocol::FunctionKind::SCALAR);

  EXPECT_EQ(handle3->signature.name, "function3");
  EXPECT_EQ(handle3->signature.kind, protocol::FunctionKind::SCALAR);

  auto& instance = PrestoRestFunctionRegistration::getInstance();
  std::string result1 = instance.getRemoteFunctionServerUrl(*handle1);
  std::string result2 = instance.getRemoteFunctionServerUrl(*handle2);
  std::string result3 = instance.getRemoteFunctionServerUrl(*handle3);

  EXPECT_EQ(result1, "http://server1:9090");
  EXPECT_EQ(result2, "http://server2:9091");
  EXPECT_EQ(result3, "http://server1:9090");
  EXPECT_EQ(result1, result3);
  EXPECT_NE(result1, result2);
}

TEST_F(
    PrestoRestFunctionRegistrationTest,
    getRemoteFunctionServerUrlWithDifferentProtocols) {
  // Test with different URL protocols
  const std::string httpJsonStr = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.httpFunction;BIGINT",
      "version": "v1",
      "signature": {
        "name": "httpFunction",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": "http://server:8080"
    }
  )JSON";

  const std::string httpsJsonStr = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.httpsFunction;BIGINT",
      "version": "v1",
      "signature": {
        "name": "httpsFunction",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": "https://secure-server:8443"
    }
  )JSON";

  auto httpHandle = parseRestFunctionHandle(httpJsonStr);
  auto httpsHandle = parseRestFunctionHandle(httpsJsonStr);

  EXPECT_EQ(httpHandle->signature.name, "httpFunction");
  EXPECT_EQ(httpHandle->signature.kind, protocol::FunctionKind::SCALAR);
  EXPECT_EQ(httpHandle->signature.returnType, "bigint");

  EXPECT_EQ(httpsHandle->signature.name, "httpsFunction");
  EXPECT_EQ(httpsHandle->signature.kind, protocol::FunctionKind::SCALAR);
  EXPECT_EQ(httpsHandle->signature.returnType, "bigint");

  std::string httpResult =
      PrestoRestFunctionRegistration::getInstance().getRemoteFunctionServerUrl(
          *httpHandle);
  std::string httpsResult =
      PrestoRestFunctionRegistration::getInstance().getRemoteFunctionServerUrl(
          *httpsHandle);

  EXPECT_EQ(httpResult, "http://server:8080");
  EXPECT_EQ(httpsResult, "https://secure-server:8443");
}

TEST_F(
    PrestoRestFunctionRegistrationTest,
    getRemoteFunctionServerUrlWithComplexUrls) {
  // Test with URLs containing paths and query parameters
  const std::string jsonStr = R"JSON(
    {
      "@type": "RestFunctionHandle",
      "functionId": "remote.testSchema.complexFunction;BIGINT",
      "version": "v1",
      "signature": {
        "name": "complexFunction",
        "kind": "SCALAR",
        "returnType": "bigint",
        "argumentTypes": ["bigint"],
        "typeVariableConstraints": [],
        "longVariableConstraints": [],
        "variableArity": false
      },
      "executionEndpoint": "http://server:8080/api/v1/functions?param=value"
    }
  )JSON";

  auto handle = parseRestFunctionHandle(jsonStr);
  ASSERT_NE(handle, nullptr);

  EXPECT_EQ(handle->signature.name, "complexFunction");
  EXPECT_EQ(handle->signature.kind, protocol::FunctionKind::SCALAR);
  EXPECT_EQ(handle->signature.returnType, "bigint");
  ASSERT_EQ(handle->signature.argumentTypes.size(), 1);
  EXPECT_EQ(handle->signature.argumentTypes[0], "bigint");
  EXPECT_FALSE(handle->signature.variableArity);

  std::string result =
      PrestoRestFunctionRegistration::getInstance().getRemoteFunctionServerUrl(
          *handle);

  EXPECT_EQ(result, "http://server:8080/api/v1/functions?param=value");
}

} // namespace facebook::presto::functions::remote::rest
