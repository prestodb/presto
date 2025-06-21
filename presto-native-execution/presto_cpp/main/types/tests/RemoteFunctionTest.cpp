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

#include <gtest/gtest.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/remote/client/Remote.h"
#include "velox/functions/remote/server/RemoteFunctionService.h"

using namespace facebook::presto;
using namespace facebook::velox;

class RemoteFunctionTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManagerOptions{});
  }

  void SetUp() override {
    memoryPool_ = memory::MemoryManager::getInstance()->addLeafPool();
    converter_ =
        std::make_unique<VeloxExprConverter>(memoryPool_.get(), &typeParser_);

    const std::string str = R"(
        {
          "@type": "RestFunctionHandle",
          "functionId": "remote.testSchema.testFunction;BIGINT;BIGINT",
          "version": "v1",
          "signature": {
            "name": "testFunction",
            "kind": "SCALAR",
            "returnType": "bigint",
            "argumentTypes": ["bigint", "bigint"],
            "typeVariableConstraints": [],
            "longVariableConstraints": [],
            "variableArity": false
          }
        }
    )";

    const json j = json::parse(str);
    const std::shared_ptr<protocol::RestFunctionHandle> restFunctionHandle = j;

    expectedMetadata.serdeFormat = functions::remote::PageFormat::PRESTO_PAGE;

    testExpr.functionHandle = restFunctionHandle;
    testExpr.returnType = "bigint";
    testExpr.displayName = "testFunction";
    auto cexpr = std::make_shared<protocol::ConstantExpression>();
    cexpr->type = "bigint";
    cexpr->valueBlock.data = "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA";
    testExpr.arguments.push_back(cexpr);

    auto cexpr2 = std::make_shared<protocol::ConstantExpression>();
    cexpr2->type = "bigint";
    cexpr2->valueBlock.data = "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA";
    testExpr.arguments.push_back(cexpr2);
  }

  static std::unique_ptr<config::ConfigBase> restSystemConfig(
      const std::unordered_map<std::string, std::string>& configOverride = {}) {
    std::unordered_map<std::string, std::string> systemConfig{
        {std::string(SystemConfig::kRemoteFunctionServerSerde),
         std::string("presto_page")},
        {std::string(SystemConfig::kRemoteFunctionServerRestURL),
         std::string("http://localhost:8080")}};

    for (const auto& [configName, configValue] : configOverride) {
      systemConfig[configName] = configValue;
    }
    return std::make_unique<config::ConfigBase>(std::move(systemConfig), true);
  }

  std::shared_ptr<protocol::RestFunctionHandle> functionHandle;
  protocol::CallExpression testExpr;
  functions::RemoteVectorFunctionMetadata expectedMetadata;
  std::shared_ptr<memory::MemoryPool> memoryPool_;
  TypeParser typeParser_;
  std::unique_ptr<VeloxExprConverter> converter_;
};

TEST_F(RemoteFunctionTest, handlesRestFunctionCorrectly) {
  try {
    auto restConfig = restSystemConfig();
    auto systemConfig = SystemConfig::instance();
    systemConfig->initialize(std::move(restConfig));
    auto expr = converter_->toVeloxExpr(testExpr);
    auto callExpr = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr);
    ASSERT_NE(callExpr, nullptr);
    EXPECT_EQ(callExpr->name(), "remote.testSchema.testFunction");

    EXPECT_EQ(callExpr->inputs().size(), 2);
    auto arg0 = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
        callExpr->inputs()[0]);
    auto arg1 = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
        callExpr->inputs()[1]);
    ASSERT_NE(arg0, nullptr);
    ASSERT_NE(arg1, nullptr);
    EXPECT_EQ(arg0->type()->kind(), TypeKind::BIGINT);
    EXPECT_EQ(arg1->type()->kind(), TypeKind::BIGINT);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RemoteFunctionTest, unsupportedSerdeFormat) {
  std::unordered_map<std::string, std::string> restConfigOverride{
      {std::string(SystemConfig::kRemoteFunctionServerSerde),
       std::string("spark_unsafe_rows")}};
  auto restConfig = restSystemConfig(restConfigOverride);
  auto systemConfig = SystemConfig::instance();
  systemConfig->initialize(std::move(restConfig));

  VELOX_ASSERT_THROW(
      converter_->toVeloxExpr(testExpr),
      "presto_page serde is expected by remote function server but got : 'spark_unsafe_rows'");
}
