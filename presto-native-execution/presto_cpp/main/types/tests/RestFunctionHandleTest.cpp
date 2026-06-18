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
#include "velox/functions/remote/client/Remote.h"
#include "velox/functions/remote/server/RemoteFunctionService.h"

using namespace facebook::presto;
using namespace facebook::velox;

class RestFunctionHandleTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void setupCallExpression() {
    memoryPool_ = memory::MemoryManager::getInstance()->addLeafPool();
    converter_ =
        std::make_unique<VeloxExprConverter>(memoryPool_.get(), &typeParser_);

    expectedMetadata_.serdeFormat = functions::remote::PageFormat::PRESTO_PAGE;

    testExpr_.returnType = "bigint";
    testExpr_.displayName = "testFunction";
    auto cexpr = std::make_shared<protocol::ConstantExpression>();
    cexpr->type = "bigint";
    cexpr->valueBlock.data = "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA";
    testExpr_.arguments.push_back(cexpr);

    auto cexpr2 = std::make_shared<protocol::ConstantExpression>();
    cexpr2->type = "bigint";
    cexpr2->valueBlock.data = "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA";
    testExpr_.arguments.push_back(cexpr2);
  }

  void SetUp() override {
    auto restConfig = restSystemConfig();
    auto systemConfig = SystemConfig::instance();
    systemConfig->initialize(std::move(restConfig));
    setupCallExpression();
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

  std::shared_ptr<protocol::RestFunctionHandle> functionHandle_;
  protocol::CallExpression testExpr_;
  functions::RemoteVectorFunctionMetadata expectedMetadata_;
  std::shared_ptr<memory::MemoryPool> memoryPool_;
  TypeParser typeParser_;
  std::unique_ptr<VeloxExprConverter> converter_;
};

TEST_F(RestFunctionHandleTest, parseRestFunctionHandle) {
  try {
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
    testExpr_.functionHandle = restFunctionHandle;

    auto expr = converter_->toVeloxExpr(testExpr_);
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

TEST_F(RestFunctionHandleTest, parseRestFunctionHandleWithDecimalType) {
  try {
    const std::string str = R"JSON(
{
  "@type": "RestFunctionHandle",
  "functionId": "remote.testSchema.decimalFunction;decimal(10,2);decimal(10,2)",
  "version": "v1",
  "signature": {
    "name": "decimalFunction",
    "kind": "SCALAR",
    "returnType": "decimal(10,2)",
    "argumentTypes": ["decimal(10,2)", "decimal(10,2)"],
    "typeVariableConstraints": [],
    "longVariableConstraints": [],
    "variableArity": false
  }
}
)JSON";

    const json j = json::parse(str);
    const std::shared_ptr<protocol::RestFunctionHandle> restFunctionHandle = j;

    protocol::CallExpression callExpr;
    callExpr.returnType = "decimal(10,2)";
    callExpr.displayName = "decimalFunction";
    callExpr.functionHandle = restFunctionHandle;

    auto cexpr = std::make_shared<protocol::ConstantExpression>();
    cexpr->type = "decimal(10,2)";
    cexpr->valueBlock.data = "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA";
    callExpr.arguments.push_back(cexpr);

    auto cexpr2 = std::make_shared<protocol::ConstantExpression>();
    cexpr2->type = "decimal(10,2)";
    cexpr2->valueBlock.data = "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA";
    callExpr.arguments.push_back(cexpr2);

    auto expr = converter_->toVeloxExpr(callExpr);
    auto typedCallExpr =
        std::dynamic_pointer_cast<const core::CallTypedExpr>(expr);
    ASSERT_NE(typedCallExpr, nullptr);
    EXPECT_EQ(typedCallExpr->name(), "remote.testSchema.decimalFunction");
    EXPECT_EQ(typedCallExpr->type()->kind(), TypeKind::BIGINT);

    EXPECT_EQ(typedCallExpr->inputs().size(), 2);
    auto arg0 = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
        typedCallExpr->inputs()[0]);
    auto arg1 = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
        typedCallExpr->inputs()[1]);
    ASSERT_NE(arg0, nullptr);
    ASSERT_NE(arg1, nullptr);
    EXPECT_EQ(arg0->type()->kind(), TypeKind::BIGINT);
    EXPECT_EQ(arg1->type()->kind(), TypeKind::BIGINT);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RestFunctionHandleTest, parseRestFunctionHandleWithArrayType) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "RestFunctionHandle",
          "functionId": "remote.testSchema.arrayFunction;array(bigint);array(varchar)",
          "version": "v1",
          "signature": {
            "name": "arrayFunction",
            "kind": "SCALAR",
            "returnType": "array(bigint)",
            "argumentTypes": ["array(bigint)", "array(varchar)"],
            "typeVariableConstraints": [],
            "longVariableConstraints": [],
            "variableArity": false
          }
        }
    )JSON";
    const json j = json::parse(str);
    const std::shared_ptr<protocol::RestFunctionHandle> restFunctionHandle = j;

    // Verify the signature parsing
    ASSERT_NE(restFunctionHandle, nullptr);
    EXPECT_EQ(
        restFunctionHandle->functionId,
        "remote.testSchema.arrayFunction;array(bigint);array(varchar)");
    EXPECT_EQ(restFunctionHandle->signature.name, "arrayFunction");
    EXPECT_EQ(restFunctionHandle->signature.returnType, "array(bigint)");
    EXPECT_EQ(restFunctionHandle->signature.argumentTypes.size(), 2);
    EXPECT_EQ(restFunctionHandle->signature.argumentTypes[0], "array(bigint)");
    EXPECT_EQ(restFunctionHandle->signature.argumentTypes[1], "array(varchar)");

    // Verify type parsing
    auto returnType =
        typeParser_.parse(restFunctionHandle->signature.returnType);
    EXPECT_EQ(returnType->kind(), TypeKind::ARRAY);
    auto returnArrayType =
        std::dynamic_pointer_cast<const ArrayType>(returnType);
    ASSERT_NE(returnArrayType, nullptr);
    EXPECT_EQ(returnArrayType->elementType()->kind(), TypeKind::BIGINT);

    auto argType0 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[0]);
    EXPECT_EQ(argType0->kind(), TypeKind::ARRAY);
    auto argArrayType0 = std::dynamic_pointer_cast<const ArrayType>(argType0);
    ASSERT_NE(argArrayType0, nullptr);
    EXPECT_EQ(argArrayType0->elementType()->kind(), TypeKind::BIGINT);

    auto argType1 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[1]);
    EXPECT_EQ(argType1->kind(), TypeKind::ARRAY);
    auto argArrayType1 = std::dynamic_pointer_cast<const ArrayType>(argType1);
    ASSERT_NE(argArrayType1, nullptr);
    EXPECT_EQ(argArrayType1->elementType()->kind(), TypeKind::VARCHAR);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RestFunctionHandleTest, parseRestFunctionHandleWithMapType) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "RestFunctionHandle",
          "functionId": "remote.testSchema.mapFunction;map(varchar,bigint);map(bigint,double)",
          "version": "v1",
          "signature": {
            "name": "mapFunction",
            "kind": "SCALAR",
            "returnType": "map(varchar,bigint)",
            "argumentTypes": ["map(varchar,bigint)", "map(bigint,double)"],
            "typeVariableConstraints": [],
            "longVariableConstraints": [],
            "variableArity": false
          }
        }
    )JSON";
    const json j = json::parse(str);
    const std::shared_ptr<protocol::RestFunctionHandle> restFunctionHandle = j;

    // Verify the signature parsing
    ASSERT_NE(restFunctionHandle, nullptr);
    EXPECT_EQ(
        restFunctionHandle->functionId,
        "remote.testSchema.mapFunction;map(varchar,bigint);map(bigint,double)");
    EXPECT_EQ(restFunctionHandle->signature.name, "mapFunction");
    EXPECT_EQ(restFunctionHandle->signature.returnType, "map(varchar,bigint)");
    EXPECT_EQ(restFunctionHandle->signature.argumentTypes.size(), 2);
    EXPECT_EQ(
        restFunctionHandle->signature.argumentTypes[0], "map(varchar,bigint)");
    EXPECT_EQ(
        restFunctionHandle->signature.argumentTypes[1], "map(bigint,double)");

    // Verify type parsing
    auto returnType =
        typeParser_.parse(restFunctionHandle->signature.returnType);
    EXPECT_EQ(returnType->kind(), TypeKind::MAP);
    auto returnMapType = std::dynamic_pointer_cast<const MapType>(returnType);
    ASSERT_NE(returnMapType, nullptr);
    EXPECT_EQ(returnMapType->keyType()->kind(), TypeKind::VARCHAR);
    EXPECT_EQ(returnMapType->valueType()->kind(), TypeKind::BIGINT);

    auto argType0 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[0]);
    EXPECT_EQ(argType0->kind(), TypeKind::MAP);
    auto argMapType0 = std::dynamic_pointer_cast<const MapType>(argType0);
    ASSERT_NE(argMapType0, nullptr);
    EXPECT_EQ(argMapType0->keyType()->kind(), TypeKind::VARCHAR);
    EXPECT_EQ(argMapType0->valueType()->kind(), TypeKind::BIGINT);

    auto argType1 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[1]);
    EXPECT_EQ(argType1->kind(), TypeKind::MAP);
    auto argMapType1 = std::dynamic_pointer_cast<const MapType>(argType1);
    ASSERT_NE(argMapType1, nullptr);
    EXPECT_EQ(argMapType1->keyType()->kind(), TypeKind::BIGINT);
    EXPECT_EQ(argMapType1->valueType()->kind(), TypeKind::DOUBLE);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RestFunctionHandleTest, parseRestFunctionHandleWithRowType) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "RestFunctionHandle",
          "functionId": "remote.testSchema.rowFunction;row(bigint,varchar);row(double,boolean)",
          "version": "v1",
          "signature": {
            "name": "rowFunction",
            "kind": "SCALAR",
            "returnType": "row(bigint,varchar)",
            "argumentTypes": ["row(bigint,varchar)", "row(double,boolean)"],
            "typeVariableConstraints": [],
            "longVariableConstraints": [],
            "variableArity": false
          }
        }
    )JSON";
    const json j = json::parse(str);
    const std::shared_ptr<protocol::RestFunctionHandle> restFunctionHandle = j;

    // Verify the signature parsing
    ASSERT_NE(restFunctionHandle, nullptr);
    EXPECT_EQ(
        restFunctionHandle->functionId,
        "remote.testSchema.rowFunction;row(bigint,varchar);row(double,boolean)");
    EXPECT_EQ(restFunctionHandle->signature.name, "rowFunction");
    EXPECT_EQ(restFunctionHandle->signature.returnType, "row(bigint,varchar)");
    EXPECT_EQ(restFunctionHandle->signature.argumentTypes.size(), 2);
    EXPECT_EQ(
        restFunctionHandle->signature.argumentTypes[0], "row(bigint,varchar)");
    EXPECT_EQ(
        restFunctionHandle->signature.argumentTypes[1], "row(double,boolean)");

    // Verify type parsing
    auto returnType =
        typeParser_.parse(restFunctionHandle->signature.returnType);
    EXPECT_EQ(returnType->kind(), TypeKind::ROW);
    auto returnRowType = std::dynamic_pointer_cast<const RowType>(returnType);
    ASSERT_NE(returnRowType, nullptr);
    EXPECT_EQ(returnRowType->size(), 2);
    EXPECT_EQ(returnRowType->childAt(0)->kind(), TypeKind::BIGINT);
    EXPECT_EQ(returnRowType->childAt(1)->kind(), TypeKind::VARCHAR);

    auto argType0 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[0]);
    EXPECT_EQ(argType0->kind(), TypeKind::ROW);
    auto argRowType0 = std::dynamic_pointer_cast<const RowType>(argType0);
    ASSERT_NE(argRowType0, nullptr);
    EXPECT_EQ(argRowType0->size(), 2);
    EXPECT_EQ(argRowType0->childAt(0)->kind(), TypeKind::BIGINT);
    EXPECT_EQ(argRowType0->childAt(1)->kind(), TypeKind::VARCHAR);

    auto argType1 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[1]);
    EXPECT_EQ(argType1->kind(), TypeKind::ROW);
    auto argRowType1 = std::dynamic_pointer_cast<const RowType>(argType1);
    ASSERT_NE(argRowType1, nullptr);
    EXPECT_EQ(argRowType1->size(), 2);
    EXPECT_EQ(argRowType1->childAt(0)->kind(), TypeKind::DOUBLE);
    EXPECT_EQ(argRowType1->childAt(1)->kind(), TypeKind::BOOLEAN);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RestFunctionHandleTest, parseRestFunctionHandleWithNestedComplexTypes) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "RestFunctionHandle",
          "functionId": "remote.testSchema.nestedFunction;array(map(varchar,bigint));row(array(decimal(10,2)),map(bigint,varchar))",
          "version": "v1",
          "signature": {
            "name": "nestedFunction",
            "kind": "SCALAR",
            "returnType": "map(varchar,array(bigint))",
            "argumentTypes": ["array(map(varchar,bigint))", "row(array(decimal(10,2)),map(bigint,varchar))"],
            "typeVariableConstraints": [],
            "longVariableConstraints": [],
            "variableArity": false
          }
        }
    )JSON";
    const json j = json::parse(str);
    const std::shared_ptr<protocol::RestFunctionHandle> restFunctionHandle = j;

    // Verify the signature parsing
    ASSERT_NE(restFunctionHandle, nullptr);
    EXPECT_EQ(restFunctionHandle->signature.name, "nestedFunction");
    EXPECT_EQ(
        restFunctionHandle->signature.returnType, "map(varchar,array(bigint))");
    EXPECT_EQ(restFunctionHandle->signature.argumentTypes.size(), 2);

    // Verify return type: map(varchar,array(bigint))
    auto returnType =
        typeParser_.parse(restFunctionHandle->signature.returnType);
    EXPECT_EQ(returnType->kind(), TypeKind::MAP);
    auto returnMapType = std::dynamic_pointer_cast<const MapType>(returnType);
    ASSERT_NE(returnMapType, nullptr);
    EXPECT_EQ(returnMapType->keyType()->kind(), TypeKind::VARCHAR);
    EXPECT_EQ(returnMapType->valueType()->kind(), TypeKind::ARRAY);
    auto valueArrayType =
        std::dynamic_pointer_cast<const ArrayType>(returnMapType->valueType());
    ASSERT_NE(valueArrayType, nullptr);
    EXPECT_EQ(valueArrayType->elementType()->kind(), TypeKind::BIGINT);

    // Verify arg0 type: array(map(varchar,bigint))
    auto argType0 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[0]);
    EXPECT_EQ(argType0->kind(), TypeKind::ARRAY);
    auto argArrayType = std::dynamic_pointer_cast<const ArrayType>(argType0);
    ASSERT_NE(argArrayType, nullptr);
    EXPECT_EQ(argArrayType->elementType()->kind(), TypeKind::MAP);
    auto elementMapType =
        std::dynamic_pointer_cast<const MapType>(argArrayType->elementType());
    ASSERT_NE(elementMapType, nullptr);
    EXPECT_EQ(elementMapType->keyType()->kind(), TypeKind::VARCHAR);
    EXPECT_EQ(elementMapType->valueType()->kind(), TypeKind::BIGINT);

    // Verify arg1 type: row(array(decimal(10,2)),map(bigint,varchar))
    auto argType1 =
        typeParser_.parse(restFunctionHandle->signature.argumentTypes[1]);
    EXPECT_EQ(argType1->kind(), TypeKind::ROW);
    auto argRowType = std::dynamic_pointer_cast<const RowType>(argType1);
    ASSERT_NE(argRowType, nullptr);
    EXPECT_EQ(argRowType->size(), 2);

    // First child: array(decimal(10,2))
    EXPECT_EQ(argRowType->childAt(0)->kind(), TypeKind::ARRAY);
    auto childArrayType =
        std::dynamic_pointer_cast<const ArrayType>(argRowType->childAt(0));
    ASSERT_NE(childArrayType, nullptr);
    EXPECT_EQ(childArrayType->elementType()->kind(), TypeKind::BIGINT);

    // Second child: map(bigint,varchar)
    EXPECT_EQ(argRowType->childAt(1)->kind(), TypeKind::MAP);
    auto childMapType =
        std::dynamic_pointer_cast<const MapType>(argRowType->childAt(1));
    ASSERT_NE(childMapType, nullptr);
    EXPECT_EQ(childMapType->keyType()->kind(), TypeKind::BIGINT);
    EXPECT_EQ(childMapType->valueType()->kind(), TypeKind::VARCHAR);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}
