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
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"

using namespace facebook::presto;
using namespace facebook::velox;

class NativeFunctionHandleTest : public ::testing::Test {
 protected:
  TypeParser typeParser_;
};

TEST_F(NativeFunctionHandleTest, basic) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "native",
          "signature": {
            "name": "native.default.test",
            "kind": "SCALAR",
            "typeVariableConstraints": [],
            "longVariableConstraints": [],
            "returnType": "integer",
            "argumentTypes": ["integer", "integer"],
            "variableArity": true
          }
        }
    )JSON";
    const json j = json::parse(str);
    const std::shared_ptr<protocol::NativeFunctionHandle> nativeFunctionHandle =
        j;

    // Verify the signature parsing
    ASSERT_NE(nativeFunctionHandle, nullptr);
    EXPECT_EQ(nativeFunctionHandle->signature.name, "native.default.test");
    EXPECT_EQ(
        nativeFunctionHandle->signature.kind, protocol::FunctionKind::SCALAR);
    EXPECT_EQ(nativeFunctionHandle->signature.returnType, "integer");
    EXPECT_EQ(nativeFunctionHandle->signature.argumentTypes.size(), 2);
    EXPECT_EQ(nativeFunctionHandle->signature.argumentTypes[0], "integer");
    EXPECT_EQ(nativeFunctionHandle->signature.argumentTypes[1], "integer");
    EXPECT_EQ(nativeFunctionHandle->signature.variableArity, true);

    // Verify type parsing
    auto returnType =
        typeParser_.parse(nativeFunctionHandle->signature.returnType);
    EXPECT_EQ(returnType->kind(), TypeKind::INTEGER);

    auto argType0 =
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[0]);
    EXPECT_EQ(argType0->kind(), TypeKind::INTEGER);

    auto argType1 =
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[1]);
    EXPECT_EQ(argType1->kind(), TypeKind::INTEGER);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(NativeFunctionHandleTest, basicArray) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "native",
          "signature": {
            "name": "native.default.array_test",
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
    const std::shared_ptr<protocol::NativeFunctionHandle> nativeFunctionHandle =
        j;

    // Verify the signature parsing
    ASSERT_NE(nativeFunctionHandle, nullptr);
    EXPECT_EQ(
        nativeFunctionHandle->signature.name, "native.default.array_test");
    EXPECT_EQ(nativeFunctionHandle->signature.returnType, "array(bigint)");
    EXPECT_EQ(nativeFunctionHandle->signature.argumentTypes.size(), 2);
    EXPECT_EQ(
        nativeFunctionHandle->signature.argumentTypes[0], "array(bigint)");
    EXPECT_EQ(
        nativeFunctionHandle->signature.argumentTypes[1], "array(varchar)");

    // Verify type parsing
    auto returnType =
        typeParser_.parse(nativeFunctionHandle->signature.returnType);
    EXPECT_EQ(returnType->kind(), TypeKind::ARRAY);
    auto returnArrayType =
        std::dynamic_pointer_cast<const ArrayType>(returnType);
    ASSERT_NE(returnArrayType, nullptr);
    EXPECT_EQ(returnArrayType->elementType()->kind(), TypeKind::BIGINT);

    auto argType0 =
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[0]);
    EXPECT_EQ(argType0->kind(), TypeKind::ARRAY);
    auto argArrayType0 = std::dynamic_pointer_cast<const ArrayType>(argType0);
    ASSERT_NE(argArrayType0, nullptr);
    EXPECT_EQ(argArrayType0->elementType()->kind(), TypeKind::BIGINT);

    auto argType1 =
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[1]);
    EXPECT_EQ(argType1->kind(), TypeKind::ARRAY);
    auto argArrayType1 = std::dynamic_pointer_cast<const ArrayType>(argType1);
    ASSERT_NE(argArrayType1, nullptr);
    EXPECT_EQ(argArrayType1->elementType()->kind(), TypeKind::VARCHAR);

  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(NativeFunctionHandleTest, nestedComplexTypes) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "native",
          "signature": {
            "name": "native.default.nested_function",
            "kind": "SCALAR",
            "typeVariableConstraints": [],
            "longVariableConstraints": [],
            "returnType": "map(varchar,array(bigint))",
            "argumentTypes": ["array(map(varchar,bigint))", "row(array(decimal(10,2)),map(bigint,varchar))"],
            "variableArity": false
          }
        }
    )JSON";
    const json j = json::parse(str);
    const std::shared_ptr<protocol::NativeFunctionHandle> nativeFunctionHandle =
        j;

    // Verify the signature parsing
    ASSERT_NE(nativeFunctionHandle, nullptr);
    EXPECT_EQ(
        nativeFunctionHandle->signature.name, "native.default.nested_function");
    EXPECT_EQ(
        nativeFunctionHandle->signature.returnType,
        "map(varchar,array(bigint))");
    EXPECT_EQ(nativeFunctionHandle->signature.argumentTypes.size(), 2);
    EXPECT_EQ(
        nativeFunctionHandle->signature.argumentTypes[0],
        "array(map(varchar,bigint))");
    EXPECT_EQ(
        nativeFunctionHandle->signature.argumentTypes[1],
        "row(array(decimal(10,2)),map(bigint,varchar))");

    // Verify return type: map(varchar,array(bigint))
    auto returnType =
        typeParser_.parse(nativeFunctionHandle->signature.returnType);
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
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[0]);
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
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[1]);
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

TEST_F(NativeFunctionHandleTest, mixedConstraints) {
  try {
    const std::string str = R"JSON(
        {
          "@type": "native",
          "signature": {
            "name": "native.default.mod",
            "kind": "SCALAR",
            "typeVariableConstraints": [
              {
                "name": "T",
                "comparableRequired": false,
                "orderableRequired": false,
                "variadicBound": "",
                "nonDecimalNumericRequired": false
              }
            ],
            "longVariableConstraints":[
                {
                    "expression":"min(i2 - i6, i1 - i5) + max(i5, i6)",
                    "name":"i3"
                }
            ],
            "returnType":"decimal(10, 2)",
            "argumentTypes":["decimal(10, 2)","decimal(10, 2)"],
            "variableArity":false
          }
        }
    )JSON";
    const json j = json::parse(str);
    const std::shared_ptr<protocol::NativeFunctionHandle> nativeFunctionHandle =
        j;

    // Verify the signature parsing
    ASSERT_NE(nativeFunctionHandle, nullptr);
    EXPECT_EQ(nativeFunctionHandle->signature.name, "native.default.mod");
    EXPECT_EQ(
        nativeFunctionHandle->signature.kind, protocol::FunctionKind::SCALAR);
    EXPECT_EQ(nativeFunctionHandle->signature.returnType, "decimal(10, 2)");
    EXPECT_EQ(nativeFunctionHandle->signature.argumentTypes.size(), 2);
    EXPECT_EQ(
        nativeFunctionHandle->signature.argumentTypes[0], "decimal(10, 2)");
    EXPECT_EQ(
        nativeFunctionHandle->signature.argumentTypes[1], "decimal(10, 2)");

    // Verify type variable constraints
    EXPECT_EQ(
        nativeFunctionHandle->signature.typeVariableConstraints.size(), 1);
    EXPECT_EQ(
        nativeFunctionHandle->signature.typeVariableConstraints[0].name, "T");
    EXPECT_EQ(
        nativeFunctionHandle->signature.typeVariableConstraints[0]
            .comparableRequired,
        false);
    EXPECT_EQ(
        nativeFunctionHandle->signature.typeVariableConstraints[0]
            .orderableRequired,
        false);

    // Verify long variable constraints
    EXPECT_EQ(
        nativeFunctionHandle->signature.longVariableConstraints.size(), 1);
    EXPECT_EQ(
        nativeFunctionHandle->signature.longVariableConstraints[0].name, "i3");
    EXPECT_EQ(
        nativeFunctionHandle->signature.longVariableConstraints[0].expression,
        "min(i2 - i6, i1 - i5) + max(i5, i6)");

    // Verify type parsing for return type
    auto returnType =
        typeParser_.parse(nativeFunctionHandle->signature.returnType);
    EXPECT_EQ(returnType->kind(), TypeKind::BIGINT);

    // Verify arg0 type: decimal(10, 2)
    auto argType0 =
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[0]);
    EXPECT_EQ(argType0->kind(), TypeKind::BIGINT);

    // Verify arg1 type: decimal(10, 2)
    auto argType1 =
        typeParser_.parse(nativeFunctionHandle->signature.argumentTypes[1]);
    EXPECT_EQ(argType1->kind(), TypeKind::BIGINT);
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}
