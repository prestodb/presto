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
#include "velox/experimental/codegen/code_generator/tests/CodegenExpressionsTestBase.h"

namespace facebook::velox {
namespace codegen::expressions::test {

class UDFCallExpressionTest : public ExpressionCodegenTestBase {
 public:
  // The type of the input rows in the tests
  using TestInputRowTypeTraits =
      RowTypeTrait<TypeKind::INTEGER, TypeKind::INTEGER>;

  // The type of the output row in the tests
  using TestOutputRowTypeTraits = RowTypeTrait<TypeKind::INTEGER>;

  // Test inputs
  std::vector<TestInputRowTypeTraits::ViewTupleType> inputTestRows = {
      {1, 2},
      {1, 4},
      {{}, 1},
      {{}, {}},
      {1, {}},
  };

  // Expected test outputs
  std::vector<TestOutputRowTypeTraits::ViewTupleType> expectedOutputs = {
      {3},
      {5},
      {},
      {},
      {},
  };

  // Test inputs
  std::vector<TestInputRowTypeTraits::ViewTupleType> inputTestRowsNoNull = {
      {1, 2},
      {1, 4},
  };

  // Expected test outputs
  std::vector<TestOutputRowTypeTraits::ViewTupleType> expectedOutputsNoNull = {
      {3},
      {5},
  };

  std::string headerPath =
      "\"velox/experimental/codegen/code_generator/tests/resources/UDFTest.h\"";

  // Build the AST and run tests to verify correct results
  void runTest(
      const UDFInformation& udfInformation,
      bool argumentsNodesNullable,
      bool printCode = false) {
    // Build AST node
    auto arg1Node =
        std::make_shared<InputRefExpr>(InputRefExpr(INTEGER(), "C0", 0));
    auto arg2Node =
        std::make_shared<InputRefExpr>(InputRefExpr(INTEGER(), "C1", 1));

    auto callNode = std::make_shared<UDFCallExpr>(
        UDFCallExpr(INTEGER(), udfInformation, {arg1Node, arg2Node}));

    auto makeRowExpression = std::make_shared<MakeRowExpression>(
        MakeRowExpression(INTEGER(), {callNode}));

    arg1Node->setMaybeNull(argumentsNodesNullable);
    arg2Node->setMaybeNull(argumentsNodesNullable);

    auto inputType = ROW({"C0", "C1"}, {INTEGER(), INTEGER()});
    auto outputType = ROW({"C0"}, {INTEGER()});
    getExprCodeGenerator().getContext().addHeaderPath(
        "\"velox/experimental/codegen/vector_function/StringTypes.h\"");

    auto generatedExpression =
        getExprCodeGenerator().generateCodegenExpressionStruct(
            *makeRowExpression.get(), *inputType.get(), *outputType.get());

    // Create codegen expression struct and compile it
    std::string cppFileContent;
    std::string invokeFunctionName;
    std::tie(invokeFunctionName, cppFileContent) =
        generatedExpression.wrapAsExternCallable(
            getExprCodeGenerator().getContext(),
            TestInputRowTypeTraits::codegenReadersType(),
            TestOutputRowTypeTraits::codegenWritersType());

    using codegenFunctionType = void (*)(
        const typename TestInputRowTypeTraits::CodegenTupleReadersType&,
        typename TestOutputRowTypeTraits::CodegenTupleWritersType&);

    auto* function = compileAndGetFunction<codegenFunctionType>(
        cppFileContent, invokeFunctionName, printCode);

    auto& testInputs =
        argumentsNodesNullable ? inputTestRows : inputTestRowsNoNull;
    auto& testExpectedOutputs =
        argumentsNodesNullable ? expectedOutputs : expectedOutputsNoNull;

    for (auto i = 0; i < testInputs.size(); i++) {
      auto output = TestOutputRowTypeTraits::createWritersTuple();
      function(testInputs[i], output);
      ASSERT_EQ(output, testExpectedOutputs[i]);
    }
  }

 private:
  UDFManager udfManager;
};

// Outputs and inputs are not optionals
TEST_F(UDFCallExpressionTest, testOutAndInNotOptional) {
  UDFInformation udf;
  udf.setCalledFunctionName("testAdd1");
  udf.setNullMode(ExpressionNullMode::NullInNullOut);
  udf.setHeaderFiles({headerPath});

  udf.setIsOptionalArguments(false);
  udf.setIsOptionalOutput(false);
  udf.setOutputForm(OutputForm::Return);
  udf.setCodegenNullInNullOutChecks(true);

  runTest(udf, true /*inputNodesNullable*/, false);
  //  if (true && tmp0.has_value() && tmp1.has_value()) {
  //    tmp2 = std::optional<int32_t>{testAdd1(*tmp0, *tmp1)};
  //  } else {
  //    tmp2 = std::nullopt;
  //  }

  runTest(udf, false /*inputNodesNullable*/, false);
  // tmp2 = std::optional<int32_t>{testAdd1(*tmp0, *tmp1)};
}

// Output is optional
TEST_F(UDFCallExpressionTest, testOutOptional) {
  UDFInformation udf;
  udf.setCalledFunctionName("testAdd2");
  udf.setNullMode(ExpressionNullMode::NullInNullOut);
  udf.setHeaderFiles({headerPath});

  udf.setIsOptionalArguments(false);
  udf.setIsOptionalOutput(true);
  udf.setOutputForm(OutputForm::Return);
  udf.setCodegenNullInNullOutChecks(true);

  runTest(udf, true /*inputNodesNullable*/, false);
  //  if (true && tmp0.has_value() && tmp1.has_value()) {
  //    tmp2 = std::optional<int32_t>{testAdd1(*tmp0, *tmp1)};
  //  } else {
  //    tmp2 = std::nullopt;
  //  }

  runTest(udf, false /*inputNodesNullable*/, false);
  //  tmp2 = testAdd2(*tmp0, *tmp1);
}

// Inputs and output optionals, codegenNullInNullOutChecks = false
TEST_F(UDFCallExpressionTest, testOutAnInOptional) {
  UDFInformation udf;
  udf.setCalledFunctionName("testAdd3");
  udf.setNullMode(ExpressionNullMode::NullInNullOut);
  udf.setHeaderFiles({headerPath});

  udf.setIsOptionalArguments(true);
  udf.setIsOptionalOutput(true);
  udf.setOutputForm(OutputForm::Return);
  udf.setCodegenNullInNullOutChecks(false);

  runTest(udf, true /*inputNodesNullable*/, false);
  //  tmp2 = testAdd3(tmp0, tmp1);

  runTest(udf, false /*inputNodesNullable*/, false);
  //  tmp2 = testAdd3(tmp0, tmp1);
}

} // namespace codegen::expressions::test
} // namespace facebook::velox
