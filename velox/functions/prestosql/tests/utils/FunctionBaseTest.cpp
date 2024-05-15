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
#include "FunctionBaseTest.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::functions::test {

void FunctionBaseTest::SetUpTestCase() {
  parse::registerTypeResolver();
  functions::prestosql::registerAllScalarFunctions();
  memory::MemoryManager::testingSetInstance({});
}

// static
std::vector<const exec::FunctionSignature*> FunctionBaseTest::getSignatures(
    const std::string& functionName,
    const std::string& returnType) {
  const auto allSignatures = getFunctionSignatures();

  std::vector<const exec::FunctionSignature*> signatures;
  for (const auto& signature : allSignatures.at(functionName)) {
    const auto& typeName = signature->returnType().baseName();
    if (exec::sanitizeName(typeName) == exec::sanitizeName(returnType)) {
      signatures.push_back(signature);
    }
  }
  VELOX_CHECK(
      !signatures.empty(),
      "No signature found for function {} with return type {}.",
      functionName,
      returnType);
  return signatures;
}

// static
std::unordered_set<std::string> FunctionBaseTest::getSignatureStrings(
    const std::string& functionName) {
  auto allSignatures = getFunctionSignatures();
  const auto& signatures = allSignatures.at(functionName);

  std::unordered_set<std::string> signatureStrings;
  for (const auto& signature : signatures) {
    signatureStrings.insert(signature->toString());
  }
  return signatureStrings;
}

std::pair<VectorPtr, std::unordered_map<std::string, exec::ExprStats>>
FunctionBaseTest::evaluateWithStats(
    const std::string& expression,
    const RowVectorPtr& data,
    const std::optional<SelectivityVector>& rows) {
  auto typedExpr = makeTypedExpr(expression, asRowType(data->type()));

  std::vector<VectorPtr> results(1);

  exec::ExprSet exprSet({typedExpr}, &execCtx_);
  exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
  if (rows.has_value()) {
    exprSet.eval(*rows, evalCtx, results);
  } else {
    SelectivityVector defaultRows(data->size());
    exprSet.eval(defaultRows, evalCtx, results);
  }

  return {results[0], exprSet.stats()};
}

void FunctionBaseTest::testEncodings(
    const core::TypedExprPtr& expr,
    const std::vector<VectorPtr>& inputs,
    const VectorPtr& expected) {
  VELOX_CHECK(!inputs.empty());

  const auto size = inputs[0]->size();
  VELOX_CHECK_GE(size, 3);

  auto testDictionary = [&](vector_size_t dictionarySize,
                            std::function<vector_size_t(vector_size_t)> indexAt,
                            std::function<bool(vector_size_t)> nullAt =
                                nullptr) {
    // Wrap each input in its own dictionary.
    std::vector<VectorPtr> encodedInputs;
    encodedInputs.reserve(inputs.size());
    for (const auto& input : inputs) {
      auto indices = makeIndices(dictionarySize, indexAt);
      auto nulls =
          nullAt ? makeNulls(dictionarySize, nullAt) : BufferPtr(nullptr);
      encodedInputs.emplace_back(
          BaseVector::wrapInDictionary(nulls, indices, dictionarySize, input));
    }
    auto encodedRow = makeRowVector(encodedInputs);

    SCOPED_TRACE(fmt::format("Dictionary: {}", encodedRow->toString()));

    auto indices = makeIndices(dictionarySize, indexAt);
    auto nulls =
        nullAt ? makeNulls(dictionarySize, nullAt) : BufferPtr(nullptr);
    auto expectedResult =
        BaseVector::wrapInDictionary(nulls, indices, dictionarySize, expected);
    velox::test::assertEqualVectors(expectedResult, evaluate(expr, encodedRow));
  };

  auto testConstant = [&](vector_size_t row) {
    std::vector<VectorPtr> constantInputs;
    for (const auto& input : inputs) {
      constantInputs.push_back(BaseVector::wrapInConstant(100, row, input));
    }
    auto constantRow = makeRowVector(constantInputs);

    SCOPED_TRACE(fmt::format("Constant: {}", constantRow->toString()));

    auto expectedResult = BaseVector::wrapInConstant(100, row, expected);
    velox::test::assertEqualVectors(
        expectedResult, evaluate(expr, constantRow));
  };

  SCOPED_TRACE(expr->toString());

  // No extra encoding.
  velox::test::assertEqualVectors(
      expected, evaluate(expr, makeRowVector(inputs)));

  // Repeat each row twice: 0, 0, 1, 1,... No extra nulls.
  testDictionary(size * 2, [](auto row) { return row / 2; });

  // Select even rows: 0, 2, 4,... No extra nulls.
  testDictionary(size / 2, [](auto row) { return row * 2; });

  // Go over all rows in reverse: N, N-1, N-2,...0. No extra nulls.
  testDictionary(size, [&](auto row) { return size - 1 - row; });

  // Repeat each row twice: 0, 0, 1, 1,... Add some nulls.
  testDictionary(size * 2, [](auto row) { return row / 2; }, nullEvery(3));

  // Select even rows: 0, 2, 4,... Add some nulls.
  testDictionary(size / 2, [](auto row) { return row * 2; }, nullEvery(3));

  // Go over all rows in reverse: N, N-1, N-2,...0. Add some nulls.
  testDictionary(size, [&](auto row) { return size - 1 - row; }, nullEvery(3));

  // Generate constant vectors and verify the results.
  for (auto i = 0; i < size; ++i) {
    testConstant(i);
  }
}

} // namespace facebook::velox::functions::test
