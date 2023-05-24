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

} // namespace facebook::velox::functions::test
