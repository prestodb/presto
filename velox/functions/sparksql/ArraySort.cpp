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
#include "velox/functions/sparksql/ArraySort.h"
#include "velox/functions/sparksql/SimpleComparisonMatcher.h"

namespace facebook::velox::functions::sparksql {
std::shared_ptr<exec::VectorFunction> makeArraySortAsc(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  // If the second argument is present, it must be a lambda.
  if (inputArgs.size() == 2) {
    return makeArraySortLambdaFunction(name, inputArgs, config, true, false);
  }

  VELOX_CHECK_EQ(inputArgs.size(), 1);
  // Nulls are considered largest.
  return facebook::velox::functions::makeArraySort(
      name,
      inputArgs,
      config,
      true /*ascending*/,
      false /*nullsFirst*/,
      false /*throwOnNestedNull*/);
}

std::shared_ptr<exec::VectorFunction> makeArraySortDesc(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  return makeArraySortLambdaFunction(name, inputArgs, config, false, false);
}

// Signatures:
//   array_sort_desc(array(T), function(T,U)) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>>
arraySortDescSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .orderableTypeVariable("T")
          .orderableTypeVariable("U")
          .returnType("array(T)")
          .argumentType("array(T)")
          .constantArgumentType("function(T,U)")
          .build(),
  };
}

// Signatures:
//   sort_array(array(T)) -> array(T)
//   sort_array(array(T), boolean) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>> sortArraySignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .orderableTypeVariable("T")
          .argumentType("array(T)")
          .returnType("array(T)")
          .build(),
      exec::FunctionSignatureBuilder()
          .orderableTypeVariable("T")
          .argumentType("array(T)")
          .constantArgumentType("boolean")
          .returnType("array(T)")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeSortArray(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  VELOX_CHECK(
      inputArgs.size() == 1 || inputArgs.size() == 2,
      "Invalid number of arguments {}, expected 1 or 2",
      inputArgs.size());
  bool ascending = true;
  // Read optional sort ascending flag.
  if (inputArgs.size() == 2) {
    BaseVector* boolVector = inputArgs[1].constantValue.get();
    if (!boolVector || !boolVector->isConstantEncoding()) {
      VELOX_USER_FAIL(
          "{} requires a constant bool as the second argument.", name);
    }
    ascending = boolVector->as<ConstantVector<bool>>()->valueAt(0);
  }
  // Nulls are considered smallest.
  bool nullsFirst = ascending;
  return facebook::velox::functions::makeArraySort(
      name,
      inputArgs,
      config,
      ascending /*ascending*/,
      nullsFirst /*nullsFirst*/,
      false /*throwOnNestedNull*/);
}

} // namespace facebook::velox::functions::sparksql
