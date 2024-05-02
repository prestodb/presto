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
#pragma once

#include "velox/expression/FunctionSignature.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::fuzzer {

// Represents one available function signature.
struct CallableSignature {
  // Function name.
  std::string name;

  // Input arguments and return type.
  std::vector<TypePtr> args;
  bool variableArity{false};
  TypePtr returnType;

  // Boolean flags identifying constant arguments.
  std::vector<bool> constantArgs;

  // Convenience print function.
  std::string toString() const;
};

struct SignatureTemplate {
  std::string name;
  const exec::FunctionSignature* signature;
  std::unordered_set<std::string> typeVariables;
};

struct ResultOrError {
  RowVectorPtr result;
  std::exception_ptr exceptionPtr;
};

/// Sort callable function signatures.
void sortCallableSignatures(std::vector<CallableSignature>& signatures);

/// Sort function signature templates.
void sortSignatureTemplates(std::vector<SignatureTemplate>& signatures);

void compareExceptions(
    std::exception_ptr exceptionPtr,
    std::exception_ptr otherExceptionPr);

/// Parse the comma separated list of function names, and use it to filter the
/// input signatures. Return a signature map that (1) only include functions
/// appearing in onlyFunctions if onlyFunctions is non-empty, and (2) not
/// include any functions appearing in skipFunctions if skipFunctions is
/// non-empty.
/// @tparam SignatureMapType can be AggregateFunctionSignatureMap or
/// WindowFunctionMap.
template <typename SignatureMapType>
SignatureMapType filterSignatures(
    const SignatureMapType& input,
    const std::string& onlyFunctions,
    const std::unordered_set<std::string>& skipFunctions) {
  if (onlyFunctions.empty() && skipFunctions.empty()) {
    return input;
  }

  SignatureMapType output;
  if (!onlyFunctions.empty()) {
    // Parse, lower case and trim it.
    std::vector<folly::StringPiece> nameList;
    folly::split(',', onlyFunctions, nameList);
    std::unordered_set<std::string> nameSet;
    for (const auto& it : nameList) {
      auto str = folly::trimWhitespace(it).toString();
      folly::toLowerAscii(str);
      nameSet.insert(str);
    }

    for (const auto& it : input) {
      if (nameSet.count(it.first) > 0) {
        output.insert(it);
      }
    }
  } else {
    output = input;
  }

  for (auto s : skipFunctions) {
    auto str = s;
    folly::toLowerAscii(str);
    output.erase(str);
  }
  return output;
}
// Compare two vectors for selected rows and throw on mismatch.
// If rows not provided, left and right are checked to have the same size, and
// all rows are compared.
void compareVectors(
    const VectorPtr& left,
    const VectorPtr& right,
    const std::string& leftName = "left",
    const std::string& rightName = "right",
    const std::optional<SelectivityVector>& rows = std::nullopt);
} // namespace facebook::velox::fuzzer
