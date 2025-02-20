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

#include "velox/core/ITypedExpr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

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

struct InputTestCase {
  RowVectorPtr inputVector;
  SelectivityVector activeRows;
};

struct ResultOrError {
  RowVectorPtr result;
  std::exception_ptr exceptionPtr;
  /// Whether the exception is UNSUPPORTED_INPUT_UNCATCHABLE error. This flag
  /// should only be set to true when exceptionPtr is not a nullptr.
  bool unsupportedInputUncatchableError{false};
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

// Merges a vector of RowVectors into one RowVector.
RowVectorPtr mergeRowVectors(
    const std::vector<RowVectorPtr>& results,
    velox::memory::MemoryPool* pool);

struct InputRowMetadata {
  // Column indices to wrap in LazyVector (in a strictly increasing order)
  std::vector<int> columnsToWrapInLazy;

  // Column indices to wrap in a common dictionary layer (in a strictly
  // increasing order)
  std::vector<int> columnsToWrapInCommonDictionary;

  bool empty() const {
    return columnsToWrapInLazy.empty() &&
        columnsToWrapInCommonDictionary.empty();
  }

  void saveToFile(const char* filePath) const;
  static InputRowMetadata restoreFromFile(
      const char* filePath,
      memory::MemoryPool* pool);
};

/// Used to enable re-use of sub-expressions in expression fuzzer by exposing an
/// API that allows for randomly picking an expression that has a specific
/// return type and a nesting level less than or equal to a specified limit. It
/// ensures that all expressions that are valid candidates have an equal
/// probability of selection.
class ExprBank {
 public:
  ExprBank(FuzzerGenerator& rng, int maxLevelOfNesting)
      : rng_(rng), maxLevelOfNesting_(maxLevelOfNesting) {}

  /// Adds an expression to the bank.
  void insert(const core::TypedExprPtr& expression);

  /// Returns a randomly selected expression of the requested 'returnType'
  /// which is guaranteed to have a nesting level less than or equal to
  /// 'uptoLevelOfNesting'. Returns a nullptr if no such function can be
  /// found.
  core::TypedExprPtr getRandomExpression(
      const TypePtr& returnType,
      int uptoLevelOfNesting);

  /// Removes all the expressions from the bank. Should be called after
  /// every fuzzer iteration.
  void reset() {
    typeToExprsByLevel_.clear();
  }

 private:
  int getNestedLevel(const core::TypedExprPtr& expression) {
    int level = 0;
    for (auto& input : expression->inputs()) {
      level = std::max(level, getNestedLevel(input) + 1);
    }
    return level;
  }

  /// Reference to the random generator of the expression fuzzer.
  FuzzerGenerator& rng_;

  /// Only expression having less than or equal to this level of nesting
  /// will be generated.
  int maxLevelOfNesting_;

  /// Represents a vector where each index contains a list of expressions
  /// such that the depth of each expression tree is equal to that index.
  using ExprsIndexedByLevel = std::vector<std::vector<core::TypedExprPtr>>;

  /// Maps a 'Type' serialized as a string to an object of type
  /// ExprsIndexedByLevel
  std::unordered_map<std::string, ExprsIndexedByLevel> typeToExprsByLevel_;
};

struct ExpressionFuzzerState {
  void reset() {
    inputRowTypes_.clear();
    inputRowNames_.clear();
    typeToColumnNames_.clear();
    expressionBank_.reset();
    expressionStats_.clear();
    customInputGenerators_.clear();
  }

  ExpressionFuzzerState(FuzzerGenerator& rng, int maxLevelOfNesting)
      : expressionBank_(rng, maxLevelOfNesting),
        remainingLevelOfNesting_(maxLevelOfNesting) {}

  /// Used to track all generated expressions within a single iteration and
  /// support expression re-use.
  ExprBank expressionBank_;

  /// Contains the types and names of the input vector that the generated
  /// expressions consume.
  std::vector<TypePtr> inputRowTypes_;
  std::vector<std::string> inputRowNames_;
  /// Contains the custom input generators for the input vectors.
  std::vector<AbstractInputGeneratorPtr> customInputGenerators_;

  // Count how many times each function has been selected.
  std::unordered_map<std::string, size_t> expressionStats_;

  /// Maps a 'Type' serialized as a string to the column names that have
  /// already been generated. Used to easily look up columns that can be
  /// re-used when a specific type is required as input to a callable.
  std::unordered_map<std::string, std::vector<std::string>> typeToColumnNames_;

  /// The remaining levels of expression nesting. It's initialized by
  /// FLAGS_max_level_of_nesting and updated in generateExpression(). When
  /// its value decreases to 0, we don't generate subexpressions anymore.
  int32_t remainingLevelOfNesting_;
};

class ArgValuesGenerator {
 public:
  virtual ~ArgValuesGenerator() = default;

  virtual std::vector<core::TypedExprPtr> generate(
      const CallableSignature& signature,
      const VectorFuzzer::Options& options,
      FuzzerGenerator& rng,
      ExpressionFuzzerState& state) = 0;

 protected:
  void populateInputTypesAndNames(
      const CallableSignature& signature,
      ExpressionFuzzerState& state) {
    for (auto i = 0; i < signature.args.size(); ++i) {
      state.inputRowTypes_.emplace_back(signature.args[i]);
      state.inputRowNames_.emplace_back(
          fmt::format("c{}", state.inputRowTypes_.size() - 1));
    }
  }
};
} // namespace facebook::velox::fuzzer
