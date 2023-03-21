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
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/expression/tests/FuzzerToolkit.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DECLARE_int32(velox_fuzzer_max_level_of_nesting);

namespace facebook::velox::test {

// Generates random expressions based on `signatures`, random input data (via
// VectorFuzzer), and executes them.
void expressionFuzzer(FunctionSignatureMap signatureMap, size_t seed);

class ExpressionFuzzer {
 public:
  ExpressionFuzzer(
      FunctionSignatureMap signatureMap,
      size_t initialSeed,
      int32_t maxLevelOfNesting = FLAGS_velox_fuzzer_max_level_of_nesting);

  template <typename TFunc>
  void registerFuncOverride(TFunc func, const std::string& name);

  void go();

  /// Return a random legit expression that returns returnType.
  core::TypedExprPtr generateExpression(const TypePtr& returnType);

  /// Used to enable re-use of sub-expressions by exposing an API that allows
  /// for randomly picking an expression that has a specific return type and a
  /// nesting level less than or equal to a specified limit. It ensures that all
  /// expressions that are valid candidates have an equal probability of
  /// selection.
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

    /// Only expression having less than or equal to this level of nesting will
    /// be supported.
    int maxLevelOfNesting_;

    /// Represents a vector where each index contains a list of expressions such
    /// that the depth of each expression tree is equal to that index.
    typedef std::vector<std::vector<core::TypedExprPtr>> ExprsIndexedByLevel;

    /// Maps a 'Type' serialized as a string to an object of type
    /// ExprsIndexedByLevel
    std::unordered_map<std::string, ExprsIndexedByLevel> typeToExprsByLevel_;
  };

 private:
  struct ExprUsageStats {
    // Num of times the expression was randomly selected.
    int numTimesSelected = 0;
    // Num of rows processed by the expression.
    int numProcessedRows = 0;
  };

  // A utility class used to keep track of stats relevant to the fuzzer.
  class ExprStatsListener : public exec::ExprSetListener {
   public:
    explicit ExprStatsListener(
        std::unordered_map<std::string, ExprUsageStats>& exprNameToStats)
        : exprNameToStats_(exprNameToStats) {}

    void onCompletion(
        const std::string& /*uuid*/,
        const exec::ExprSetCompletionEvent& event) override {
      for (auto& [funcName, stats] : event.stats) {
        auto itr = exprNameToStats_.find(funcName);
        if (itr == exprNameToStats_.end()) {
          // Skip expressions like FieldReference and ConstantExpr
          continue;
        }
        itr->second.numProcessedRows += stats.numProcessedRows;
      }
    }

    // A no-op since we cannot tie errors directly to functions where they
    // occurred.
    void onError(
        const SelectivityVector& /*rows*/,
        const ::facebook::velox::ErrorVector& /*errors*/) override {}

   private:
    std::unordered_map<std::string, ExprUsageStats>& exprNameToStats_;
  };

  const std::string kTypeParameterName = "T";

  enum ArgumentKind { kArgConstant = 0, kArgColumn = 1, kArgExpression = 2 };

  void seed(size_t seed);

  void reSeed();

  void appendConjunctSignatures();

  RowVectorPtr generateRowVector();

  core::TypedExprPtr generateArgConstant(const TypePtr& arg);

  core::TypedExprPtr generateArgColumn(const TypePtr& arg);

  core::TypedExprPtr generateArg(const TypePtr& arg);

  std::vector<core::TypedExprPtr> generateArgs(const CallableSignature& input);

  core::TypedExprPtr generateArg(const TypePtr& arg, bool isConstant);

  /// Specialization for the "like" function: second and third (optional)
  /// parameters always need to be constant.
  std::vector<core::TypedExprPtr> generateLikeArgs(
      const CallableSignature& input);

  /// Specialization for the "empty_approx_set" function: first optional
  /// parameter needs to be constant.
  std::vector<core::TypedExprPtr> generateEmptyApproxSetArgs(
      const CallableSignature& input);

  /// Specialization for the "regexp_replace" function: second and third
  /// (optional) parameters always need to be constant.
  std::vector<core::TypedExprPtr> generateRegexpReplaceArgs(
      const CallableSignature& input);

  // Return a vector of expressions for each argument of callable in order.
  std::vector<core::TypedExprPtr> getArgsForCallable(
      const CallableSignature& callable);

  /// Specialization for the "switch" function. Takes in a signature that is of
  /// the form Switch (condition, then): boolean, T -> T where the type variable
  /// is bounded to a randomly selected type. It randomly decides the number
  /// of cases (upto a max of 5) to generate and whether to include the else
  /// clause. Finally, uses the type specified in the signature to generate
  /// inputs with that return type.
  std::vector<core::TypedExprPtr> generateSwitchArgs(
      const CallableSignature& input);

  core::TypedExprPtr getCallExprFromCallable(const CallableSignature& callable);

  /// Return a random signature mapped to functionName in expressionToSignature_
  /// whose return type can match returnType. Return nullptr if no such
  /// signature template exists.
  const CallableSignature* chooseRandomConcreteSignature(
      const TypePtr& returnType,
      const std::string& functionName);

  /// Generate an expression by randomly selecting a concrete function signature
  /// that returns 'returnType' among all signatures that the function named
  /// 'functionName' supports.
  core::TypedExprPtr generateExpressionFromConcreteSignatures(
      const TypePtr& returnType,
      const std::string& functionName);

  /// Return a random signature template mapped to typeName and functionName in
  /// expressionToTemplatedSignature_ whose return type can match returnType.
  /// Return nullptr if no such signature template exists.
  const SignatureTemplate* chooseRandomSignatureTemplate(
      const TypePtr& returnType,
      const std::string& typeName,
      const std::string& functionName);

  /// Generate an expression by randomly selecting a function signature template
  /// that returns 'returnType' among all signature templates that the function
  /// named 'functionName' supports.
  core::TypedExprPtr generateExpressionFromSignatureTemplate(
      const TypePtr& returnType,
      const std::string& functionName);

  /// Generate a cast expression that returns the specified type. Return a
  /// nullptr if casting to the specified type is not supported. The supported
  /// types include primitive types, array, map, and row types right now.
  core::TypedExprPtr generateCastExpression(const TypePtr& returnType);

  /// If --duration_sec > 0, check if we expired the time budget. Otherwise,
  /// check if we expired the number of iterations (--steps).
  template <typename T>
  bool isDone(size_t i, T startTime) const;

  /// Reset any stateful members. Should be called before every fuzzer
  /// iteration.
  void reset();

  /// Should be called whenever a function is selected by the fuzzer.
  void markSelected(const std::string& funcName) {
    exprNameToStats_[funcName].numTimesSelected++;
  }

  /// Called at the end of a successful fuzzer run. It logs the top and bottom
  /// 10 functions based on the num of rows processed by them. Also logs a full
  /// list of all functions sorted in descending order by the num of times they
  /// were selected by the fuzzer. Every logged function contains the
  /// information in the following format which can be easily exported to a
  /// spreadsheet for further analysis: functionName numTimesSelected
  /// proportionOfTimesSelected numProcessedRows.
  void logStats();

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::vector<CallableSignature> signatures_;
  std::vector<SignatureTemplate> signatureTemplates_;

  /// Maps the base name of a return type signature to the function names that
  /// support that return type. Base name could be "T" if the return type is a
  /// type variable.
  std::unordered_map<std::string, std::vector<std::string>>
      typeToExpressionList_;

  /// Maps the base name of a *concrete* return type signature to the function
  /// names that support that return type. Those names then each further map to
  /// a list of CallableSignature objects that they support. Base name could be
  /// "T" if the return type is a type variable.
  std::unordered_map<
      std::string,
      std::unordered_map<std::string, std::vector<const CallableSignature*>>>
      expressionToSignature_;

  /// Maps the base name of a *templated* return type signature to the function
  /// names that support that return type. Those names then each further map to
  /// a list of SignatureTemplate objects that they support. Base name could be
  /// "T" if the return type is a type variable.
  std::unordered_map<
      std::string,
      std::unordered_map<std::string, std::vector<const SignatureTemplate*>>>
      expressionToTemplatedSignature_;

  /// The remaining levels of expression nesting. It's initialized by
  /// FLAGS_max_level_of_nesting and updated in generateExpression(). When its
  /// value decreases to 0, we don't generate subexpressions anymore.
  int32_t remainingLevelOfNesting_;

  /// We allow the arg generation routine to be specialized for particular
  /// functions. This map stores the mapping between function name and the
  /// overridden method.
  using ArgsOverrideFunc = std::function<std::vector<core::TypedExprPtr>(
      const CallableSignature& input)>;
  std::unordered_map<std::string, ArgsOverrideFunc> funcArgOverrides_;

  std::shared_ptr<core::QueryCtx> queryCtx_{std::make_shared<core::QueryCtx>()};
  std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  test::ExpressionVerifier verifier_;

  test::VectorMaker vectorMaker_{execCtx_.pool()};
  VectorFuzzer vectorFuzzer_;

  /// Contains the input column references that need to be generated for one
  /// particular iteration.
  std::vector<TypePtr> inputRowTypes_;
  std::vector<std::string> inputRowNames_;

  /// Maps a 'Type' serialized as a string to the column names that have already
  /// been generated. Used to easily look up columns that can be re-used when a
  /// specific type is required as input to a callable.
  std::unordered_map<std::string, std::vector<std::string>> typeToColumnNames_;

  /// Used to track all generated expressions and support expreesion re-use.
  ExprBank expressionBank_;

  std::shared_ptr<ExprStatsListener> statListener_;
  std::unordered_map<std::string, ExprUsageStats> exprNameToStats_;
};

} // namespace facebook::velox::test
