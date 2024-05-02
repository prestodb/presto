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
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::fuzzer {

// A tool that can be used to generate random expressions.
class ExpressionFuzzer {
 public:
  struct Options {
    // The maximum number of variadic arguments fuzzer will generate for
    // functions that accept variadic arguments. Fuzzer will generate up to
    // max_num_varargs arguments for the variadic list in addition to the
    // required arguments by the function.
    int32_t maxNumVarArgs = 5;

    // Enable testing of function signatures with variadic arguments.
    bool enableVariadicSignatures = false;

    // Allow fuzzer to generate random expressions with dereference and
    // row_constructor functions.
    bool enableDereference = false;

    // Enable testing of function signatures with complex argument or return
    // types.
    bool enableComplexTypes = false;

    // Enable generation of expressions where one input column can be used by
    // multiple subexpressions.
    bool enableColumnReuse = false;

    // Enable generation of expressions that re-uses already generated
    // subexpressions.
    bool enableExpressionReuse = false;

    int32_t maxLevelOfNesting = 10;

    //  Comma separated list of function names and their tickets in the format
    //  <function_name>=<tickets>. Every ticket represents an opportunity for a
    //  function to be chosen from a pool of candidates. By default, every
    //  function has one ticket, and the likelihood of a function being picked
    //  an be increased by allotting it more tickets. Note that in practice,
    //  increasing the number of tickets does not proportionally increase the
    //  likelihood of selection, as the selection process involves filtering the
    //  pool of candidates by a required return type so not all functions may
    //  compete against the same number of functions at every instance. Number
    //  of tickets must be a positive integer. Example: eq=3,floor=5
    std::string functionTickets = "";

    // Chance of adding a null constant to the plan, or null value in a vector
    // (expressed as double from 0 to 1).
    double nullRatio = 0.1;

    // If specified, Fuzzer will only choose functions from this comma separated
    // list of function names (e.g: --only \"split\" or --only
    // \"substr,ltrim\")."
    std::string useOnlyFunctions = "";

    // Comma-separated list of special forms to use in generated expression.
    // Supported special forms: and, or, coalesce, if, switch, cast.")
    std::string specialForms = "and,or,cast,coalesce,if,switch";

    // This list can include a mix of function names and function signatures.
    // Use function name to exclude all signatures of a given function from
    // testing. Use function signature to exclude only a specific signature.
    // ex skipFunctions{
    //   "width_bucket",
    //   "array_sort(array(T),constant function(T,T,bigint)) -> array(T)"}
    std::unordered_set<std::string> skipFunctions;

    // When set, when the input size of the generated expressions reaches
    // maxInputsThreshold, fuzzing input columns will reuse one of the existing
    // columns if any is already generated with the same type.
    // This can be used to control the size of the input of the fuzzer
    // expression.
    std::optional<int32_t> maxInputsThreshold = std::nullopt;
  };

  ExpressionFuzzer(
      FunctionSignatureMap signatureMap,
      size_t initialSeed,
      const std::shared_ptr<VectorFuzzer>& vectorFuzzer,
      const std::optional<ExpressionFuzzer::Options>& options = std::nullopt);

  template <typename TFunc>
  void registerFuncOverride(TFunc func, const std::string& name);

  struct FuzzedExpressionData {
    // A list of generated expressions.
    std::vector<core::TypedExprPtr> expressions;

    // The input vector type that is expected by the generated expressions.
    RowTypePtr inputType;

    // Count how many times each expression has been selected in expressions.
    std::unordered_map<std::string, size_t> selectionStats;
  };

  /// Fuzz a set of expressions.
  FuzzedExpressionData fuzzExpressions(size_t expressionCount);

  /// Fuzz a set of expressions given a output row type.
  FuzzedExpressionData fuzzExpressions(const RowTypePtr& outType);

  // Fuzz a single expression and return it along with the input row type.
  FuzzedExpressionData fuzzExpression();

  /// Used to enable re-use of sub-expressions by exposing an API that allows
  /// for randomly picking an expression that has a specific return type and a
  /// nesting level less than or equal to a specified limit. It ensures that
  /// all expressions that are valid candidates have an equal probability of
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

  void seed(size_t seed);

  const std::vector<std::string>& supportedFunctions() const {
    return supportedFunctions_;
  }

  // Generate a random return type.
  TypePtr fuzzReturnType();

  RowTypePtr fuzzRowReturnType(size_t size, char prefix = 'p');

 private:
  // Either generates a new expression of the required return type or if
  // already generated expressions of the same return type exist then there is
  // a 30% chance that it will re-use one of them.
  core::TypedExprPtr generateExpression(const TypePtr& type);

  enum ArgumentKind { kArgConstant = 0, kArgColumn = 1, kArgExpression = 2 };

  // Parse options.functionTickets into a map that maps function name to its
  // number of tickets.
  void getTicketsForFunctions();

  // Get tickets for one function assigned by the options.functionTickets
  // option. This function should be called after getTicketsForFunctions().
  int getTickets(const std::string& funcName);

  // Add `funcName` that returns `type` to typeToExpressionList_.
  void addToTypeToExpressionListByTicketTimes(
      const std::string& type,
      const std::string& funcName);

  void appendConjunctSignatures();

  core::TypedExprPtr generateArgConstant(const TypePtr& arg);

  core::TypedExprPtr generateArgColumn(const TypePtr& arg);

  core::TypedExprPtr generateArg(const TypePtr& arg);

  // Given lambda argument type, generate matching LambdaTypedExpr.
  //
  // The 'arg' specifies inputs types and result type for the lambda. This
  // method finds all matching signatures and signature templates, picks one
  // randomly and generates LambdaTypedExpr. If no matching signatures or
  // signature templates found, this method returns LambdaTypedExpr that
  // represents a constant lambda, i.e lambda that returns the same value for
  // all input. The constant value is generated using 'generateArgConstant'.
  core::TypedExprPtr generateArgFunction(const TypePtr& arg);

  std::vector<core::TypedExprPtr> generateArgs(const CallableSignature& input);

  std::vector<core::TypedExprPtr> generateArgs(
      const std::vector<TypePtr>& argTypes,
      const std::vector<bool>& constantArgs,
      uint32_t numVarArgs = 0);

  core::TypedExprPtr generateArg(const TypePtr& arg, bool isConstant);

  // Return a vector of expressions for each argument of callable in order.
  std::vector<core::TypedExprPtr> getArgsForCallable(
      const CallableSignature& callable);

  /// Specialization for the "switch" function. Takes in a signature that is
  /// of the form Switch (condition, then): boolean, T -> T where the type
  /// variable is bounded to a randomly selected type. It randomly decides the
  /// number of cases (upto a max of 5) to generate and whether to include the
  /// else clause. Finally, uses the type specified in the signature to
  /// generate inputs with that return type.
  std::vector<core::TypedExprPtr> generateSwitchArgs(
      const CallableSignature& input);

  core::TypedExprPtr getCallExprFromCallable(
      const CallableSignature& callable,
      const TypePtr& type);

  /// Return a random signature mapped to functionName in
  /// expressionToSignature_ whose return type can match returnType. Return
  /// nullptr if no such signature template exists.
  const CallableSignature* chooseRandomConcreteSignature(
      const TypePtr& returnType,
      const std::string& functionName);

  /// Returns a signature with matching input types and return type. Returns
  /// nullptr if matching signature doesn't exist.
  const CallableSignature* findConcreteSignature(
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType,
      const std::string& functionName);

  /// Generate an expression by randomly selecting a concrete function
  /// signature that returns 'returnType' among all signatures that the
  /// function named 'functionName' supports.
  core::TypedExprPtr generateExpressionFromConcreteSignatures(
      const TypePtr& returnType,
      const std::string& functionName);

  /// Return a random signature template mapped to typeName and functionName
  /// in expressionToTemplatedSignature_ whose return type can match
  /// returnType. Return nullptr if no such signature template exists.
  const SignatureTemplate* chooseRandomSignatureTemplate(
      const TypePtr& returnType,
      const std::string& typeName,
      const std::string& functionName);

  /// Returns a signature template with matching input types and return type.
  /// Returns nullptr if matching signature template doesn't exist.
  const SignatureTemplate* findSignatureTemplate(
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType,
      const std::string& typeName,
      const std::string& functionName);

  /// Generate an expression by randomly selecting a function signature
  /// template that returns 'returnType' among all signature templates that
  /// the function named 'functionName' supports.
  core::TypedExprPtr generateExpressionFromSignatureTemplate(
      const TypePtr& returnType,
      const std::string& functionName);

  /// Generate a cast expression that returns the specified type. Return a
  /// nullptr if casting to the specified type is not supported. The supported
  /// types include primitive types, array, map, and row types right now.
  core::TypedExprPtr generateCastExpression(const TypePtr& returnType);

  // Generate an expression of the row_constructor special form that returns
  // `returnType`. `returnType` must be a RowType.
  core::TypedExprPtr generateRowConstructorExpression(
      const TypePtr& returnType);

  // Generate a random row type with `referencedType` be its field at
  // `referencedIndex`.
  TypePtr generateRandomRowTypeWithReferencedField(
      uint32_t numFields,
      uint32_t referencedIndex,
      const TypePtr& referencedType);

  // Generate an expression of the dereference special form that returns
  // `returnType`.
  core::TypedExprPtr generateDereferenceExpression(const TypePtr& returnType);

  /// Should be called whenever a function is selected by the fuzzer.
  void markSelected(const std::string& funcName) {
    state.expressionStats_[funcName]++;
  }

  // Returns random integer between min and max inclusive.
  int32_t rand32(int32_t min, int32_t max);

  static const inline std::string kTypeParameterName = "T";

  const Options options_;

  std::vector<CallableSignature> signatures_;
  std::vector<SignatureTemplate> signatureTemplates_;

  /// Maps the base name of a return type signature to the function names that
  /// support that return type. Base name could be "T" if the return type is a
  /// type variable.
  std::unordered_map<std::string, std::vector<std::string>>
      typeToExpressionList_;

  /// Maps the base name of a *concrete* return type signature to the function
  /// names that support that return type. Those names then each further map
  /// to a list of CallableSignature objects that they support. Base name
  /// could be "T" if the return type is a type variable.
  std::unordered_map<
      std::string,
      std::unordered_map<std::string, std::vector<const CallableSignature*>>>
      expressionToSignature_;

  /// Maps the base name of a *templated* return type signature to the
  /// function names that support that return type. Those names then each
  /// further map to a list of SignatureTemplate objects that they support.
  /// Base name could be "T" if the return type is a type variable.
  std::unordered_map<
      std::string,
      std::unordered_map<std::string, std::vector<const SignatureTemplate*>>>
      expressionToTemplatedSignature_;

  // A map that maps function name to its number of tickets parsed from the
  // --assign_function_tickets startup flag .
  std::unordered_map<std::string, int> functionsToTickets_;

  /// We allow the arg generation routine to be specialized for particular
  /// functions. This map stores the mapping between function name and the
  /// overridden method.
  using ArgsOverrideFunc = std::function<std::vector<core::TypedExprPtr>(
      const CallableSignature& input)>;

  std::unordered_map<std::string, ArgsOverrideFunc> funcArgOverrides_;

  std::shared_ptr<VectorFuzzer> vectorFuzzer_;

  FuzzerGenerator rng_;

  std::vector<std::string> supportedFunctions_;

  struct State {
    void reset() {
      inputRowTypes_.clear();
      inputRowNames_.clear();
      typeToColumnNames_.clear();
      expressionBank_.reset();
      expressionStats_.clear();
    }

    State(FuzzerGenerator& rng, int maxLevelOfNesting)
        : expressionBank_(rng, maxLevelOfNesting),
          remainingLevelOfNesting_(maxLevelOfNesting) {}

    /// Used to track all generated expressions within a single iteration and
    /// support expression re-use.
    ExprBank expressionBank_;

    /// Contains the types and names of the input vector that the generated
    /// expressions consume.
    std::vector<TypePtr> inputRowTypes_;
    std::vector<std::string> inputRowNames_;

    // Count how many times each function has been selected.
    std::unordered_map<std::string, size_t> expressionStats_;

    /// Maps a 'Type' serialized as a string to the column names that have
    /// already been generated. Used to easily look up columns that can be
    /// re-used when a specific type is required as input to a callable.
    std::unordered_map<std::string, std::vector<std::string>>
        typeToColumnNames_;

    /// The remaining levels of expression nesting. It's initialized by
    /// FLAGS_max_level_of_nesting and updated in generateExpression(). When
    /// its value decreases to 0, we don't generate subexpressions anymore.
    int32_t remainingLevelOfNesting_;

  } state;
  friend class ExpressionFuzzerUnitTest;
};

} // namespace facebook::velox::fuzzer
