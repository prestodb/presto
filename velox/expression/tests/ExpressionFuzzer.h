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
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DECLARE_int32(velox_fuzzer_max_level_of_nesting);

namespace facebook::velox::test {

// Generates random expressions based on `signatures`, random input data (via
// VectorFuzzer), and executes them.
void expressionFuzzer(FunctionSignatureMap signatureMap, size_t seed);

// Represents one available function signature.
struct CallableSignature {
  // Function name.
  std::string name;

  // Input arguments and return type.
  std::vector<TypePtr> args;
  bool variableArity{false};
  TypePtr returnType;

  // Convenience print function.
  std::string toString() const;
};

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

 private:
  enum ArgumentKind { kArgConstant = 0, kArgColumn = 1, kArgExpression = 2 };

  void seed(size_t seed);

  void reSeed();

  void appendConjunctSignatures();

  RowVectorPtr generateRowVector();

  core::TypedExprPtr generateArgConstant(const TypePtr& arg);

  core::TypedExprPtr generateArgColumn(const TypePtr& arg);

  core::TypedExprPtr generateArg(const TypePtr& arg);

  std::vector<core::TypedExprPtr> generateArgs(const CallableSignature& input);

  // Specialization for the "like" function: second and third (optional)
  // parameters always need to be constant.
  std::vector<core::TypedExprPtr> generateLikeArgs(
      const CallableSignature& input);

  // Specialization for the "empty_approx_set" function: first optional
  // parameter needs to be constant.
  std::vector<core::TypedExprPtr> generateEmptyApproxSetArgs(
      const CallableSignature& input);

  // Specialization for the "regexp_replace" function: second and third
  // (optional) parameters always need to be constant.
  std::vector<core::TypedExprPtr> generateRegexpReplaceArgs(
      const CallableSignature& input);

  // If --duration_sec > 0, check if we expired the time budget. Otherwise,
  // check if we expired the number of iterations (--steps).
  template <typename T>
  bool isDone(size_t i, T startTime) const;

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::vector<CallableSignature> signatures_;

  // Maps a given type to the functions that return that type.
  std::unordered_map<TypeKind, std::vector<const CallableSignature*>>
      signaturesMap_;

  // The remaining levels of expression nesting. It's initialized by
  // FLAGS_max_level_of_nesting and updated in generateExpression(). When its
  // value decreases to 0, we don't generate subexpressions anymore.
  int32_t remainingLevelOfNesting_;

  // We allow the arg generation routine to be specialized for particular
  // functions. This map stores the mapping between function name and the
  // overridden method.
  using ArgsOverrideFunc = std::function<std::vector<core::TypedExprPtr>(
      const CallableSignature& input)>;
  std::unordered_map<std::string, ArgsOverrideFunc> funcArgOverrides_;

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  test::ExpressionVerifier verifier_;

  test::VectorMaker vectorMaker_{execCtx_.pool()};
  VectorFuzzer vectorFuzzer_;

  // Contains the input column references that need to be generated for one
  // particular iteration.
  std::vector<TypePtr> inputRowTypes_;
  std::vector<std::string> inputRowNames_;
};

} // namespace facebook::velox::test
