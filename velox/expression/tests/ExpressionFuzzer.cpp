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

#include <folly/Random.h>
#include <glog/logging.h>
#include <exception>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/expression/tests/ExpressionFuzzer.h"
#include "velox/type/Type.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorMaker.h"

DEFINE_int32(steps, 10, "Number of expressions to generate and execute.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null constant to the plan, or null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_bool(
    retry_with_try,
    false,
    "Retry failed expressions by wrapping it using a try() statement.");

DEFINE_bool(
    disable_constant_folding,
    false,
    "Disable constant-folding in the common evaluation path.");

namespace facebook::velox::test {

namespace {

using exec::SignatureBinder;

void compareExceptions(const VeloxException& ve1, const VeloxException& ve2) {
  // Error messages sometimes differ; check at least error codes.
  // Since the common path may peel the input encoding off, whereas the
  // simplified path flatten input vectors, the common and the simplified
  // paths may evaluate the input rows in different orders and hence throw
  // different exceptions depending on which bad input they come across
  // first. We have seen this happen for the format_datetime Presto function
  // that leads to unmatched error codes UNSUPPORTED vs. INVALID_ARGUMENT.
  // Therefore, we intentionally relax the comparision here.
  VELOX_CHECK(
      ve1.errorCode() == ve2.errorCode() ||
      (ve1.errorCode() == "INVALID_ARGUMENT" &&
       ve2.errorCode() == "UNSUPPORTED") ||
      (ve2.errorCode() == "INVALID_ARGUMENT" &&
       ve1.errorCode() == "UNSUPPORTED"));
  VELOX_CHECK_EQ(ve1.errorSource(), ve2.errorSource());
  VELOX_CHECK_EQ(ve1.exceptionName(), ve2.exceptionName());
  if (ve1.message() != ve2.message()) {
    LOG(WARNING) << "Two different VeloxExceptions were thrown:\n\t"
                 << ve1.message() << "\nand\n\t" << ve2.message();
  }
}

// Called if at least one of the ptrs has an exception.
void compareExceptions(
    std::exception_ptr commonPtr,
    std::exception_ptr simplifiedPtr) {
  // If we don't have two exceptions, fail.
  if (!commonPtr || !simplifiedPtr) {
    if (!commonPtr) {
      LOG(ERROR) << "Only simplified path threw exception:";
      std::rethrow_exception(simplifiedPtr);
    }
    LOG(ERROR) << "Only common path threw exception:";
    std::rethrow_exception(commonPtr);
  }

  // Otherwise, make sure the exceptions are the same.
  try {
    std::rethrow_exception(commonPtr);
  } catch (const VeloxException& ve1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const VeloxException& ve2) {
      compareExceptions(ve1, ve2);
      return;
    } catch (const std::exception& e2) {
      LOG(WARNING) << "Two different exceptions were thrown:\n\t"
                   << ve1.message() << "\nand\n\t" << e2.what();
    }
  } catch (const std::exception& e1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const std::exception& e2) {
      if (e1.what() != e2.what()) {
        LOG(WARNING) << "Two different std::exceptions were thrown:\n\t"
                     << e1.what() << "\nand\n\t" << e2.what();
      }
      return;
    }
  }
  VELOX_FAIL("Got two incompatible exceptions.");
}

void compareVectors(const VectorPtr& vec1, const VectorPtr& vec2) {
  VELOX_CHECK_EQ(vec1->size(), vec2->size());

  // Print vector contents if in verbose mode.
  size_t vectorSize = vec1->size();
  if (VLOG_IS_ON(1)) {
    LOG(INFO) << "== Result contents (common vs. simple): ";
    for (auto i = 0; i < vectorSize; i++) {
      LOG(INFO) << "At " << i << ": [" << vec1->toString(i) << " vs "
                << vec2->toString(i) << "]";
    }
    LOG(INFO) << "===================";
  }

  for (auto i = 0; i < vectorSize; i++) {
    VELOX_CHECK(
        vec1->equalValueAt(vec2.get(), i, i),
        "Different results at idx '{}': '{}' vs. '{}'",
        i,
        vec1->toString(i),
        vec2->toString(i));
  }
  LOG(INFO) << "All results match.";
}

/// Returns if `functionName` with the given `argTypes` is deterministic.
/// Returns std::nullopt if the function was not found.
std::optional<bool> isDeterministic(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  // Check if this is a simple function.
  if (auto simpleFunctionEntry =
          exec::SimpleFunctions().resolveFunction(functionName, argTypes)) {
    return simpleFunctionEntry->getMetadata()->isDeterministic();
  }

  // Vector functions are a bit more complicated. We need to fetch the list of
  // available signatures and check if any of them bind given the current input
  // arg types. If it binds (if there's a match), we fetch the function and
  // return the isDeterministic bool.
  try {
    if (auto vectorFunctionSignatures =
            exec::getVectorFunctionSignatures(functionName)) {
      for (const auto& signature : *vectorFunctionSignatures) {
        if (exec::SignatureBinder(*signature, argTypes).tryBind()) {
          if (auto vectorFunction =
                  exec::getVectorFunction(functionName, argTypes, {})) {
            return vectorFunction->isDeterministic();
          }
        }
      }
    }
  }
  // TODO: Some stateful functions can only be built when constant arguments are
  // passed, making the getVectorFunction() call above to throw. We only have a
  // few of these functions, so for now we assume they are deterministic so they
  // are picked for Fuzz testing. Once we make the isDeterministic() flag static
  // (and hence we won't need to build the function object in here) we can clean
  // up this code.
  catch (const std::exception& e) {
    LOG(WARNING) << "Unable to determine if '" << functionName
                 << "' is deterministic or not. Assuming it is.";
    return true;
  }
  return std::nullopt;
}

VectorFuzzer::Options getFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_batch_size;
  opts.stringVariableLength = true;
  opts.stringLength = 100;
  opts.nullRatio = FLAGS_null_ratio;
  return opts;
}

// Represents one available function signature.
struct CallableSignature {
  // Function name.
  std::string name;

  // Input arguments and return type.
  std::vector<TypePtr> args;
  TypePtr returnType;

  // Convenience print function.
  std::string toString() const {
    std::string buf = name;
    buf.append("( ");
    for (const auto& arg : args) {
      buf.append(arg->toString());
      buf.append(" ");
    }
    buf.append(") -> ");
    buf.append(returnType->toString());
    return buf;
  }
};

std::optional<CallableSignature> processSignature(
    const std::string& functionName,
    const exec::FunctionSignature& signature) {
  // Don't support functions with parametrized signatures or variable number of
  // arguments yet.
  if (!signature.typeVariableConstants().empty() || signature.variableArity() ||
      !signature.variables().empty()) {
    LOG(WARNING) << "Skipping unsupported signature: " << functionName
                 << signature.toString();
    return std::nullopt;
  }

  CallableSignature callable{
      .name = functionName,
      .args = {},
      .returnType =
          SignatureBinder::tryResolveType(signature.returnType(), {})};
  VELOX_CHECK_NOT_NULL(callable.returnType);

  // For now, ensure that this function only takes (and returns) primitives
  // types.
  bool onlyPrimitiveTypes = callable.returnType->isPrimitiveType();

  // Process each argument and figure out its type.
  for (const auto& arg : signature.argumentTypes()) {
    auto resolvedType = SignatureBinder::tryResolveType(arg, {});

    // TODO: Check if any input is Generic and substitute all
    // possible primitive types, creating a list of signatures to fuzz.
    if (!resolvedType) {
      LOG(WARNING) << "Skipping unsupported signature with generic: "
                   << functionName << signature.toString();
      return std::nullopt;
    }

    onlyPrimitiveTypes &= resolvedType->isPrimitiveType();
    callable.args.emplace_back(resolvedType);
  }

  if (onlyPrimitiveTypes) {
    if (isDeterministic(callable.name, callable.args).value()) {
      return callable;
    } else {
      LOG(WARNING) << "Skipping non-deterministic function: "
                   << callable.toString();
    }
  } else {
    LOG(WARNING) << "Skipping '" << callable.toString()
                 << "' because it contains non-primitive types.";
  }
  return std::nullopt;
}

class ExpressionFuzzer {
 public:
  ExpressionFuzzer(FunctionSignatureMap signatureMap, size_t initialSeed)
      : vectorFuzzer_(getFuzzerOptions(), execCtx_.pool()) {
    seed(initialSeed);

    // Process each available signature for every function.
    for (const auto& function : signatureMap) {
      for (const auto& signature : function.second) {
        if (auto callableFunction =
                processSignature(function.first, *signature)) {
          signatures_.emplace_back(*callableFunction);
        }
      }
    }

    // We sort the available signatures to ensure we can deterministically
    // generate expressions across platforms. We just do this once and the
    // vector is small, so it doesn't need to be very efficient.
    std::sort(
        signatures_.begin(),
        signatures_.end(),
        // Returns true if lhs is less (comes before).
        [](const CallableSignature& lhs, const CallableSignature& rhs) {
          // The comparison logic is the following:
          //
          // 1. Compare based on function name.
          // 2. If names are the same, compare the number of args.
          // 3. If number of args are the same, look for any different arg
          // types.
          // 4. If all arg types are the same, compare return type.
          if (lhs.name == rhs.name) {
            if (lhs.args.size() == rhs.args.size()) {
              for (size_t i = 0; i < lhs.args.size(); ++i) {
                if (!lhs.args[i]->kindEquals(rhs.args[i])) {
                  return lhs.args[i]->toString() < rhs.args[i]->toString();
                }
              }
              return lhs.returnType->toString() < rhs.returnType->toString();
            }
            return lhs.args.size() < rhs.args.size();
          }
          return lhs.name < rhs.name;
        });

    // Generates signaturesMap, which maps a given type to the function
    // signature that returns it.
    for (const auto& it : signatures_) {
      signaturesMap_[it.returnType->kind()].push_back(&it);
    }

    // Register function override (for cases where we want to restrict the types
    // or parameters we pass to functions).
    registerFuncOverride(&ExpressionFuzzer::generateLikeArgs, "like");
    registerFuncOverride(
        &ExpressionFuzzer::generateEmptyApproxSetArgs, "empty_approx_set");
    registerFuncOverride(
        &ExpressionFuzzer::generateRegexpReplaceArgs, "regexp_replace");
  }

  template <typename TFunc>
  void registerFuncOverride(TFunc func, const std::string& name) {
    funcArgOverrides_[name] = std::bind(func, this, std::placeholders::_1);
  }

 private:
  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(folly::Random::rand32(rng_));
  }

  void printRowVector(const RowVectorPtr& rowVector) {
    LOG(INFO) << "RowVector contents (" << rowVector->type()->toString()
              << "):";

    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      LOG(INFO) << "\tAt " << i << ": " << rowVector->toString(i);
    }
  }

  RowVectorPtr generateRowVector() {
    return vectorFuzzer_.fuzzRow(
        ROW(std::move(inputRowNames_), std::move(inputRowTypes_)));
  }

  core::TypedExprPtr generateArgConstant(const TypePtr& arg) {
    // 10% of times return a NULL constant.
    if (vectorFuzzer_.coinToss(FLAGS_null_ratio)) {
      return std::make_shared<core::ConstantTypedExpr>(
          variant::null(arg->kind()));
    }
    return std::make_shared<core::ConstantTypedExpr>(
        vectorFuzzer_.randVariant(arg));
  }

  core::TypedExprPtr generateArgColumn(const TypePtr& arg) {
    inputRowTypes_.emplace_back(arg);
    inputRowNames_.emplace_back(fmt::format("c{}", inputRowTypes_.size() - 1));

    return std::make_shared<core::FieldAccessTypedExpr>(
        arg, inputRowNames_.back());
  }

  core::TypedExprPtr generateArg(const TypePtr& arg) {
    size_t argClass = folly::Random::rand32(3, rng_);

    // Toss a coin and choose between a constant, a column reference, or another
    // expression (function).
    //
    // TODO: Add more expression types:
    // - Conjunctions
    // - IF/ELSE/SWITCH
    // - Lambdas
    // - Try
    if (argClass == 0) {
      return generateArgConstant(arg);
    } else if (argClass == 1) {
      return generateArgColumn(arg);
    } else if (argClass == 2) {
      return generateExpression(arg);
    } else {
      VELOX_UNREACHABLE();
    }
  }

  std::vector<core::TypedExprPtr> generateArgs(const CallableSignature& input) {
    std::vector<core::TypedExprPtr> inputExpressions;
    inputExpressions.reserve(input.args.size());

    for (const auto& arg : input.args) {
      inputExpressions.emplace_back(generateArg(arg));
    }
    return inputExpressions;
  }

  // Specialization for the "like" function: second and third (optional)
  // parameters always need to be constant.
  std::vector<core::TypedExprPtr> generateLikeArgs(
      const CallableSignature& input) {
    std::vector<core::TypedExprPtr> inputExpressions = {
        generateArg(input.args[0]), generateArgConstant(input.args[1])};
    if (input.args.size() == 3) {
      inputExpressions.emplace_back(generateArgConstant(input.args[2]));
    }
    return inputExpressions;
  }

  // Specialization for the "empty_approx_set" function: first optional
  // parameter needs to be constant.
  std::vector<core::TypedExprPtr> generateEmptyApproxSetArgs(
      const CallableSignature& input) {
    if (input.args.empty()) {
      return {};
    }
    return {generateArgConstant(input.args[0])};
  }

  // Specialization for the "regexp_replace" function: second and third
  // (optional) parameters always need to be constant.
  std::vector<core::TypedExprPtr> generateRegexpReplaceArgs(
      const CallableSignature& input) {
    std::vector<core::TypedExprPtr> inputExpressions = {
        generateArg(input.args[0]), generateArgConstant(input.args[1])};
    if (input.args.size() == 3) {
      inputExpressions.emplace_back(generateArgConstant(input.args[2]));
    }
    return inputExpressions;
  }

  core::TypedExprPtr generateExpression(const TypePtr& returnType) {
    // If no functions can return `returnType`, return a constant instead.
    auto it = signaturesMap_.find(returnType->kind());
    if (it == signaturesMap_.end()) {
      LOG(INFO) << "Couldn't find any function to return '"
                << returnType->toString() << "'. Returning a constant instead.";
      return generateArgConstant(returnType);
    }

    // Randomly pick a function that can return `returnType`.
    const auto& eligible = it->second;
    size_t idx = folly::Random::rand32(eligible.size(), rng_);
    const auto& chosen = eligible[idx];

    // Generate the function args recursively.
    auto funcIt = funcArgOverrides_.find(chosen->name);

    auto args = funcIt == funcArgOverrides_.end() ? generateArgs(*chosen)
                                                  : funcIt->second(*chosen);

    return std::make_shared<core::CallTypedExpr>(
        chosen->returnType, args, chosen->name);
  }

  // Executes an expression. Returns:
  //
  //  - true if both succeeded and returned the exact same results.
  //  - false if both failed with compatible exceptions.
  //  - throws otherwise (incompatible exceptions or different results).
  bool executeExpression(
      const core::TypedExprPtr& plan,
      const RowVectorPtr& rowVector,
      bool canThrow) {
    LOG(INFO) << "Executing expression: " << plan->toString();

    if (rowVector) {
      LOG(INFO) << rowVector->childrenSize() << " vectors as input:";
      for (const auto& child : rowVector->children()) {
        LOG(INFO) << "\t" << child->toString();
      }

      if (VLOG_IS_ON(1)) {
        printRowVector(rowVector);
      }
    }

    // Execute expression plan using both common and simplified evals.
    std::vector<VectorPtr> commonEvalResult(1);
    std::vector<VectorPtr> simplifiedEvalResult(1);

    std::exception_ptr exceptionCommonPtr;
    std::exception_ptr exceptionSimplifiedPtr;

    VLOG(1) << "Starting common eval execution.";
    SelectivityVector rows{rowVector ? rowVector->size() : 1};

    // Execute with common expression eval path.
    try {
      exec::ExprSet exprSetCommon(
          {plan}, &execCtx_, !FLAGS_disable_constant_folding);
      exec::EvalCtx evalCtxCommon(&execCtx_, &exprSetCommon, rowVector.get());

      try {
        exprSetCommon.eval(rows, &evalCtxCommon, &commonEvalResult);
      } catch (...) {
        if (!canThrow) {
          LOG(ERROR)
              << "Common eval wasn't supposed to throw, but it did. Aborting.";
          throw;
        }
        exceptionCommonPtr = std::current_exception();
      }
    } catch (...) {
      exceptionCommonPtr = std::current_exception();
    }

    VLOG(1) << "Starting simplified eval execution.";

    // Execute with simplified expression eval path.
    try {
      exec::ExprSetSimplified exprSetSimplified({plan}, &execCtx_);
      exec::EvalCtx evalCtxSimplified(
          &execCtx_, &exprSetSimplified, rowVector.get());

      try {
        exprSetSimplified.eval(rows, &evalCtxSimplified, &simplifiedEvalResult);
      } catch (...) {
        if (!canThrow) {
          LOG(ERROR)
              << "Simplified eval wasn't supposed to throw, but it did. Aborting.";
          throw;
        }
        exceptionSimplifiedPtr = std::current_exception();
      }
    } catch (...) {
      exceptionSimplifiedPtr = std::current_exception();
    }

    // Compare results or exceptions (if any). Fail is anything is different.
    if (exceptionCommonPtr || exceptionSimplifiedPtr) {
      // Throws in case exceptions are not compatible. If they are compatible,
      // return false to signal that the expression failed.
      compareExceptions(exceptionCommonPtr, exceptionSimplifiedPtr);
      return false;
    } else {
      // Throws in case output is different.
      compareVectors(commonEvalResult.front(), simplifiedEvalResult.front());
    }
    return true;
  }

  // If --duration_sec > 0, check if we expired the time budget. Otherwise,
  // check if we expired the number of iterations (--steps).
  template <typename T>
  bool isDone(size_t i, T startTime) const {
    if (FLAGS_duration_sec > 0) {
      std::chrono::duration<double> elapsed =
          std::chrono::system_clock::now() - startTime;
      return elapsed.count() >= FLAGS_duration_sec;
    }
    return i >= FLAGS_steps;
  }

 public:
  void go() {
    VELOX_CHECK(!signatures_.empty(), "No function signatures available.");
    VELOX_CHECK(
        FLAGS_steps > 0 || FLAGS_duration_sec > 0,
        "Either --steps or --duration_sec needs to be greater than zero.")

    auto startTime = std::chrono::system_clock::now();
    size_t i = 0;

    while (!isDone(i, startTime)) {
      LOG(INFO) << "==============================> Started iteration " << i
                << " (seed: " << currentSeed_ << ")";
      VELOX_CHECK(inputRowTypes_.empty());
      VELOX_CHECK(inputRowNames_.empty());

      // Pick a random signature to chose the root return type.
      size_t idx = folly::Random::rand32(signatures_.size(), rng_);
      const auto& rootType = signatures_[idx].returnType;

      // Generate expression tree and input data vectors.
      auto plan = generateExpression(rootType);
      auto rowVector = generateRowVector();

      // If both paths threw compatible exceptions, we add a try() function to
      // the expression's root and execute it again. This time the expression
      // cannot throw.
      if (!executeExpression(plan, rowVector, true) && FLAGS_retry_with_try) {
        LOG(INFO)
            << "Both paths failed with compatible exceptions. Retrying expression using try().";

        plan = std::make_shared<core::CallTypedExpr>(
            plan->type(), std::vector<core::TypedExprPtr>{plan}, "try");

        // At this point, the function throws if anything goes wrong.
        executeExpression(plan, rowVector, false);
      }

      LOG(INFO) << "==============================> Done with iteration " << i;
      reSeed();
      ++i;
    }
  }

 private:
  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::vector<CallableSignature> signatures_;

  // Maps a given type to the functions that return that type.
  std::unordered_map<TypeKind, std::vector<const CallableSignature*>>
      signaturesMap_;

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

  test::VectorMaker vectorMaker_{execCtx_.pool()};
  VectorFuzzer vectorFuzzer_;

  // Contains the input column references that need to be generated for one
  // particular iteration.
  std::vector<TypePtr> inputRowTypes_;
  std::vector<std::string> inputRowNames_;
};

} // namespace

void expressionFuzzer(FunctionSignatureMap signatureMap, size_t seed) {
  ExpressionFuzzer(std::move(signatureMap), seed).go();
}

} // namespace facebook::velox::test
