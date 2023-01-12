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

#include <boost/random/uniform_int_distribution.hpp>
#include <folly/ScopeGuard.h>
#include <glog/logging.h>
#include <exception>
#include <unordered_set>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/ReverseSignatureBinder.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/tests/ArgumentTypeFuzzer.h"
#include "velox/expression/tests/ExpressionFuzzer.h"

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

DEFINE_int32(
    max_num_varargs,
    5,
    "The maximum number of variadic arguments fuzzer will generate for "
    "functions that accept variadic arguments. Fuzzer will generate up to "
    "max_num_varargs arguments for the variadic list in addition to the "
    "required arguments by the function.");

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

DEFINE_bool(
    enable_variadic_signatures,
    false,
    "Enable testing of function signatures with variadic arguments.");

DEFINE_bool(enable_cast, false, "Enable testing with cast expression.");

DEFINE_bool(
    choose_root_type_from_signature_template,
    false,
    "Allow choosing the top-level root type from signature templates.");

DEFINE_string(
    repro_persist_path,
    "",
    "Directory path for persistence of data and SQL when fuzzer fails for "
    "future reproduction. Empty string disables this feature.");

DEFINE_bool(
    persist_and_run_once,
    false,
    "Persist repro info before evaluation and only run one iteration. "
    "This is to rerun with the seed number and persist repro info upon a "
    "crash failure. Only effective if repro_persist_path is set.");

DEFINE_int32(
    velox_fuzzer_max_level_of_nesting,
    10,
    "Max levels of expression nesting. The default value is 10 and minimum is 1.");

DEFINE_bool(
    velox_fuzzer_enable_complex_types,
    false,
    "Enable testing of function signatures with complex argument or return types.");

DEFINE_double(
    lazy_vector_generation_ratio,
    0.0,
    "Specifies the probability with which columns in the input row "
    "vector will be selected to be wrapped in lazy encoding "
    "(expressed as double from 0 to 1).");

DEFINE_bool(
    velox_fuzzer_enable_column_reuse,
    false,
    "Enable generation of expressions that re-use already generated columns.");

DEFINE_bool(
    velox_fuzzer_enable_expression_reuse,
    false,
    "Enable re-use already generated expression. Currently it only re-uses "
    "expressions that do not have nested expressions.");

namespace facebook::velox::test {

namespace {

using exec::SignatureBinder;

/// Returns if `functionName` with the given `argTypes` is deterministic.
/// Returns true if the function was not found or determinism cannot be
/// established.
bool isDeterministic(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  // Check if this is a simple function.
  if (auto simpleFunctionEntry =
          exec::SimpleFunctions().resolveFunction(functionName, argTypes)) {
    return simpleFunctionEntry->getMetadata().isDeterministic();
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

  // functionName must be a special form.
  LOG(WARNING) << "Unable to determine if '" << functionName
               << "' is deterministic or not. Assuming it is.";
  return true;
}

VectorFuzzer::Options getFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_batch_size;
  opts.stringVariableLength = true;
  opts.stringLength = 100;
  opts.nullRatio = FLAGS_null_ratio;
  return opts;
}

std::optional<CallableSignature> processConcreteSignature(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes,
    const exec::FunctionSignature& signature) {
  VELOX_CHECK(
      signature.variables().empty(),
      "Only concrete signatures are processed here.");

  CallableSignature callable{
      .name = functionName,
      .args = argTypes,
      .variableArity = signature.variableArity(),
      .returnType =
          SignatureBinder::tryResolveType(signature.returnType(), {}, {})};
  VELOX_CHECK_NOT_NULL(callable.returnType);

  bool onlyPrimitiveTypes = callable.returnType->isPrimitiveType();

  for (const auto& arg : argTypes) {
    onlyPrimitiveTypes = onlyPrimitiveTypes && arg->isPrimitiveType();
  }

  if (!(onlyPrimitiveTypes || FLAGS_velox_fuzzer_enable_complex_types)) {
    LOG(WARNING) << "Skipping '" << callable.toString()
                 << "' because it contains non-primitive types.";

    return std::nullopt;
  }
  return callable;
}

// Determine whether type is or contains typeName. typeName should be in lower
// case.
bool containTypeName(
    const exec::TypeSignature& type,
    const std::string& typeName) {
  auto sanitizedTypeName = exec::sanitizeName(type.baseName());
  if (sanitizedTypeName == typeName) {
    return true;
  }
  for (const auto& parameter : type.parameters()) {
    if (containTypeName(parameter, typeName)) {
      return true;
    }
  }
  return false;
}

// Determine whether the signature has an argument or return type that contains
// typeName. typeName should be in lower case.
bool useTypeName(
    const exec::FunctionSignature& signature,
    const std::string& typeName) {
  if (containTypeName(signature.returnType(), typeName)) {
    return true;
  }
  for (const auto& argument : signature.argumentTypes()) {
    if (containTypeName(argument, typeName)) {
      return true;
    }
  }
  return false;
}

bool isSupportedSignature(const exec::FunctionSignature& signature) {
  // Not supporting lambda functions, or functions using decimal and
  // timestamp with time zone types.
  return !(
      useTypeName(signature, "function") ||
      useTypeName(signature, "long_decimal") ||
      useTypeName(signature, "short_decimal") ||
      useTypeName(signature, "decimal") ||
      useTypeName(signature, "timestamp with time zone") ||
      useTypeName(signature, "interval day to second") ||
      (FLAGS_velox_fuzzer_enable_complex_types &&
       useTypeName(signature, "unknown")));
}

// Randomly pick columns from the input row vector to wrap in lazy.
std::vector<column_index_t> generateLazyColumnIds(
    const RowVectorPtr& rowVector,
    VectorFuzzer& vectorFuzzer) {
  std::vector<column_index_t> columnsToWrapInLazy;
  if (FLAGS_lazy_vector_generation_ratio > 0) {
    for (column_index_t idx = 0; idx < rowVector->childrenSize(); idx++) {
      VELOX_CHECK_NOT_NULL(rowVector->childAt(idx));
      if (vectorFuzzer.coinToss(FLAGS_lazy_vector_generation_ratio)) {
        columnsToWrapInLazy.push_back(idx);
      }
    }
  }
  return columnsToWrapInLazy;
}

} // namespace

ExpressionFuzzer::ExpressionFuzzer(
    FunctionSignatureMap signatureMap,
    size_t initialSeed,
    int32_t maxLevelOfNesting)
    : remainingLevelOfNesting_(std::max(1, maxLevelOfNesting)),
      verifier_(
          &execCtx_,
          {FLAGS_disable_constant_folding,
           FLAGS_repro_persist_path,
           FLAGS_persist_and_run_once}),
      vectorFuzzer_(getFuzzerOptions(), execCtx_.pool()) {
  seed(initialSeed);

  size_t totalFunctions = 0;
  size_t totalFunctionSignatures = 0;
  std::vector<std::string> supportedFunctions;
  size_t supportedFunctionSignatures = 0;
  // Process each available signature for every function.
  for (const auto& function : signatureMap) {
    ++totalFunctions;
    bool atLeastOneSupported = false;
    for (const auto& signature : function.second) {
      ++totalFunctionSignatures;

      if (!isSupportedSignature(*signature)) {
        continue;
      }
      if (!(signature->variables().empty() ||
            FLAGS_velox_fuzzer_enable_complex_types)) {
        LOG(WARNING) << "Skipping unsupported signature: " << function.first
                     << signature->toString();
        continue;
      }
      if (signature->variableArity() && !FLAGS_enable_variadic_signatures) {
        LOG(WARNING) << "Skipping variadic function signature: "
                     << function.first << signature->toString();
        continue;
      }

      // Determine a list of concrete argument types that can bind to the
      // signature. For non-parameterized signatures, these argument types will
      // be used to create a callable signature. For parameterized signatures,
      // these argument types are only used to fetch the function instance to
      // get their determinism.
      std::vector<TypePtr> argTypes;
      if (signature->variables().empty()) {
        for (const auto& arg : signature->argumentTypes()) {
          auto resolvedType = SignatureBinder::tryResolveType(arg, {}, {});
          if (!resolvedType) {
            LOG(WARNING) << "Skipping unsupported signature with generic: "
                         << function.first << signature->toString();
            continue;
          }
          argTypes.push_back(resolvedType);
        }
      } else {
        ArgumentTypeFuzzer typeFuzzer{*signature, rng_};
        typeFuzzer.fuzzReturnType();
        VELOX_CHECK_EQ(
            typeFuzzer.fuzzArgumentTypes(FLAGS_max_num_varargs), true);
        argTypes = typeFuzzer.argumentTypes();
      }
      if (!isDeterministic(function.first, argTypes)) {
        LOG(WARNING) << "Skipping non-deterministic function: "
                     << function.first << signature->toString();
        continue;
      }

      if (!signature->variables().empty()) {
        std::unordered_set<std::string> typeVariables;
        for (const auto& [name, _] : signature->variables()) {
          typeVariables.insert(name);
        }
        atLeastOneSupported = true;
        ++supportedFunctionSignatures;
        signatureTemplates_.emplace_back(SignatureTemplate{
            function.first, signature, std::move(typeVariables)});
      } else if (
          auto callableFunction =
              processConcreteSignature(function.first, argTypes, *signature)) {
        atLeastOneSupported = true;
        ++supportedFunctionSignatures;
        signatures_.emplace_back(*callableFunction);
      }
    }

    if (atLeastOneSupported) {
      supportedFunctions.push_back(function.first);
    }
  }

  auto unsupportedFunctions = totalFunctions - supportedFunctions.size();
  auto unsupportedFunctionSignatures =
      totalFunctionSignatures - supportedFunctionSignatures;
  LOG(INFO) << fmt::format(
      "Total candidate functions: {} ({} signatures)",
      totalFunctions,
      totalFunctionSignatures);
  LOG(INFO) << fmt::format(
      "Functions with at least one supported signature: {} ({:.2f}%)",
      supportedFunctions.size(),
      (double)supportedFunctions.size() / totalFunctions * 100);
  LOG(INFO) << fmt::format(
      "Functions with no supported signature: {} ({:.2f}%)",
      unsupportedFunctions,
      (double)unsupportedFunctions / totalFunctions * 100);
  LOG(INFO) << fmt::format(
      "Supported function signatures: {} ({:.2f}%)",
      supportedFunctionSignatures,
      (double)supportedFunctionSignatures / totalFunctionSignatures * 100);
  LOG(INFO) << fmt::format(
      "Unsupported function signatures: {} ({:.2f}%)",
      unsupportedFunctionSignatures,
      (double)unsupportedFunctionSignatures / totalFunctionSignatures * 100);

  // We sort the available signatures before inserting them into
  // signaturesMap_. The purpose of this step is to ensure the vector of
  // function signatures associated with each key in signaturesMap_ has a
  // deterministic order, so that we can deterministically generate
  // expressions across platforms. We just do this once and the vector is
  // small, so it doesn't need to be very efficient.
  sortCallableSignatures(signatures_);

  // Generates signaturesMap, which maps a given type to the function
  // signature that returns it.
  for (const auto& it : signatures_) {
    signaturesMap_[it.returnType->kind()].push_back(&it);
  }

  // Similarly, sort all template signatures.
  sortSignatureTemplates(signatureTemplates_);

  // Insert signature templates into signatureTemplateMap_ grouped by their
  // return type base name. If the return type is a type variable, insert the
  // signature template into the list of key kTypeParameterName.
  for (const auto& it : signatureTemplates_) {
    auto& returnType = it.signature->returnType().baseName();
    if (it.typeVariables.find(returnType) == it.typeVariables.end()) {
      signatureTemplateMap_[it.signature->returnType().baseName()].push_back(
          &it);
    } else {
      signatureTemplateMap_[kTypeParameterName].push_back(&it);
    }
  }

  // Register function override (for cases where we want to restrict the types
  // or parameters we pass to functions).
  registerFuncOverride(&ExpressionFuzzer::generateLikeArgs, "like");
  registerFuncOverride(
      &ExpressionFuzzer::generateEmptyApproxSetArgs, "empty_approx_set");
  registerFuncOverride(
      &ExpressionFuzzer::generateRegexpReplaceArgs, "regexp_replace");
  registerFuncOverride(&ExpressionFuzzer::generateSwitchArgs, "switch");

  // Init stats and register listener.
  for (auto& name : supportedFunctions) {
    exprNameToStats_.insert({name, ExprUsageStats()});
  }
  statListener_ = std::make_shared<ExprStatsListener>(exprNameToStats_);
  if (!exec::registerExprSetListener(statListener_)) {
    LOG(WARNING) << "Listener should only be registered once.";
  }
}

template <typename TFunc>
void ExpressionFuzzer::registerFuncOverride(
    TFunc func,
    const std::string& name) {
  funcArgOverrides_[name] = std::bind(func, this, std::placeholders::_1);
}

void ExpressionFuzzer::seed(size_t seed) {
  currentSeed_ = seed;
  vectorFuzzer_.reSeed(seed);
  rng_.seed(currentSeed_);
}

void ExpressionFuzzer::reSeed() {
  seed(rng_());
}

RowVectorPtr ExpressionFuzzer::generateRowVector() {
  return vectorFuzzer_.fuzzInputRow(
      ROW(std::move(inputRowNames_), std::move(inputRowTypes_)));
}

core::TypedExprPtr ExpressionFuzzer::generateArgConstant(const TypePtr& arg) {
  if (vectorFuzzer_.coinToss(FLAGS_null_ratio)) {
    return std::make_shared<core::ConstantTypedExpr>(
        arg, variant::null(arg->kind()));
  }
  return std::make_shared<core::ConstantTypedExpr>(
      vectorFuzzer_.fuzzConstant(arg, 1));
}

// Either generates a new column of the required type or if already generated
// columns of the same type exist then there is a 30% chance that it will
// re-use one of them.
core::TypedExprPtr ExpressionFuzzer::generateArgColumn(const TypePtr& arg) {
  auto& listOfCandidateCols = typeToColumnNames_[arg->toString()];
  bool reuseColumn = FLAGS_velox_fuzzer_enable_column_reuse &&
      !listOfCandidateCols.empty() && vectorFuzzer_.coinToss(0.3);
  if (!reuseColumn) {
    inputRowTypes_.emplace_back(arg);
    inputRowNames_.emplace_back(fmt::format("c{}", inputRowTypes_.size() - 1));
    listOfCandidateCols.push_back(inputRowNames_.back());
    return std::make_shared<core::FieldAccessTypedExpr>(
        arg, inputRowNames_.back());
  }
  size_t chosenColIndex = boost::random::uniform_int_distribution<uint32_t>(
      0, listOfCandidateCols.size() - 1)(rng_);
  return std::make_shared<core::FieldAccessTypedExpr>(
      arg, listOfCandidateCols[chosenColIndex]);
}

core::TypedExprPtr ExpressionFuzzer::generateArg(const TypePtr& arg) {
  size_t argClass =
      boost::random::uniform_int_distribution<uint32_t>(0, 3)(rng_);

  // Toss a coin and choose between a constant, a column reference, or another
  // expression (function).
  //
  // TODO: Add more expression types:
  // - Conjunctions
  // - IF/ELSE/SWITCH
  // - Lambdas
  // - Try
  if (argClass >= kArgExpression) {
    if (remainingLevelOfNesting_ > 0) {
      return generateExpression(arg);
    }
    argClass = boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng_);
  }

  if (argClass == kArgConstant) {
    return generateArgConstant(arg);
  }
  // argClass == kArgColumn
  return generateArgColumn(arg);
}

std::vector<core::TypedExprPtr> ExpressionFuzzer::generateArgs(
    const CallableSignature& input) {
  std::vector<core::TypedExprPtr> inputExpressions;
  auto numVarArgs = !input.variableArity
      ? 0
      : boost::random::uniform_int_distribution<uint32_t>(
            0, FLAGS_max_num_varargs)(rng_);
  inputExpressions.reserve(input.args.size() + numVarArgs);

  for (const auto& arg : input.args) {
    inputExpressions.emplace_back(generateArg(arg));
  }
  // Append varargs to the argument list.
  for (int i = 0; i < numVarArgs; i++) {
    inputExpressions.emplace_back(generateArg(input.args.back()));
  }
  return inputExpressions;
}

// Specialization for the "like" function: second and third (optional)
// parameters always need to be constant.
std::vector<core::TypedExprPtr> ExpressionFuzzer::generateLikeArgs(
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
std::vector<core::TypedExprPtr> ExpressionFuzzer::generateEmptyApproxSetArgs(
    const CallableSignature& input) {
  if (input.args.empty()) {
    return {};
  }
  return {generateArgConstant(input.args[0])};
}

// Specialization for the "regexp_replace" function: second and third
// (optional) parameters always need to be constant.
std::vector<core::TypedExprPtr> ExpressionFuzzer::generateRegexpReplaceArgs(
    const CallableSignature& input) {
  std::vector<core::TypedExprPtr> inputExpressions = {
      generateArg(input.args[0]), generateArgConstant(input.args[1])};
  if (input.args.size() == 3) {
    inputExpressions.emplace_back(generateArgConstant(input.args[2]));
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> ExpressionFuzzer::generateSwitchArgs(
    const CallableSignature& input) {
  VELOX_CHECK_EQ(
      input.args.size(),
      2,
      "Only two inputs are expected from the template signature.");
  size_t cases = boost::random::uniform_int_distribution<uint32_t>(1, 5)(rng_);
  bool useFinalElse =
      boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng_) > 0;

  auto conditionClauseType = input.args[0];
  auto thenClauseType = input.args[1];
  std::vector<core::TypedExprPtr> inputExpressions;
  for (int case_idx = 0; case_idx < cases; case_idx++) {
    inputExpressions.push_back(generateArg(conditionClauseType));
    inputExpressions.push_back(generateArg(thenClauseType));
  }
  if (useFinalElse) {
    inputExpressions.push_back(generateArg(thenClauseType));
  }
  return inputExpressions;
}

// Either generates a new expression of the required return type or if already
// generated expressions of the same return type exist then there is a 30%
// chance that it will re-use one of them. Only expressions with no nested
// expressions are re-used.
core::TypedExprPtr ExpressionFuzzer::generateExpression(
    const TypePtr& returnType) {
  VELOX_CHECK_GT(remainingLevelOfNesting_, 0);
  --remainingLevelOfNesting_;
  auto guard = folly::makeGuard([&] { ++remainingLevelOfNesting_; });

  auto& listOfCandidateExprs = typeToExpressions_[returnType->toString()];
  bool reuseExpression = FLAGS_velox_fuzzer_enable_expression_reuse &&
      !listOfCandidateExprs.empty() && vectorFuzzer_.coinToss(0.3);
  if (!reuseExpression) {
    core::TypedExprPtr expression;

    // Generate a cast expression with 40% chance.
    if (FLAGS_enable_cast && vectorFuzzer_.coinToss(0.4)) {
      expression = generateCastExpression(returnType);
      if (!expression) {
        LOG(INFO) << "Casting to '" << returnType->toString()
                  << "' is unsupported. Returning a constant instead.";
        expression = generateArgConstant(returnType);
      }
      return expression;
    }
    auto firstAttempt =
        &ExpressionFuzzer::generateExpressionFromConcreteSignatures;
    auto secondAttempt =
        &ExpressionFuzzer::generateExpressionFromSignatureTemplate;

    size_t useSignatureTemplate =
        boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng_);
    if (FLAGS_velox_fuzzer_enable_complex_types && useSignatureTemplate) {
      std::swap(firstAttempt, secondAttempt);
    }

    expression = (this->*firstAttempt)(returnType);
    if (!expression) {
      if (FLAGS_velox_fuzzer_enable_complex_types) {
        expression = (this->*secondAttempt)(returnType);
      }
      if (!expression) {
        LOG(INFO) << "Couldn't find any function to return '"
                  << returnType->toString()
                  << "'. Returning a constant instead.";
        expression = generateArgConstant(returnType);
      }
    }
    if (remainingLevelOfNesting_ == 0) {
      // Only add expressions that do not have nested expressions.
      listOfCandidateExprs.emplace_back(expression);
    }
    return expression;
  }
  size_t chosenExprIndex = boost::random::uniform_int_distribution<uint32_t>(
      0, listOfCandidateExprs.size() - 1)(rng_);
  return listOfCandidateExprs[chosenExprIndex];
}

std::vector<core::TypedExprPtr> ExpressionFuzzer::getArgsForCallable(
    const CallableSignature& callable) {
  auto funcIt = funcArgOverrides_.find(callable.name);
  if (funcIt == funcArgOverrides_.end()) {
    return generateArgs(callable);
  }
  return funcIt->second(callable);
}

core::TypedExprPtr ExpressionFuzzer::getCallExprFromCallable(
    const CallableSignature& callable) {
  auto args = getArgsForCallable(callable);

  return std::make_shared<core::CallTypedExpr>(
      callable.returnType, args, callable.name);
}

core::TypedExprPtr ExpressionFuzzer::generateExpressionFromConcreteSignatures(
    const TypePtr& returnType) {
  auto it = signaturesMap_.find(returnType->kind());
  if (it == signaturesMap_.end()) {
    return nullptr;
  }

  // Only function signatures whose return type equals to returnType are
  // eligible. There may be ineligible signatures in signaturesMap_ because
  // the map keys only differentiate top-level type kinds.
  std::vector<const CallableSignature*> eligible;
  const auto& signatures = it->second;
  for (const auto* signature : signatures) {
    if (signature->returnType->equivalent(*returnType)) {
      eligible.push_back(signature);
    }
  }
  if (eligible.empty()) {
    return nullptr;
  }

  // Randomly pick a function that can return `returnType`.
  size_t idx = boost::random::uniform_int_distribution<uint32_t>(
      0, eligible.size() - 1)(rng_);
  const auto& chosen = eligible[idx];

  markSelected(chosen->name);
  return getCallExprFromCallable(*chosen);
}

const SignatureTemplate* ExpressionFuzzer::chooseRandomSignatureTemplate(
    const TypePtr& returnType,
    const std::string& typeName) {
  std::vector<const SignatureTemplate*> eligible;
  auto it = signatureTemplateMap_.find(typeName);
  if (it == signatureTemplateMap_.end()) {
    return nullptr;
  }
  // Only function signatures whose return type can match returnType are
  // eligible. There may be ineligible signatures in signaturesMap_ because
  // the map keys only differentiate the top-level type names.
  auto& signatureTemplates = it->second;
  for (auto* signatureTemplate : signatureTemplates) {
    exec::ReverseSignatureBinder binder{
        *signatureTemplate->signature, returnType};
    if (binder.tryBind()) {
      eligible.push_back(signatureTemplate);
    }
  }
  if (eligible.empty()) {
    return nullptr;
  }

  auto idx = boost::random::uniform_int_distribution<uint32_t>(
      0, eligible.size() - 1)(rng_);
  return eligible[idx];
}

core::TypedExprPtr ExpressionFuzzer::generateExpressionFromSignatureTemplate(
    const TypePtr& returnType) {
  auto typeName = typeToBaseName(returnType);

  auto* chosen = chooseRandomSignatureTemplate(returnType, typeName);
  if (!chosen) {
    chosen = chooseRandomSignatureTemplate(returnType, kTypeParameterName);
    if (!chosen) {
      return nullptr;
    }
  }

  ArgumentTypeFuzzer fuzzer{*chosen->signature, returnType, rng_};
  VELOX_CHECK_EQ(fuzzer.fuzzArgumentTypes(FLAGS_max_num_varargs), true);
  auto& argumentTypes = fuzzer.argumentTypes();

  CallableSignature callable{chosen->name, argumentTypes, false, returnType};

  markSelected(chosen->name);
  return getCallExprFromCallable(callable);
}

TypePtr ExpressionFuzzer::chooseCastFromType(const TypePtr& to) {
  if (to->isPrimitiveType()) {
    return vectorFuzzer_.randType(0);
  }
  if (to->isArray()) {
    return ARRAY(chooseCastFromType(to->childAt(0)));
  }
  if (to->isMap()) {
    return MAP(
        chooseCastFromType(to->childAt(0)), chooseCastFromType(to->childAt(1)));
  }
  if (to->isRow()) {
    std::vector<TypePtr> children;
    for (auto& child : to->asRow().children()) {
      children.push_back(chooseCastFromType(child));
    }
    return ROW(std::move(children));
  }
  // Placeholder for unsupported types.
  return nullptr;
}

core::TypedExprPtr ExpressionFuzzer::generateCastExpression(
    const TypePtr& returnType) {
  // Choose a random from type.
  auto fromType = chooseCastFromType(returnType);
  if (!fromType) {
    return nullptr;
  }

  CallableSignature callable{"cast", {fromType}, false, returnType};
  auto args = getArgsForCallable(callable);

  // Generate try_cast expression with 50% chance.
  bool nullOnFailure =
      boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng_);
  return std::make_shared<core::CastTypedExpr>(
      callable.returnType, args, nullOnFailure);
}

template <typename T>
bool ExpressionFuzzer::isDone(size_t i, T startTime) const {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

void ExpressionFuzzer::reset() {
  VELOX_CHECK(inputRowTypes_.empty());
  VELOX_CHECK(inputRowNames_.empty());
  typeToColumnNames_.clear();
  typeToExpressions_.clear();
}

void ExpressionFuzzer::logStats() {
  std::vector<std::pair<std::string, ExprUsageStats>> entries;
  uint64_t totalSelections = 0;
  for (auto& elem : exprNameToStats_) {
    totalSelections += elem.second.numTimesSelected;
    entries.push_back(elem);
  }

  // sort by numProcessedRows
  std::sort(entries.begin(), entries.end(), [](auto& left, auto& right) {
    return left.second.numProcessedRows > right.second.numProcessedRows;
  });
  int maxEntriesLimit = std::min<size_t>(10, entries.size());
  LOG(INFO) << "==============================> Top " << maxEntriesLimit
            << " by number of rows processed";
  LOG(INFO)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (int i = 0; i < maxEntriesLimit; i++) {
    LOG(INFO) << entries[i].first << " " << entries[i].second.numTimesSelected
              << " " << std::fixed << std::setprecision(2)
              << (entries[i].second.numTimesSelected * 100.00) / totalSelections
              << "% " << entries[i].second.numProcessedRows;
  }

  LOG(INFO) << "==============================> Bottom " << maxEntriesLimit
            << " by number of rows processed";
  LOG(INFO)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (int i = 0; i < maxEntriesLimit; i++) {
    int idx = entries.size() - 1 - i;
    LOG(INFO) << entries[idx].first << " "
              << entries[idx].second.numTimesSelected << " " << std::fixed
              << std::setprecision(2)
              << (entries[idx].second.numTimesSelected * 100.00) /
            totalSelections
              << "% " << entries[idx].second.numProcessedRows;
  }

  // sort by numTimesSelected
  std::sort(entries.begin(), entries.end(), [](auto& left, auto& right) {
    return left.second.numTimesSelected > right.second.numTimesSelected;
  });

  LOG(INFO) << "==============================> All stats sorted by number "
               "of times the function was chosen";
  LOG(INFO)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (auto& elem : entries) {
    LOG(INFO) << elem.first << " " << elem.second.numTimesSelected << " "
              << std::fixed << std::setprecision(2)
              << (elem.second.numTimesSelected * 100.00) / totalSelections
              << "% " << elem.second.numProcessedRows;
  }
}

void ExpressionFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.")

  auto startTime = std::chrono::system_clock::now();
  size_t i = 0;

  while (!isDone(i, startTime)) {
    LOG(INFO) << "==============================> Started iteration " << i
              << " (seed: " << currentSeed_ << ")";
    reset();

    auto chooseFromConcreteSignatures =
        boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng_);
    chooseFromConcreteSignatures =
        (chooseFromConcreteSignatures && !signatures_.empty()) ||
        (!chooseFromConcreteSignatures && signatureTemplates_.empty());
    TypePtr rootType;
    if (!FLAGS_choose_root_type_from_signature_template ||
        chooseFromConcreteSignatures) {
      // Pick a random signature to choose the root return type.
      VELOX_CHECK(!signatures_.empty(), "No function signature available.");
      size_t idx = boost::random::uniform_int_distribution<uint32_t>(
          0, signatures_.size() - 1)(rng_);
      rootType = signatures_[idx].returnType;
    } else {
      // Pick a random concrete return type that can bind to the return type of
      // a chosen signature.
      VELOX_CHECK(
          !signatureTemplates_.empty(), "No function signature available.");
      size_t idx = boost::random::uniform_int_distribution<uint32_t>(
          0, signatureTemplates_.size() - 1)(rng_);
      ArgumentTypeFuzzer typeFuzzer{*signatureTemplates_[idx].signature, rng_};
      rootType = typeFuzzer.fuzzReturnType();
    }

    // Generate expression tree and input data vectors.
    auto plan = generateExpression(rootType);
    auto rowVector = generateRowVector();

    // Randomize initial result vector data to test for correct null and data
    // setting in functions.
    VectorPtr resultVector;
    if (vectorFuzzer_.coinToss(0.5)) {
      resultVector = vectorFuzzer_.fuzzFlat(plan->type());
    }

    auto columnsToWrapInLazy = generateLazyColumnIds(rowVector, vectorFuzzer_);

    // If both paths threw compatible exceptions, we add a try() function to
    // the expression's root and execute it again. This time the expression
    // cannot throw.
    if (!verifier_.verify(
            plan,
            rowVector,
            resultVector ? BaseVector::copy(*resultVector) : nullptr,
            true,
            columnsToWrapInLazy) &&
        FLAGS_retry_with_try) {
      LOG(INFO)
          << "Both paths failed with compatible exceptions. Retrying expression using try().";

      plan = std::make_shared<core::CallTypedExpr>(
          plan->type(), std::vector<core::TypedExprPtr>{plan}, "try");

      // At this point, the function throws if anything goes wrong.
      verifier_.verify(
          plan,
          rowVector,
          resultVector ? BaseVector::copy(*resultVector) : nullptr,
          false,
          columnsToWrapInLazy);
    }

    LOG(INFO) << "==============================> Done with iteration " << i;
    reSeed();
    ++i;
  }
  logStats();
}

void expressionFuzzer(FunctionSignatureMap signatureMap, size_t seed) {
  ExpressionFuzzer(std::move(signatureMap), seed).go();
}

} // namespace facebook::velox::test
