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

#include <boost/algorithm/string.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <folly/ScopeGuard.h>
#include <glog/logging.h>
#include <exception>
#include <unordered_set>

#include "velox/common/base/Exceptions.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/ReverseSignatureBinder.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/fuzzer/ArgumentTypeFuzzer.h"
#include "velox/expression/fuzzer/ExpressionFuzzer.h"

namespace facebook::velox::fuzzer {

namespace {
using exec::SignatureBinder;
using exec::SignatureBinderBase;
using exec::test::sanitizeTryResolveType;
using exec::test::usesTypeName;

class FullSignatureBinder : public SignatureBinderBase {
 public:
  FullSignatureBinder(
      const exec::FunctionSignature& signature,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType)
      : SignatureBinderBase(signature) {
    if (signature_.argumentTypes().size() != argTypes.size()) {
      return;
    }

    for (auto i = 0; i < argTypes.size(); ++i) {
      if (!SignatureBinderBase::tryBind(
              signature_.argumentTypes()[i], argTypes[i])) {
        return;
      }
    }

    if (!SignatureBinderBase::tryBind(signature_.returnType(), returnType)) {
      return;
    }

    bound_ = true;
  }

  // Returns true if argument types and return type specified in the constructor
  // match specified signature.
  bool tryBind() {
    return bound_;
  }

 private:
  bool bound_{false};
};

static const std::vector<std::string> kIntegralTypes{
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "boolean"};

static const std::vector<std::string> kFloatingPointTypes{"real", "double"};

facebook::velox::exec::FunctionSignaturePtr makeCastSignature(
    const std::string& fromType,
    const std::string& toType) {
  return facebook::velox::exec::FunctionSignatureBuilder()
      .argumentType(fromType)
      .returnType(toType)
      .build();
}

void addCastFromIntegralSignatures(
    const std::string& toType,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>& signatures) {
  for (const auto& fromType : kIntegralTypes) {
    signatures.push_back(makeCastSignature(fromType, toType));
  }
}

void addCastFromFloatingPointSignatures(
    const std::string& toType,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>& signatures) {
  for (const auto& fromType : kFloatingPointTypes) {
    signatures.push_back(makeCastSignature(fromType, toType));
  }
}

void addCastFromVarcharSignature(
    const std::string& toType,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>& signatures) {
  signatures.push_back(makeCastSignature("varchar", toType));
}

void addCastFromTimestampSignature(
    const std::string& toType,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>& signatures) {
  signatures.push_back(makeCastSignature("timestamp", toType));
}

void addCastFromDateSignature(
    const std::string& toType,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>& signatures) {
  signatures.push_back(makeCastSignature("date", toType));
}

std::vector<facebook::velox::exec::FunctionSignaturePtr>
getSignaturesForCast() {
  std::vector<facebook::velox::exec::FunctionSignaturePtr> signatures;

  // To integral types.
  for (const auto& toType : kIntegralTypes) {
    addCastFromIntegralSignatures(toType, signatures);
    addCastFromFloatingPointSignatures(toType, signatures);
    addCastFromVarcharSignature(toType, signatures);
  }

  // To floating-point types.
  for (const auto& toType : kFloatingPointTypes) {
    addCastFromIntegralSignatures(toType, signatures);
    addCastFromFloatingPointSignatures(toType, signatures);
    addCastFromVarcharSignature(toType, signatures);
  }

  // To varchar type.
  addCastFromIntegralSignatures("varchar", signatures);
  addCastFromFloatingPointSignatures("varchar", signatures);
  addCastFromVarcharSignature("varchar", signatures);
  addCastFromDateSignature("varchar", signatures);
  addCastFromTimestampSignature("varchar", signatures);

  // To timestamp type.
  addCastFromVarcharSignature("timestamp", signatures);
  addCastFromDateSignature("timestamp", signatures);

  // To date type.
  addCastFromVarcharSignature("date", signatures);
  addCastFromTimestampSignature("date", signatures);

  // For each supported translation pair T --> U, add signatures of array(T) -->
  // array(U), map(varchar, T) --> map(varchar, U), row(T) --> row(U).
  auto size = signatures.size();
  for (auto i = 0; i < size; ++i) {
    auto from = signatures[i]->argumentTypes()[0].baseName();
    auto to = signatures[i]->returnType().baseName();

    signatures.push_back(makeCastSignature(
        fmt::format("array({})", from), fmt::format("array({})", to)));

    signatures.push_back(makeCastSignature(
        fmt::format("map(varchar, {})", from),
        fmt::format("map(varchar, {})", to)));

    signatures.push_back(makeCastSignature(
        fmt::format("row({})", from), fmt::format("row({})", to)));
  }
  return signatures;
}

static const std::unordered_map<
    std::string,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>>
    kSpecialForms = {
        {"and",
         std::vector<facebook::velox::exec::FunctionSignaturePtr>{
             // Signature: and (condition,...) -> output:
             // boolean, boolean,.. -> boolean
             facebook::velox::exec::FunctionSignatureBuilder()
                 .argumentType("boolean")
                 .variableArity("boolean")
                 .returnType("boolean")
                 .build()}},
        {"or",
         std::vector<facebook::velox::exec::FunctionSignaturePtr>{
             // Signature: or (condition,...) -> output:
             // boolean, boolean,.. -> boolean
             facebook::velox::exec::FunctionSignatureBuilder()
                 .argumentType("boolean")
                 .variableArity("boolean")
                 .returnType("boolean")
                 .build()}},
        {"coalesce",
         std::vector<facebook::velox::exec::FunctionSignaturePtr>{
             // Signature: coalesce (input,...) -> output:
             // T, T,.. -> T
             facebook::velox::exec::FunctionSignatureBuilder()
                 .typeVariable("T")
                 .argumentType("T")
                 .variableArity("T")
                 .returnType("T")
                 .build()}},
        {
            "if",
            std::vector<facebook::velox::exec::FunctionSignaturePtr>{
                // Signature: if (condition, then) -> output:
                // boolean, T -> T
                facebook::velox::exec::FunctionSignatureBuilder()
                    .typeVariable("T")
                    .argumentType("boolean")
                    .argumentType("T")
                    .returnType("T")
                    .build(),
                // Signature: if (condition, then, else) -> output:
                // boolean, T, T -> T
                facebook::velox::exec::FunctionSignatureBuilder()
                    .typeVariable("T")
                    .argumentType("boolean")
                    .argumentType("T")
                    .argumentType("T")
                    .returnType("T")
                    .build()},
        },
        {
            "switch",
            std::vector<facebook::velox::exec::FunctionSignaturePtr>{
                // Signature: Switch (condition, then) -> output:
                // boolean, T -> T
                // This is only used to bind to a randomly selected type for the
                // output, then while generating arguments, an override is used
                // to generate inputs that can create variation of multiple
                // cases and may or may not include a final else clause.
                facebook::velox::exec::FunctionSignatureBuilder()
                    .typeVariable("T")
                    .argumentType("boolean")
                    .argumentType("T")
                    .returnType("T")
                    .build()},
        },
        {
            "cast",
            /// TODO: Add supported Cast signatures to CastTypedExpr and expose
            /// them to fuzzer instead of hard-coding signatures here.
            getSignaturesForCast(),
        },
};

static std::unordered_set<std::string> splitNames(const std::string& names) {
  // Parse, lower case and trim it.
  std::vector<folly::StringPiece> nameList;
  folly::split(',', names, nameList);
  std::unordered_set<std::string> nameSet;

  for (const auto& it : nameList) {
    auto str = folly::trimWhitespace(it).toString();
    folly::toLowerAscii(str);
    nameSet.insert(str);
  }
  return nameSet;
}

static std::pair<std::string, std::string> splitSignature(
    const std::string& signature) {
  const auto parenPos = signature.find("(");

  if (parenPos != std::string::npos) {
    return {signature.substr(0, parenPos), signature.substr(parenPos)};
  }

  return {signature, ""};
}

// Returns if `functionName` is deterministic. Returns true if the function was
// not found or determinism cannot be established.
bool isDeterministic(const std::string& functionName) {
  // We know that the 'cast', 'and', and 'or' special forms are deterministic.
  // Hard-code them here because they are not real functions and hence cannot
  // be resolved by the code below.
  if (functionName == "and" || functionName == "or" ||
      functionName == "coalesce" || functionName == "if" ||
      functionName == "switch" || functionName == "cast") {
    return true;
  }

  const auto determinism = velox::isDeterministic(functionName);
  if (!determinism.has_value()) {
    // functionName must be a special form.
    LOG(WARNING) << "Unable to determine if '" << functionName
                 << "' is deterministic or not. Assuming it is.";
    return true;
  }
  return determinism.value();
}

// Parse the comma separated list of function names, and use it to filter the
// input signatures.
static void filterSignatures(
    facebook::velox::FunctionSignatureMap& input,
    const std::string& onlyFunctions,
    const std::unordered_set<std::string>& skipFunctions) {
  if (!onlyFunctions.empty()) {
    // Parse, lower case and trim it.
    auto nameSet = splitNames(onlyFunctions);

    // Use the generated set to filter the input signatures.
    for (auto it = input.begin(); it != input.end();) {
      if (!nameSet.count(it->first)) {
        it = input.erase(it);
      } else
        it++;
    }
  }

  for (auto skip : skipFunctions) {
    // 'skip' can be function name or signature.
    const auto [skipName, skipSignature] = splitSignature(skip);

    if (skipSignature.empty()) {
      input.erase(skipName);
    } else {
      auto it = input.find(skipName);
      if (it != input.end()) {
        // Compiler refuses to reference 'skipSignature' from the lambda as
        // is.
        const auto& signatureToRemove = skipSignature;

        auto removeIt = std::find_if(
            it->second.begin(), it->second.end(), [&](const auto& signature) {
              return signature->toString() == signatureToRemove;
            });
        VELOX_CHECK(
            removeIt != it->second.end(), "Skip signature not found: {}", skip);
        it->second.erase(removeIt);
      }
    }
  }

  for (auto it = input.begin(); it != input.end();) {
    if (!isDeterministic(it->first)) {
      LOG(WARNING) << "Skipping non-deterministic function: " << it->first;
      it = input.erase(it);
    } else {
      ++it;
    }
  }
}

static void appendSpecialForms(
    facebook::velox::FunctionSignatureMap& signatureMap,
    const std::string& specialForms) {
  auto specialFormNames = splitNames(specialForms);
  for (const auto& [name, signatures] : kSpecialForms) {
    if (specialFormNames.count(name) == 0) {
      LOG(INFO) << "Skipping special form: " << name;
      continue;
    }
    std::vector<const facebook::velox::exec::FunctionSignature*> rawSignatures;
    for (const auto& signature : signatures) {
      rawSignatures.push_back(signature.get());
    }
    signatureMap.insert({name, std::move(rawSignatures)});
  }
}

std::optional<CallableSignature> processConcreteSignature(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes,
    const exec::FunctionSignature& signature,
    bool enableComplexTypes) {
  VELOX_CHECK(
      signature.variables().empty(),
      "Only concrete signatures are processed here.");

  CallableSignature callable{
      .name = functionName,
      .args = argTypes,
      .variableArity = signature.variableArity(),
      .returnType = sanitizeTryResolveType(signature.returnType(), {}, {}),
      .constantArgs = signature.constantArguments()};
  VELOX_CHECK_NOT_NULL(callable.returnType);

  bool onlyPrimitiveTypes = callable.returnType->isPrimitiveType();

  for (const auto& arg : argTypes) {
    onlyPrimitiveTypes = onlyPrimitiveTypes && arg->isPrimitiveType();
  }

  if (!(onlyPrimitiveTypes || enableComplexTypes)) {
    LOG(WARNING) << "Skipping '" << callable.toString()
                 << "' because it contains non-primitive types.";

    return std::nullopt;
  }
  return callable;
}

/// Returns row numbers for non-null rows among all children in'data' or null
/// if all rows are null.
BufferPtr extractNonNullIndices(const RowVectorPtr& data) {
  DecodedVector decoded;
  SelectivityVector nonNullRows(data->size());

  for (auto& child : data->children()) {
    decoded.decode(*child);
    auto* rawNulls = decoded.nulls(nullptr);
    if (rawNulls) {
      nonNullRows.deselectNulls(rawNulls, 0, data->size());
    }
    if (!nonNullRows.hasSelections()) {
      return nullptr;
    }
  }

  BufferPtr indices = allocateIndices(nonNullRows.end(), data->pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t cnt = 0;
  nonNullRows.applyToSelected(
      [&](vector_size_t row) { rawIndices[cnt++] = row; });
  VELOX_CHECK_GT(cnt, 0);
  indices->setSize(cnt * sizeof(vector_size_t));
  return indices;
}

/// Wraps child vectors of the specified 'rowVector' in dictionary using
/// specified 'indices'. Returns new RowVector created from the wrapped
/// vectors.
RowVectorPtr wrapChildren(
    const BufferPtr& indices,
    const RowVectorPtr& rowVector) {
  auto size = indices->size() / sizeof(vector_size_t);

  std::vector<VectorPtr> newInputs;
  for (const auto& child : rowVector->children()) {
    newInputs.push_back(
        BaseVector::wrapInDictionary(nullptr, indices, size, child));
  }

  return std::make_shared<RowVector>(
      rowVector->pool(), rowVector->type(), nullptr, size, newInputs);
}

// Return the level of nestin of `type`. Return 0 if `type` is primitive.
uint32_t levelOfNesting(const TypePtr& type) {
  if (type->isPrimitiveType()) {
    return 0;
  }
  switch (type->kind()) {
    case TypeKind::ARRAY:
      return 1 + levelOfNesting(type->asArray().elementType());
    case TypeKind::MAP:
      return 1 +
          std::max(
                 levelOfNesting(type->asMap().keyType()),
                 levelOfNesting(type->asMap().valueType()));
    case TypeKind::ROW: {
      auto children = type->asRow().children();
      VELOX_CHECK(!children.empty());
      uint32_t maxChildNesting = 0;
      for (const auto& child : children) {
        maxChildNesting = std::max(maxChildNesting, levelOfNesting(child));
      }
      return 1 + maxChildNesting;
    }
    default:
      VELOX_UNREACHABLE("Not a supported type.");
  }
}

} // namespace

ExpressionFuzzer::ExpressionFuzzer(
    FunctionSignatureMap signatureMap,
    size_t initialSeed,
    const std::shared_ptr<VectorFuzzer>& vectorFuzzer,
    const std::optional<ExpressionFuzzer::Options>& options,
    const std::unordered_map<std::string, std::shared_ptr<ArgGenerator>>&
        argGenerators)
    : options_(options.value_or(Options())),
      vectorFuzzer_(vectorFuzzer),
      state{rng_, std::max(1, options_.maxLevelOfNesting)},
      argGenerators_(argGenerators) {
  VELOX_CHECK(vectorFuzzer, "Vector fuzzer must be provided");
  seed(initialSeed);

  appendSpecialForms(signatureMap, options_.specialForms);
  filterSignatures(
      signatureMap, options_.useOnlyFunctions, options_.skipFunctions);

  size_t totalFunctions = 0;
  size_t totalFunctionSignatures = 0;
  size_t supportedFunctionSignatures = 0;
  // A local random number generator to be used just in ExpressionFuzzer
  // constructor. We do not use rng_ in this function because code in this
  // function may change rng_ and cause it to mismatch with the seed printed in
  // the log.
  FuzzerGenerator localRng{
      static_cast<FuzzerGenerator::result_type>(initialSeed)};
  // Process each available signature for every function.
  for (const auto& function : signatureMap) {
    ++totalFunctions;
    bool atLeastOneSupported = false;
    for (const auto& signature : function.second) {
      ++totalFunctionSignatures;

      if (!isSupportedSignature(*signature)) {
        continue;
      }
      if (!(signature->variables().empty() || options_.enableComplexTypes ||
            options_.enableDecimalType)) {
        LOG(WARNING) << "Skipping unsupported signature: " << function.first
                     << signature->toString();
        continue;
      }
      if (signature->variableArity() && !options_.enableVariadicSignatures) {
        LOG(WARNING) << "Skipping variadic function signature: "
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
      } else {
        // Determine a list of concrete argument types that can bind to the
        // signature. For non-parameterized signatures, these argument types
        // will be used to create a callable signature.
        std::vector<TypePtr> argTypes;
        bool supportedSignature = true;
        for (const auto& arg : signature->argumentTypes()) {
          auto resolvedType = sanitizeTryResolveType(arg, {}, {});
          if (!resolvedType) {
            supportedSignature = false;
            continue;
          }
          argTypes.push_back(resolvedType);
        }

        if (!supportedSignature) {
          LOG(WARNING) << "Skipping unsupported signature with generic: "
                       << function.first << signature->toString();
          continue;
        }
        if (auto callableFunction = processConcreteSignature(
                function.first,
                argTypes,
                *signature,
                options_.enableComplexTypes)) {
          atLeastOneSupported = true;
          ++supportedFunctionSignatures;
          signatures_.emplace_back(*callableFunction);
        }
      }
    }

    if (atLeastOneSupported) {
      supportedFunctions_.push_back(function.first);
    }
  }

  auto unsupportedFunctions = totalFunctions - supportedFunctions_.size();
  auto unsupportedFunctionSignatures =
      totalFunctionSignatures - supportedFunctionSignatures;
  LOG(INFO) << fmt::format(
      "Total candidate functions: {} ({} signatures)",
      totalFunctions,
      totalFunctionSignatures);
  LOG(INFO) << fmt::format(
      "Functions with at least one supported signature: {} ({:.2f}%)",
      supportedFunctions_.size(),
      (double)supportedFunctions_.size() / totalFunctions * 100);
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

  getTicketsForFunctions();

  // We sort the available signatures before inserting them into
  // typeToExpressionList_ and expressionToSignature_. The purpose of this step
  // is to ensure the vector of function signatures associated with each key in
  // signaturesMap_ has a deterministic order, so that we can deterministically
  // generate expressions across platforms. We just do this once and the vector
  // is small, so it doesn't need to be very efficient.
  sortCallableSignatures(signatures_);

  for (const auto& it : signatures_) {
    auto returnType = typeToBaseName(it.returnType);
    if (typeToExpressionList_[returnType].empty() ||
        typeToExpressionList_[returnType].back() != it.name) {
      // Ensure entries for a function name are added only once. This
      // gives all others a fair chance to be selected. Since signatures
      // are sorted on the function name this check will always work.
      // Add multiple entries to increase likelihood of its selection.
      addToTypeToExpressionListByTicketTimes(returnType, it.name);
    }
    expressionToSignature_[it.name][returnType].push_back(&it);
  }

  // Similarly, sort all template signatures.
  sortSignatureTemplates(signatureTemplates_);

  for (const auto& it : signatureTemplates_) {
    const auto returnType =
        exec::sanitizeName(it.signature->returnType().baseName());
    const auto* returnTypeKey = &returnType;

    if (it.typeVariables.find(returnType) != it.typeVariables.end()) {
      // Return type is a template variable.
      returnTypeKey = &kTypeParameterName;
    }
    if (typeToExpressionList_[*returnTypeKey].empty() ||
        typeToExpressionList_[*returnTypeKey].back() != it.name) {
      addToTypeToExpressionListByTicketTimes(*returnTypeKey, it.name);
    }
    expressionToTemplatedSignature_[it.name][*returnTypeKey].push_back(&it);
  }

  if (options_.enableDereference) {
    addToTypeToExpressionListByTicketTimes("row", "row_constructor");
    addToTypeToExpressionListByTicketTimes(kTypeParameterName, "dereference");
  }

  // Register function override (for cases where we want to restrict the types
  // or parameters we pass to functions).
  registerFuncOverride(&ExpressionFuzzer::generateSwitchArgs, "switch");
}

bool ExpressionFuzzer::isSupportedSignature(
    const exec::FunctionSignature& signature) {
  // When enableComplexType is disabled, not supporting complex functions.
  const bool useComplexType = usesTypeName(signature, "array") ||
      usesTypeName(signature, "map") || usesTypeName(signature, "row");
  // Not supporting functions using custom types, timestamp with time zone types
  // and interval day to second types.
  if (usesTypeName(signature, "opaque") ||
      usesTypeName(signature, "timestamp with time zone") ||
      usesTypeName(signature, "interval day to second") ||
      (!options_.enableDecimalType && usesTypeName(signature, "decimal")) ||
      (!options_.enableComplexTypes && useComplexType) ||
      (options_.enableComplexTypes && usesTypeName(signature, "unknown"))) {
    return false;
  }

  if (options_.referenceQueryRunner &&
      !options_.referenceQueryRunner->isSupported(signature)) {
    return false;
  }

  return true;
}

void ExpressionFuzzer::getTicketsForFunctions() {
  if (options_.functionTickets.empty()) {
    return;
  }
  std::vector<std::string> results;
  boost::algorithm::split(
      results, options_.functionTickets, boost::is_any_of(","));

  for (auto& entry : results) {
    std::vector<std::string> separated;
    boost::algorithm::split(separated, entry, boost::is_any_of("="));
    if (separated.size() != 2) {
      LOG(FATAL)
          << "Invalid format. Expected a function name and its number of "
             "tickets separated by '=', instead found: "
          << entry;
    }
    int tickets = 0;
    try {
      tickets = stoi(separated[1]);
    } catch (std::exception& e) {
      LOG(FATAL)
          << "Invalid number of tickets. Expected a function name and its "
             "number of tickets separated by '=', instead found: "
          << entry << " Error encountered: " << e.what();
    }

    if (tickets < 1) {
      LOG(FATAL)
          << "Number of tickets should be a positive integer. Expected a "
             "function name and its number of tickets separated by '=',"
             " instead found: "
          << entry;
    }
    functionsToTickets_.insert({separated[0], tickets});
  }
}

int ExpressionFuzzer::getTickets(const std::string& funcName) {
  auto itr = functionsToTickets_.find(funcName);
  int tickets = 1;
  if (itr != functionsToTickets_.end()) {
    tickets = itr->second;
  }
  return tickets;
}

void ExpressionFuzzer::addToTypeToExpressionListByTicketTimes(
    const std::string& type,
    const std::string& funcName) {
  int tickets = getTickets(funcName);
  for (int i = 0; i < tickets; i++) {
    typeToExpressionList_[exec::sanitizeName(type)].push_back(funcName);
  }
}

template <typename TFunc>
void ExpressionFuzzer::registerFuncOverride(
    TFunc func,
    const std::string& name) {
  funcArgOverrides_[name] = std::bind(func, this, std::placeholders::_1);
}

void ExpressionFuzzer::seed(size_t seed) {
  rng_.seed(seed);
  vectorFuzzer_->reSeed(seed);
}

int32_t ExpressionFuzzer::rand32(int32_t min, int32_t max) {
  return boost::random::uniform_int_distribution<uint32_t>(min, max)(rng_);
}

core::TypedExprPtr ExpressionFuzzer::generateArgConstant(const TypePtr& arg) {
  if (vectorFuzzer_->coinToss(options_.nullRatio)) {
    return std::make_shared<core::ConstantTypedExpr>(
        arg, variant::null(arg->kind()));
  }
  return std::make_shared<core::ConstantTypedExpr>(
      vectorFuzzer_->fuzzConstant(arg, 1));
}

// Either generates a new column of the required type or if already generated
// columns of the same type exist then there is a 30% chance that it will
// re-use one of them.
core::TypedExprPtr ExpressionFuzzer::generateArgColumn(const TypePtr& arg) {
  auto& listOfCandidateCols = state.typeToColumnNames_[arg->toString()];
  bool reuseColumn = options_.enableColumnReuse &&
      !listOfCandidateCols.empty() && vectorFuzzer_->coinToss(0.3);

  if (!reuseColumn && options_.maxInputsThreshold.has_value() &&
      state.inputRowTypes_.size() >= options_.maxInputsThreshold.value()) {
    reuseColumn = !listOfCandidateCols.empty();
  }

  if (!reuseColumn) {
    state.inputRowTypes_.emplace_back(arg);
    state.inputRowNames_.emplace_back(
        fmt::format("c{}", state.inputRowTypes_.size() - 1));
    listOfCandidateCols.push_back(state.inputRowNames_.back());
    return std::make_shared<core::FieldAccessTypedExpr>(
        arg, state.inputRowNames_.back());
  }
  size_t chosenColIndex = rand32(0, listOfCandidateCols.size() - 1);
  return std::make_shared<core::FieldAccessTypedExpr>(
      arg, listOfCandidateCols[chosenColIndex]);
}

core::TypedExprPtr ExpressionFuzzer::generateArg(const TypePtr& arg) {
  size_t argClass = rand32(0, 3);

  // Toss a coin and choose between a constant, a column reference, or another
  // expression (function).
  //
  // TODO: Add more expression types:
  // - Conjunctions
  // - IF/ELSE/SWITCH
  // - Lambdas
  // - Try
  if (argClass >= kArgExpression) {
    if (state.remainingLevelOfNesting_ > 0) {
      return generateExpression(arg);
    }
    argClass = rand32(0, 1);
  }

  if (argClass == kArgConstant) {
    auto argExpr = generateArgConstant(arg);
    if ((options_.referenceQueryRunner == nullptr ||
         options_.referenceQueryRunner->isConstantExprSupported(argExpr))) {
      return argExpr;
    }
  }
  // argClass == kArgColumn
  return generateArgColumn(arg);
}

std::vector<core::TypedExprPtr> ExpressionFuzzer::generateArgs(
    const std::vector<TypePtr>& argTypes,
    const std::vector<bool>& constantArgs,
    uint32_t numVarArgs) {
  std::vector<core::TypedExprPtr> inputExpressions;
  inputExpressions.reserve(argTypes.size() + numVarArgs);

  for (auto i = 0; i < argTypes.size(); ++i) {
    inputExpressions.emplace_back(
        generateArg(argTypes.at(i), constantArgs.at(i)));
  }
  // Append varargs to the argument list.
  for (int i = 0; i < numVarArgs; i++) {
    inputExpressions.emplace_back(
        generateArg(argTypes.back(), constantArgs.back()));
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> ExpressionFuzzer::generateArgs(
    const CallableSignature& input) {
  auto numVarArgs =
      !input.variableArity ? 0 : rand32(0, options_.maxNumVarArgs);
  return generateArgs(input.args, input.constantArgs, numVarArgs);
}

core::TypedExprPtr ExpressionFuzzer::generateArgFunction(const TypePtr& arg) {
  const auto& functionType = arg->asFunction();

  std::vector<TypePtr> args;
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> inputs;
  args.reserve(arg->size() - 1);
  names.reserve(arg->size() - 1);
  inputs.reserve(arg->size() - 1);

  for (auto i = 0; i < arg->size() - 1; ++i) {
    args.push_back(arg->childAt(i));
    names.push_back(fmt::format("__a{}", i));
    inputs.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        args.back(), names.back()));
  }

  const auto& returnType = functionType.children().back();

  const auto baseType = typeToBaseName(returnType);
  const auto& baseList = typeToExpressionList_[baseType];
  const auto& templateList = typeToExpressionList_[kTypeParameterName];

  std::vector<std::string> eligible;
  for (const auto& functionName : baseList) {
    if (auto* signature =
            findConcreteSignature(args, returnType, functionName)) {
      eligible.push_back(functionName);
    } else if (
        auto* signatureTemplate =
            findSignatureTemplate(args, returnType, baseType, functionName)) {
      eligible.push_back(functionName);
    }
  }

  for (const auto& functionName : templateList) {
    if (auto* signatureTemplate =
            findSignatureTemplate(args, returnType, baseType, functionName)) {
      eligible.push_back(functionName);
    }
  }

  core::TypedExprPtr body;
  if (eligible.empty()) {
    body = generateArgConstant(returnType);
    if (options_.referenceQueryRunner == nullptr ||
        options_.referenceQueryRunner->isConstantExprSupported(body)) {
      return std::make_shared<core::LambdaTypedExpr>(
          ROW(std::move(names), std::move(args)), body);
    } else {
      return std::make_shared<core::LambdaTypedExpr>(
          ROW(std::move(names), std::move(args)),
          generateArgColumn(returnType));
    }
  }

  const auto idx = rand32(0, eligible.size() - 1);
  const auto name = eligible[idx];

  if (name == "cast") {
    bool tryCast = rand32(0, 1);
    body = std::make_shared<core::CastTypedExpr>(returnType, inputs, tryCast);
  } else {
    body = std::make_shared<core::CallTypedExpr>(returnType, inputs, name);
  }
  return std::make_shared<core::LambdaTypedExpr>(
      ROW(std::move(names), std::move(args)), body);
}

core::TypedExprPtr ExpressionFuzzer::generateArg(
    const TypePtr& arg,
    bool isConstant) {
  if (arg->isFunction()) {
    return generateArgFunction(arg);
  }

  if (isConstant) {
    return generateArgConstant(arg);
  } else {
    return generateArg(arg);
  }
}

std::vector<core::TypedExprPtr> ExpressionFuzzer::generateSwitchArgs(
    const CallableSignature& input) {
  VELOX_CHECK_EQ(
      input.args.size(),
      2,
      "Only two inputs are expected from the template signature.");
  size_t cases = rand32(1, 5);
  bool useFinalElse = vectorFuzzer_->coinToss(0.5);

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

ExpressionFuzzer::FuzzedExpressionData ExpressionFuzzer::fuzzExpressions(
    const RowTypePtr& outType) {
  state.reset();
  VELOX_CHECK_EQ(
      state.remainingLevelOfNesting_, std::max(1, options_.maxLevelOfNesting));

  std::vector<core::TypedExprPtr> expressions;
  for (int i = 0; i < outType->size(); i++) {
    expressions.push_back(generateExpression(outType->childAt(i)));
  }
  return {
      std::move(expressions),
      ROW(std::move(state.inputRowNames_), std::move(state.inputRowTypes_)),
      std::move(state.expressionStats_)};
}

ExpressionFuzzer::FuzzedExpressionData ExpressionFuzzer::fuzzExpressions(
    size_t numberOfExpressions) {
  return fuzzExpressions(fuzzRowReturnType(numberOfExpressions));
}

ExpressionFuzzer::FuzzedExpressionData ExpressionFuzzer::fuzzExpression() {
  return fuzzExpressions(1);
}

// Either generates a new expression of the required return type or if already
// generated expressions of the same return type exist then there is a 30%
// chance that it will re-use one of them.
core::TypedExprPtr ExpressionFuzzer::generateExpression(
    const TypePtr& returnType) {
  VELOX_CHECK_GT(state.remainingLevelOfNesting_, 0);
  --state.remainingLevelOfNesting_;
  auto guard = folly::makeGuard([&] { ++state.remainingLevelOfNesting_; });

  core::TypedExprPtr expression;
  bool reuseExpression =
      options_.enableExpressionReuse && vectorFuzzer_->coinToss(0.3);
  if (reuseExpression) {
    expression = state.expressionBank_.getRandomExpression(
        returnType, state.remainingLevelOfNesting_ + 1);
    if (expression) {
      return expression;
    }
  }
  auto baseType = typeToBaseName(returnType);
  VELOX_CHECK_NE(
      baseType, "T", "returnType should have all concrete types defined");
  // Randomly pick among all functions that support this return type. Also,
  // consider all functions that have return type "T" as they can
  // support any concrete return type.
  auto& baseList = typeToExpressionList_[baseType];
  auto& templateList = typeToExpressionList_[kTypeParameterName];
  uint32_t numEligible = baseList.size() + templateList.size();

  if (numEligible > 0) {
    size_t chosenExprIndex = rand32(0, numEligible - 1);
    std::string chosenFunctionName;
    if (chosenExprIndex < baseList.size()) {
      chosenFunctionName = baseList[chosenExprIndex];
    } else {
      chosenExprIndex -= baseList.size();
      chosenFunctionName = templateList[chosenExprIndex];
    }

    if (chosenFunctionName == "cast") {
      expression = generateCastExpression(returnType);
    } else if (chosenFunctionName == "row_constructor") {
      // Avoid generating deeply nested types that is rarely used in practice.
      if (levelOfNesting(returnType) < 3) {
        expression = generateRowConstructorExpression(returnType);
      }
    } else if (chosenFunctionName == "dereference") {
      expression = generateDereferenceExpression(returnType);
    } else {
      expression = generateExpressionFromConcreteSignatures(
          returnType, chosenFunctionName);
      if (!expression &&
          (options_.enableComplexTypes || options_.enableDecimalType)) {
        expression = generateExpressionFromSignatureTemplate(
            returnType, chosenFunctionName);
      }
    }
  }
  if (!expression) {
    VLOG(1) << "Couldn't find a proper function to return '"
            << returnType->toString()
            << "'. Returning a constant or column instead.";
    expression = generateArgConstant(returnType);
    if (options_.referenceQueryRunner == nullptr ||
        options_.referenceQueryRunner->isConstantExprSupported(expression)) {
      return expression;
    } else {
      return generateArgColumn(returnType);
    }
  }
  state.expressionBank_.insert(expression);
  return expression;
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
    const CallableSignature& callable,
    const TypePtr& type) {
  auto args = getArgsForCallable(callable);
  // Generate a CallTypedExpr with type because callable.returnType may not have
  // the required field names.
  return std::make_shared<core::CallTypedExpr>(type, args, callable.name);
}

const CallableSignature* ExpressionFuzzer::chooseRandomConcreteSignature(
    const TypePtr& returnType,
    const std::string& functionName) {
  if (expressionToSignature_.find(functionName) ==
      expressionToSignature_.end()) {
    return nullptr;
  }
  auto baseType = typeToBaseName(returnType);
  auto itr = expressionToSignature_[functionName].find(baseType);
  if (itr == expressionToSignature_[functionName].end()) {
    return nullptr;
  }
  // Only function signatures whose return type equals to returnType are
  // eligible. There may be ineligible signatures in signaturesMap_ because
  // the map keys only differentiate top-level type kinds.
  std::vector<const CallableSignature*> eligible;
  for (auto signature : itr->second) {
    if (signature->returnType->equivalent(*returnType)) {
      eligible.push_back(signature);
    }
  }
  if (eligible.empty()) {
    return nullptr;
  }

  // Randomly pick a function that can return `returnType`.
  size_t idx = rand32(0, eligible.size() - 1);
  return eligible[idx];
}

const CallableSignature* ExpressionFuzzer::findConcreteSignature(
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType,
    const std::string& functionName) {
  if (expressionToSignature_.find(functionName) ==
      expressionToSignature_.end()) {
    return nullptr;
  }
  auto baseType = typeToBaseName(returnType);
  auto it = expressionToSignature_[functionName].find(baseType);
  if (it == expressionToSignature_[functionName].end()) {
    return nullptr;
  }

  for (auto signature : it->second) {
    if (!signature->returnType->equivalent(*returnType)) {
      continue;
    }

    if (signature->args.size() != argTypes.size()) {
      continue;
    }

    bool argTypesMatch = true;
    for (auto i = 0; i < argTypes.size(); ++i) {
      if (signature->constantArgs[i] ||
          !signature->args[i]->equivalent(*argTypes[i])) {
        argTypesMatch = false;
        break;
      }
    }

    if (argTypesMatch) {
      return signature;
    }
  }

  return nullptr;
}

core::TypedExprPtr ExpressionFuzzer::generateExpressionFromConcreteSignatures(
    const TypePtr& returnType,
    const std::string& functionName) {
  const auto* chosen = chooseRandomConcreteSignature(returnType, functionName);
  if (!chosen) {
    return nullptr;
  }

  markSelected(chosen->name);
  return getCallExprFromCallable(*chosen, returnType);
}

const SignatureTemplate* ExpressionFuzzer::chooseRandomSignatureTemplate(
    const TypePtr& returnType,
    const std::string& typeName,
    const std::string& functionName) {
  std::vector<const SignatureTemplate*> eligible;
  if (expressionToTemplatedSignature_.find(functionName) ==
      expressionToTemplatedSignature_.end()) {
    return nullptr;
  }
  auto it = expressionToTemplatedSignature_[functionName].find(typeName);
  if (it == expressionToTemplatedSignature_[functionName].end()) {
    return nullptr;
  }
  // Only function signatures whose return type can match returnType are
  // eligible. There may be ineligible signatures in signaturesMap_ because
  // the map keys only differentiate the top-level type names.
  auto& signatureTemplates = it->second;
  for (auto signatureTemplate : signatureTemplates) {
    exec::ReverseSignatureBinder binder{
        *signatureTemplate->signature, returnType};
    if (binder.tryBind()) {
      eligible.push_back(signatureTemplate);
    }
  }
  if (eligible.empty()) {
    return nullptr;
  }

  auto idx = rand32(0, eligible.size() - 1);
  return eligible[idx];
}

const SignatureTemplate* ExpressionFuzzer::findSignatureTemplate(
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType,
    const std::string& typeName,
    const std::string& functionName) {
  std::vector<const SignatureTemplate*> eligible;
  if (expressionToTemplatedSignature_.find(functionName) ==
      expressionToTemplatedSignature_.end()) {
    return nullptr;
  }
  auto it = expressionToTemplatedSignature_[functionName].find(typeName);
  if (it == expressionToTemplatedSignature_[functionName].end()) {
    return nullptr;
  }

  for (auto signatureTemplate : it->second) {
    // Skip signatures with constant arguments.
    if (signatureTemplate->signature->hasConstantArgument()) {
      continue;
    }

    FullSignatureBinder binder{
        *signatureTemplate->signature, argTypes, returnType};
    if (binder.tryBind()) {
      return signatureTemplate;
    }
  }

  return nullptr;
}

core::TypedExprPtr ExpressionFuzzer::generateExpressionFromSignatureTemplate(
    const TypePtr& returnType,
    const std::string& functionName) {
  auto typeName = typeToBaseName(returnType);

  auto* chosen =
      chooseRandomSignatureTemplate(returnType, typeName, functionName);
  if (!chosen) {
    chosen = chooseRandomSignatureTemplate(
        returnType, kTypeParameterName, functionName);
    if (!chosen) {
      return nullptr;
    }
  }

  auto chosenSignature = *chosen->signature;
  ArgumentTypeFuzzer fuzzer{chosenSignature, returnType, rng_};

  std::vector<TypePtr> argumentTypes;
  if (fuzzer.fuzzArgumentTypes(options_.maxNumVarArgs)) {
    // Use the argument fuzzer to generate argument types.
    argumentTypes = fuzzer.argumentTypes();
  } else {
    auto it = argGenerators_.find(functionName);
    // Since the argument type fuzzer cannot produce argument types, argument
    // generators should be provided.
    VELOX_CHECK(
        it != argGenerators_.end(),
        "Cannot generate argument types for {} with return type {}.",
        functionName,
        returnType->toString());
    argumentTypes = it->second->generateArgs(chosenSignature, returnType, rng_);
    if (argumentTypes.empty()) {
      return nullptr;
    }
  }

  auto constantArguments = chosenSignature.constantArguments();

  // ArgumentFuzzer may generate duplicate arguments if the signature's
  // variableArity is true, so we need to pad duplicate constant flags.
  if (!constantArguments.empty()) {
    auto repeat = argumentTypes.size() - constantArguments.size();
    auto lastConstant = constantArguments.back();
    for (int i = 0; i < repeat; ++i) {
      constantArguments.push_back(lastConstant);
    }
  }

  CallableSignature callable{
      .name = chosen->name,
      .args = argumentTypes,
      .variableArity = false,
      .returnType = returnType,
      .constantArgs = constantArguments};

  markSelected(chosen->name);
  return getCallExprFromCallable(callable, returnType);
}

core::TypedExprPtr ExpressionFuzzer::generateCastExpression(
    const TypePtr& returnType) {
  const auto* callable = chooseRandomConcreteSignature(returnType, "cast");
  if (!callable) {
    return nullptr;
  }

  auto args = getArgsForCallable(*callable);
  markSelected("cast");

  // Generate try_cast expression with 50% chance.
  bool nullOnFailure = vectorFuzzer_->coinToss(0.5);
  return std::make_shared<core::CastTypedExpr>(returnType, args, nullOnFailure);
}

core::TypedExprPtr ExpressionFuzzer::generateRowConstructorExpression(
    const TypePtr& returnType) {
  VELOX_CHECK(returnType->isRow());
  auto argTypes = asRowType(returnType)->children();
  std::vector<bool> constantArgs(argTypes.size(), false);

  auto inputExpressions = generateArgs(argTypes, constantArgs);
  return std::make_shared<core::ConcatTypedExpr>(
      asRowType(returnType)->names(), inputExpressions);
}

TypePtr ExpressionFuzzer::generateRandomRowTypeWithReferencedField(
    uint32_t numFields,
    uint32_t referencedIndex,
    const TypePtr& referencedType) {
  std::vector<TypePtr> fieldTypes(numFields);
  std::vector<std::string> fieldNames(numFields);
  for (auto i = 0; i < numFields; ++i) {
    if (i == referencedIndex) {
      fieldTypes[i] = referencedType;
    } else {
      fieldTypes[i] = vectorFuzzer_->randType();
    }
    fieldNames[i] = fmt::format("row_field{}", i);
  }
  return ROW(std::move(fieldNames), std::move(fieldTypes));
}

core::TypedExprPtr ExpressionFuzzer::generateDereferenceExpression(
    const TypePtr& returnType) {
  auto numFields = rand32(1, 3);
  auto referencedIndex = rand32(0, numFields - 1);
  auto argType = generateRandomRowTypeWithReferencedField(
      numFields, referencedIndex, returnType);

  auto inputExpressions = generateArgs({argType}, {false});
  return std::make_shared<core::FieldAccessTypedExpr>(
      returnType,
      inputExpressions[0],
      fmt::format("row_field{}", referencedIndex));
}
void ExpressionFuzzer::ExprBank::insert(const core::TypedExprPtr& expression) {
  auto typeString = expression->type()->toString();
  if (typeToExprsByLevel_.find(typeString) == typeToExprsByLevel_.end()) {
    typeToExprsByLevel_.insert(
        {typeString, ExprsIndexedByLevel(maxLevelOfNesting_ + 1)});
  }
  auto& expressionsByLevel = typeToExprsByLevel_[typeString];
  int nestingLevel = getNestedLevel(expression);
  VELOX_CHECK_LE(nestingLevel, maxLevelOfNesting_);
  expressionsByLevel[nestingLevel].push_back(expression);
}

core::TypedExprPtr ExpressionFuzzer::ExprBank::getRandomExpression(
    const facebook::velox::TypePtr& returnType,
    int uptoLevelOfNesting) {
  VELOX_CHECK_LE(uptoLevelOfNesting, maxLevelOfNesting_);
  auto typeString = returnType->toString();
  if (typeToExprsByLevel_.find(typeString) == typeToExprsByLevel_.end()) {
    return nullptr;
  }
  auto& expressionsByLevel = typeToExprsByLevel_[typeString];
  int totalToConsider = 0;
  for (int i = 0; i <= uptoLevelOfNesting; i++) {
    totalToConsider += expressionsByLevel[i].size();
  }
  if (totalToConsider > 0) {
    int choice = boost::random::uniform_int_distribution<uint32_t>(
        0, totalToConsider - 1)(rng_);
    for (int i = 0; i <= uptoLevelOfNesting; i++) {
      if (choice >= expressionsByLevel[i].size()) {
        choice -= expressionsByLevel[i].size();
        continue;
      }
      return expressionsByLevel[i][choice];
    }
    VELOX_CHECK(false, "Should have found an expression.");
  }
  return nullptr;
}

TypePtr ExpressionFuzzer::fuzzReturnType() {
  auto chooseFromConcreteSignatures = rand32(0, 1);

  chooseFromConcreteSignatures =
      (chooseFromConcreteSignatures && !signatures_.empty()) ||
      (!chooseFromConcreteSignatures && signatureTemplates_.empty());
  TypePtr rootType;
  if (signatures_.empty() && signatureTemplates_.empty() &&
      options_.enableDereference) {
    // Dereference does not have signatures in either list. So even if these
    // signature lists are both empty, we can still generate a random return
    // type for dereference.
    rootType = vectorFuzzer_->randType();
  } else if (chooseFromConcreteSignatures) {
    // Pick a random signature to choose the root return type.
    VELOX_CHECK(!signatures_.empty(), "No function signature available.");
    size_t idx = rand32(0, signatures_.size() - 1);
    rootType = signatures_[idx].returnType;
  } else {
    // Pick a random concrete return type that can bind to the return type of
    // a chosen signature.
    VELOX_CHECK(
        !signatureTemplates_.empty(), "No function signature available.");
    size_t idx = rand32(0, signatureTemplates_.size() - 1);
    ArgumentTypeFuzzer typeFuzzer{*signatureTemplates_[idx].signature, rng_};
    rootType = typeFuzzer.fuzzReturnType();
  }
  return rootType;
}

RowTypePtr ExpressionFuzzer::fuzzRowReturnType(size_t size, char prefix) {
  std::vector<TypePtr> children;
  std::vector<std::string> names;
  for (int i = 0; i < size; i++) {
    children.push_back(fuzzReturnType());
    names.push_back(fmt::format("{}{}", prefix, i));
  }
  return ROW(std::move(names), std::move(children));
}

} // namespace facebook::velox::fuzzer
