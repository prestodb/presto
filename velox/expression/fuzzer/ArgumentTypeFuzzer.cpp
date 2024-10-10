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

#include "velox/expression/fuzzer/ArgumentTypeFuzzer.h"

#include <boost/algorithm/string.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/expression/ReverseSignatureBinder.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::fuzzer {

using exec::test::sanitizeTryResolveType;

std::string typeToBaseName(const TypePtr& type) {
  if (type->isDecimal()) {
    return "decimal";
  }
  return boost::algorithm::to_lower_copy(std::string{type->kindName()});
}

std::optional<TypeKind> baseNameToTypeKind(const std::string& typeName) {
  auto kindName = boost::algorithm::to_upper_copy(typeName);
  return tryMapNameToTypeKind(kindName);
}

namespace {

bool isDecimalBaseName(const std::string& typeName) {
  auto normalized = boost::algorithm::to_lower_copy(typeName);

  return normalized == "decimal";
}

/// Returns true only if 'str' contains digits.
bool isPositiveInteger(const std::string& str) {
  return !str.empty() &&
      std::find_if(str.begin(), str.end(), [](unsigned char c) {
        return !std::isdigit(c);
      }) == str.end();
}

} // namespace

void ArgumentTypeFuzzer::determineUnboundedIntegerVariables(
    const exec::TypeSignature& type) {
  std::vector<exec::TypeSignature> decimalTypes;
  findDecimalTypes(type, decimalTypes);
  if (decimalTypes.empty()) {
    return;
  }
  for (const auto& decimalType : decimalTypes) {
    auto [p, s] = tryBindFixedPrecisionScale(decimalType);

    if (p.has_value() && s.has_value()) {
      return;
    }

    const auto& precision = decimalType.parameters()[0].baseName();
    const auto& scale = decimalType.parameters()[1].baseName();

    if (s.has_value()) {
      p = std::max<int>(ShortDecimalType::kMinPrecision, s.value());
      if (p < LongDecimalType::kMaxPrecision) {
        p = p.value() + rand32(0, LongDecimalType::kMaxPrecision - p.value());
      }

      integerBindings_[precision] = p.value();
      return;
    }

    if (p.has_value()) {
      s = rand32(0, p.value());
      integerBindings_[scale] = s.value();
      return;
    }

    p = rand32(1, LongDecimalType::kMaxPrecision);
    s = rand32(0, p.value());

    integerBindings_[precision] = p.value();
    integerBindings_[scale] = s.value();
  }
}

void ArgumentTypeFuzzer::determineUnboundedTypeVariables() {
  for (auto& [variableName, variableInfo] : variables()) {
    if (!variableInfo.isTypeParameter()) {
      continue;
    }

    if (bindings_[variableName] != nullptr) {
      continue;
    }

    // Random randomType() never generates unknown here.
    // TODO: we should extend randomType types and exclude unknown based
    // on variableInfo.
    if (variableInfo.orderableTypesOnly()) {
      bindings_[variableName] = randOrderableType();
    } else {
      bindings_[variableName] = randType();
    }
  }
}

TypePtr ArgumentTypeFuzzer::randType() {
  return velox::randType(rng_, 2);
}

TypePtr ArgumentTypeFuzzer::randOrderableType() {
  return velox::randOrderableType(rng_, 2);
}

bool ArgumentTypeFuzzer::fuzzArgumentTypes(uint32_t maxVariadicArgs) {
  if (returnType_ == nullptr) {
    for (const auto& [name, _] : signature_.variables()) {
      bindings_.insert({name, nullptr});
    }
  } else {
    exec::ReverseSignatureBinder binder{signature_, returnType_};
    if (!binder.tryBind()) {
      return false;
    }

    auto types = signature_.argumentTypes();
    types.emplace_back(signature_.returnType());
    for (const auto& type : types) {
      std::vector<exec::TypeSignature> decimalTypes;
      findDecimalTypes(type, decimalTypes);
      if (decimalTypes.empty()) {
        continue;
      }

      // Verify if the precision and scale variables of argument types and
      // return type can be bound to constant values. If not and extra
      // constraint is provided, argument types cannot be generated without
      // following these constraints.
      for (const auto& decimalType : decimalTypes) {
        const auto [p, s] = tryBindFixedPrecisionScale(decimalType);

        const auto& precision = decimalType.parameters()[0].baseName();
        const auto& scale = decimalType.parameters()[1].baseName();

        const auto hasConstraint = [&](const std::string& name) {
          return !variables().at(name).constraint().empty();
        };

        if ((!p.has_value() && hasConstraint(precision)) ||
            (!s.has_value() && hasConstraint(scale))) {
          return false;
        }
      }
    }

    bindings_ = binder.bindings();
    integerBindings_ = binder.integerBindings();
  }

  const auto& formalArgs = signature_.argumentTypes();
  auto formalArgsCnt = formalArgs.size();

  determineUnboundedTypeVariables();
  for (const auto& argType : formalArgs) {
    determineUnboundedIntegerVariables(argType);
  }
  for (auto i = 0; i < formalArgsCnt; i++) {
    TypePtr actualArg;
    if (formalArgs[i].baseName() == "any") {
      actualArg = randType();
    } else {
      actualArg = sanitizeTryResolveType(
          formalArgs[i], variables(), bindings_, integerBindings_);
      VELOX_CHECK(actualArg != nullptr);
    }
    argumentTypes_.push_back(actualArg);
  }

  // Generate random repeats of the last argument type if the signature is
  // variadic.
  if (signature_.variableArity()) {
    auto repeat = boost::random::uniform_int_distribution<uint32_t>(
        0, maxVariadicArgs)(rng_);
    auto last = argumentTypes_[formalArgsCnt - 1];
    for (int i = 0; i < repeat; ++i) {
      argumentTypes_.push_back(last);
    }
  }

  return true;
}

TypePtr ArgumentTypeFuzzer::fuzzReturnType() {
  VELOX_CHECK_EQ(
      returnType_,
      nullptr,
      "Only fuzzing uninitialized return type is allowed.");

  determineUnboundedTypeVariables();
  determineUnboundedIntegerVariables(signature_.returnType());

  const auto& returnType = signature_.returnType();

  if (returnType.baseName() == "any") {
    returnType_ = randType();
  } else {
    returnType_ = sanitizeTryResolveType(
        returnType, variables(), bindings_, integerBindings_);
  }

  VELOX_CHECK_NOT_NULL(returnType_);
  return returnType_;
}

int32_t ArgumentTypeFuzzer::rand32(int32_t min, int32_t max) {
  return boost::random::uniform_int_distribution<uint32_t>(min, max)(rng_);
}

std::optional<int> ArgumentTypeFuzzer::tryFixedBinding(
    const std::string& name) {
  auto it = variables().find(name);
  if (it == variables().end()) {
    VELOX_CHECK(
        isPositiveInteger(name),
        "Precision and scale of a decimal type must refer to a variable "
        "or specify a positive integer constant: {}",
        name);
    return std::stoi(name);
  }

  if (integerBindings_.count(name) > 0) {
    return integerBindings_[name];
  }

  if (isPositiveInteger(it->second.constraint())) {
    const auto value = std::stoi(it->second.constraint());
    integerBindings_[name] = value;
    return value;
  }

  return std::nullopt;
}

std::pair<std::optional<int>, std::optional<int>>
ArgumentTypeFuzzer::tryBindFixedPrecisionScale(
    const exec::TypeSignature& type) {
  VELOX_CHECK(isDecimalBaseName(type.baseName()));
  VELOX_CHECK_EQ(2, type.parameters().size());

  const auto& precisionName = type.parameters()[0].baseName();
  const auto& scaleName = type.parameters()[1].baseName();

  std::optional<int> p = tryFixedBinding(precisionName);
  std::optional<int> s = tryFixedBinding(scaleName);
  return {p, s};
}

void ArgumentTypeFuzzer::findDecimalTypes(
    const exec::TypeSignature& type,
    std::vector<exec::TypeSignature>& decimalTypes) const {
  if (isDecimalBaseName(type.baseName())) {
    decimalTypes.push_back(type);
  }
  for (const auto& param : type.parameters()) {
    findDecimalTypes(param, decimalTypes);
  }
}

} // namespace facebook::velox::fuzzer
