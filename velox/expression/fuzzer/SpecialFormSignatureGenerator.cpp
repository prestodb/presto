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
#include "velox/expression/fuzzer/SpecialFormSignatureGenerator.h"
#include "velox/expression/signature_parser/ParseUtil.h"

namespace facebook::velox::fuzzer {

void SpecialFormSignatureGenerator::appendSpecialForms(
    FunctionSignatureMap& signatureMap,
    const std::string& specialForms) const {
  auto specialFormNames = exec::splitNames(specialForms);
  for (const auto& [name, signatures] : getSignatures()) {
    if (specialFormNames.count(name) == 0) {
      LOG(INFO) << "Skipping special form: " << name;
      continue;
    }
    std::vector<const exec::FunctionSignature*> rawSignatures;
    for (const auto& signature : signatures) {
      rawSignatures.push_back(signature.get());
    }
    signatureMap.insert({name, std::move(rawSignatures)});
  }
}

void SpecialFormSignatureGenerator::addCastFromIntegralSignatures(
    const std::string& toType,
    std::vector<exec::FunctionSignaturePtr>& signatures) const {
  for (const auto& fromType : kIntegralTypes_) {
    signatures.push_back(makeCastSignature(fromType, toType));
  }
}

void SpecialFormSignatureGenerator::addCastFromFloatingPointSignatures(
    const std::string& toType,
    std::vector<exec::FunctionSignaturePtr>& signatures) const {
  for (const auto& fromType : kFloatingPointTypes_) {
    signatures.push_back(makeCastSignature(fromType, toType));
  }
}

void SpecialFormSignatureGenerator::addCastFromVarcharSignature(
    const std::string& toType,
    std::vector<exec::FunctionSignaturePtr>& signatures) const {
  signatures.push_back(makeCastSignature("varchar", toType));
}

void SpecialFormSignatureGenerator::addCastFromTimestampSignature(
    const std::string& toType,
    std::vector<exec::FunctionSignaturePtr>& signatures) const {
  signatures.push_back(makeCastSignature("timestamp", toType));
}

void SpecialFormSignatureGenerator::addCastFromDateSignature(
    const std::string& toType,
    std::vector<exec::FunctionSignaturePtr>& signatures) const {
  signatures.push_back(makeCastSignature("date", toType));
}

const std::unordered_map<std::string, std::vector<exec::FunctionSignaturePtr>>&
SpecialFormSignatureGenerator::getSignatures() const {
  const static std::
      unordered_map<std::string, std::vector<exec::FunctionSignaturePtr>>
          kSpecialForms{
              {"and", getSignaturesForAnd()},
              {"or", getSignaturesForOr()},
              {"coalesce", getSignaturesForCoalesce()},
              {"if", getSignaturesForIf()},
              {"switch", getSignaturesForSwitch()},
              {"cast", getSignaturesForCast()}};
  return kSpecialForms;
}

std::vector<exec::FunctionSignaturePtr>
SpecialFormSignatureGenerator::getSignaturesForAnd() const {
  // Signature: and (condition,...) -> output:
  // boolean, boolean,.. -> boolean
  return {exec::FunctionSignatureBuilder()
              .argumentType("boolean")
              .variableArity("boolean")
              .returnType("boolean")
              .build()};
}

std::vector<exec::FunctionSignaturePtr>
SpecialFormSignatureGenerator::getSignaturesForOr() const {
  // Signature: or (condition,...) -> output:
  // boolean, boolean,.. -> boolean
  return {exec::FunctionSignatureBuilder()
              .argumentType("boolean")
              .variableArity("boolean")
              .returnType("boolean")
              .build()};
}

std::vector<exec::FunctionSignaturePtr>
SpecialFormSignatureGenerator::getSignaturesForCoalesce() const {
  // Signature: coalesce (input,...) -> output:
  // T, T,.. -> T
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .argumentType("T")
              .variableArity("T")
              .returnType("T")
              .build()};
}

std::vector<exec::FunctionSignaturePtr>
SpecialFormSignatureGenerator::getSignaturesForIf() const {
  // Signature: if (condition, then) -> output:
  // boolean, T -> T
  const auto ifThen = exec::FunctionSignatureBuilder()
                          .typeVariable("T")
                          .argumentType("boolean")
                          .argumentType("T")
                          .returnType("T")
                          .build();
  // Signature: if (condition, then, else) -> output:
  // boolean, T, T -> T
  const auto ifThenElse = exec::FunctionSignatureBuilder()
                              .typeVariable("T")
                              .argumentType("boolean")
                              .argumentType("T")
                              .argumentType("T")
                              .returnType("T")
                              .build();
  return {ifThen, ifThenElse};
}

std::vector<exec::FunctionSignaturePtr>
SpecialFormSignatureGenerator::getSignaturesForSwitch() const {
  // Signature: Switch (condition, then) -> output:
  // boolean, T -> T
  // This is only used to bind to a randomly selected type for the
  // output, then while generating arguments, an override is used
  // to generate inputs that can create variation of multiple
  // cases and may or may not include a final else clause.
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .argumentType("boolean")
              .argumentType("T")
              .returnType("T")
              .build()};
}

std::vector<exec::FunctionSignaturePtr>
SpecialFormSignatureGenerator::getSignaturesForCast() const {
  std::vector<exec::FunctionSignaturePtr> signatures;

  // To integral types.
  for (const auto& toType : kIntegralTypes_) {
    addCastFromIntegralSignatures(toType, signatures);
    addCastFromFloatingPointSignatures(toType, signatures);
    addCastFromVarcharSignature(toType, signatures);
  }

  // To floating-point types.
  for (const auto& toType : kFloatingPointTypes_) {
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

exec::FunctionSignaturePtr SpecialFormSignatureGenerator::makeCastSignature(
    const std::string& fromType,
    const std::string& toType) const {
  return exec::FunctionSignatureBuilder()
      .argumentType(fromType)
      .returnType(toType)
      .build();
}
} // namespace facebook::velox::fuzzer
