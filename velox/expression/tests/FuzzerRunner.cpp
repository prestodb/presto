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
#include "velox/expression/tests/FuzzerRunner.h"

namespace {

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

  // TODO: Enable this signature after we fix the timestamp --> varchar
  // conversion. https://github.com/facebookincubator/velox/issues/4169.
  // addCastFromTimestampSignature("varchar", signatures);

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

} // namespace

// static
const std::unordered_map<
    std::string,
    std::vector<facebook::velox::exec::FunctionSignaturePtr>>
    FuzzerRunner::kSpecialForms = {
        {"and",
         std::vector<facebook::velox::exec::FunctionSignaturePtr>{
             // Signature: and (condition,...) -> output:
             // boolean, boolean,.. -> boolean
             facebook::velox::exec::FunctionSignatureBuilder()
                 .argumentType("boolean")
                 .argumentType("boolean")
                 .variableArity()
                 .returnType("boolean")
                 .build()}},
        {"or",
         std::vector<facebook::velox::exec::FunctionSignaturePtr>{
             // Signature: or (condition,...) -> output:
             // boolean, boolean,.. -> boolean
             facebook::velox::exec::FunctionSignatureBuilder()
                 .argumentType("boolean")
                 .argumentType("boolean")
                 .variableArity()
                 .returnType("boolean")
                 .build()}},
        {"coalesce",
         std::vector<facebook::velox::exec::FunctionSignaturePtr>{
             // Signature: coalesce (input,...) -> output:
             // T, T,.. -> T
             facebook::velox::exec::FunctionSignatureBuilder()
                 .typeVariable("T")
                 .argumentType("T")
                 .argumentType("T")
                 .variableArity()
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
