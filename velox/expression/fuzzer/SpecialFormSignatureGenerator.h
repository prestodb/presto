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

#include "velox/functions/FunctionRegistry.h"

namespace facebook::velox::fuzzer {

// Generates signatures of special forms for the expression fuzzer test to use.
class SpecialFormSignatureGenerator {
 public:
  virtual ~SpecialFormSignatureGenerator() {}

  /// Generates valid signatures for the specified special forms and append them
  /// to the signature map.
  /// @param specialForms Specifies a list of special form names delimited by
  /// comma.
  void appendSpecialForms(
      FunctionSignatureMap& signatureMap,
      const std::string& specialForms) const;

 protected:
  /// Generates signatures for cast from integral types to the given type and
  /// adds them to signatures.
  void addCastFromIntegralSignatures(
      const std::string& toType,
      std::vector<exec::FunctionSignaturePtr>& signatures) const;

  /// Generates signatures for cast from floating-point types to the given type
  /// and adds them to signatures.
  void addCastFromFloatingPointSignatures(
      const std::string& toType,
      std::vector<exec::FunctionSignaturePtr>& signatures) const;

  /// Generates signatures for cast from varchar to the given type and adds them
  /// to signatures.
  void addCastFromVarcharSignature(
      const std::string& toType,
      std::vector<exec::FunctionSignaturePtr>& signatures) const;

  /// Generates signatures for cast from timestamp to the given type and adds
  /// them to signatures.
  void addCastFromTimestampSignature(
      const std::string& toType,
      std::vector<exec::FunctionSignaturePtr>& signatures) const;

  /// Generates signatures for cast from date to the given type and adds them to
  /// signatures.
  void addCastFromDateSignature(
      const std::string& toType,
      std::vector<exec::FunctionSignaturePtr>& signatures) const;

  // Returns the map of special form names to their signatures.
  virtual const std::
      unordered_map<std::string, std::vector<exec::FunctionSignaturePtr>>&
      getSignatures() const;

  // Returns the signatures for the 'and' special form.
  virtual std::vector<exec::FunctionSignaturePtr> getSignaturesForAnd() const;

  // Returns the signatures for the 'or' special form.
  virtual std::vector<exec::FunctionSignaturePtr> getSignaturesForOr() const;

  // Returns the signatures for the 'coalesce' special form.
  virtual std::vector<exec::FunctionSignaturePtr> getSignaturesForCoalesce()
      const;

  // Returns the signatures for the 'if' special form.
  virtual std::vector<exec::FunctionSignaturePtr> getSignaturesForIf() const;

  // Returns the signatures for the 'switch' special form.
  virtual std::vector<exec::FunctionSignaturePtr> getSignaturesForSwitch()
      const;

  // Returns the signatures for the 'cast' special form.
  virtual std::vector<exec::FunctionSignaturePtr> getSignaturesForCast() const;

  // Creates a signature for the cast(fromType as toType).
  exec::FunctionSignaturePtr makeCastSignature(
      const std::string& fromType,
      const std::string& toType) const;

  const std::vector<std::string> kIntegralTypes_{
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "boolean"};

  const std::vector<std::string> kFloatingPointTypes_{"real", "double"};
};

} // namespace facebook::velox::fuzzer
