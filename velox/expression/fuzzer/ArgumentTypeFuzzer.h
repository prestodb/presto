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

#include <unordered_map>

#include "velox/expression/FunctionSignature.h"
#include "velox/type/Type.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::fuzzer {

/// For function signatures using type variables, generates a list of
/// arguments types. Optionally, allows to specify a desired return type. If
/// specified, the return type acts as a constraint on the possible set of
/// argument types. If no return type is specified, it also allows generate a
/// random type that can bind to the function's return type.
class ArgumentTypeFuzzer {
 public:
  ArgumentTypeFuzzer(
      const exec::FunctionSignature& signature,
      FuzzerGenerator& rng,
      const std::vector<TypePtr>& scalarTypes = velox::defaultScalarTypes())
      : ArgumentTypeFuzzer(signature, nullptr, rng, scalarTypes) {}

  ArgumentTypeFuzzer(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      FuzzerGenerator& rng,
      const std::vector<TypePtr>& scalarTypes = velox::defaultScalarTypes())
      : signature_{signature},
        returnType_{returnType},
        rng_{rng},
        scalarTypes_(scalarTypes) {}

  /// Generate random argument types. If the desired returnType has been
  /// specified, checks that it can be bound to the return type of signature_.
  /// Return true if the generation succeeds, false otherwise. If signature_ has
  /// variable arity, repeat the last argument at most maxVariadicArgs times.
  bool fuzzArgumentTypes(uint32_t maxVariadicArgs);

  /// Return the generated list of argument types. This function should be
  /// called after fuzzArgumentTypes() is called and returns true.
  const std::vector<TypePtr>& argumentTypes() const {
    return argumentTypes_;
  }

  /// Return a random type that can bind to the function signature's return
  /// type and set returnType_ to this type. This function can only be called
  /// when returnType_ is uninitialized.
  TypePtr fuzzReturnType();

 private:
  /// Return the variables in the signature.
  auto& variables() const {
    return signature_.variables();
  }

  // Returns random integer between min and max inclusive.
  int32_t rand32(int32_t min, int32_t max);

  // Bind each type variable that is not determined by the return type to a
  // randomly generated type.
  void determineUnboundedTypeVariables();

  // Bind integer variables used in specified decimal(p,s) type signature to
  // randomly generated values if not already bound.
  // Noop if 'type' is not a decimal type signature.
  void determineUnboundedIntegerVariables(const exec::TypeSignature& type);

  // Bind LongEnumParameter variables used in enum type signatures to
  // randomly generated values if not already bound.
  void determineUnboundedEnumVariables(const exec::TypeSignature& type);

  TypePtr randType();

  /// Generates an orderable random type, including structs, and arrays.
  TypePtr randOrderableType();

  // Bind 'name' variable, if not already bound, using 'constant' constraint
  // ('name'='123'). Return bound value if 'name' is already bound or was
  // successfully bound to a constant value. Return std::nullopt otherwise.
  std::optional<int> tryFixedBinding(const std::string& name);

  // Bind the precision and scale variables in a decimal type signature to
  // constant values. Return std::nullopt if the variable cannot be bound to a
  // constant value.
  std::pair<std::optional<int>, std::optional<int>> tryBindFixedPrecisionScale(
      const exec::TypeSignature& type);

  // Find all the nested decimal type signatures recursively.
  void findDecimalTypes(
      const exec::TypeSignature& type,
      std::vector<exec::TypeSignature>& decimalTypes) const;

  const exec::FunctionSignature& signature_;

  TypePtr returnType_;

  std::vector<TypePtr> argumentTypes_;

  /// Bindings between type variables and their actual types.
  std::unordered_map<std::string, TypePtr> bindings_;

  std::unordered_map<std::string, int> integerBindings_;

  /// Bindings between LongEnumParameter variables and their actual types.
  std::unordered_map<std::string, LongEnumParameter> longEnumParameterBindings_;

  /// Bindings between VarcharEnumParameter variables and their actual types.
  std::unordered_map<std::string, VarcharEnumParameter>
      varcharEnumParameterBindings_;

  /// RNG to generate random types for unbounded type variables when necessary.
  FuzzerGenerator& rng_;

  /// The set of scalar types that can be used in random argument types.
  const std::vector<TypePtr> scalarTypes_;
};

/// Return the kind name of type in lower case. This is expected to match the
/// TypeSignature::baseName_ corresponding to type.
std::string typeToBaseName(const TypePtr& type);

/// Return the TypeKind that corresponds to typeName.
std::optional<TypeKind> baseNameToTypeKind(const std::string& typeName);

} // namespace facebook::velox::fuzzer
