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

#include <random>
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::test {

/// For function signatures using type variables, generates a list of
/// arguments types. Optionally, allows to specify a desired return type. If
/// specified, the return type acts as a constraint on the possible set of
/// argument types.
class ArgumentTypeFuzzer {
 public:
  ArgumentTypeFuzzer(
      const exec::FunctionSignature& signature,
      std::mt19937& rng)
      : signature_{signature}, rng_{rng} {}

  ArgumentTypeFuzzer(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      std::mt19937& rng)
      : signature_{signature}, returnType_{returnType}, rng_{rng} {
    VELOX_CHECK_NOT_NULL(returnType);
  }

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

 private:
  /// Bind each type variable that is not determined by the return type to a
  /// randomly generated type.
  void determineUnboundedTypeVariables();

  const exec::FunctionSignature& signature_;

  const TypePtr returnType_;

  std::vector<TypePtr> argumentTypes_;

  /// Bindings between type variables and their actual types.
  std::unordered_map<std::string, TypePtr> bindings_;

  /// RNG to generate random types for unbounded type variables when necessary.
  std::mt19937& rng_;
};

/// Return the kind name of type in lower case. This is expected to match the
/// TypeSignature::baseName_ corresponding to type.
std::string typeToBaseName(const TypePtr& type);

/// Return the TypeKind that corresponds to typeName.
std::optional<TypeKind> baseNameToTypeKind(const std::string& typeName);

} // namespace facebook::velox::test
