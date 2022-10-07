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

#include <folly/Random.h>

#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::test {

/// For function signatures containing type variables, try generate a list of
/// arguments types such that the function taking these arugments returns the
/// given type. If there are type variables unbounded by the return type,
/// generate a random type for them with seed_.
class ArgumentTypeFuzzer {
 public:
  ArgumentTypeFuzzer(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      std::mt19937& seed)
      : signature_{signature}, returnType_{returnType}, seed_{seed} {}

  /// Generate random argument types if returnType_ can be bound to the return
  /// type of signature_. Return true if the generation succeeds, false
  /// otherwise. If signature_ has variable arity, repeat the last argument at
  /// most maxVariadicArgs times.
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

  /// Seed to generate random types for unbounded type variables when necessary.
  std::mt19937& seed_;
};

} // namespace facebook::velox::test
