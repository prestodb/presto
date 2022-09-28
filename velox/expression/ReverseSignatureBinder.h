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

#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

/// Bind a function signature with a concrete return type. bindings() return a
/// map from each type variable in the signature to the corresponding concrete
/// type if determined, or a nullptr if the type variable cannot be determined
/// by the return type.
class ReverseSignatureBinder : private SignatureBinderBase {
 public:
  ReverseSignatureBinder(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType)
      : SignatureBinderBase{signature}, returnType_{returnType} {}

  /// Try bind returnType_ to the return type of the function signature. Return
  /// true if the binding succeeds, or false otherwise.
  bool tryBind();

  /// Return the determined bindings. This function should be called after
  /// tryBind() and only if tryBind() returns true. If a type variable is not
  /// determined by tryBind(), it maps to a nullptr.
  const std::unordered_map<std::string, TypePtr>& bindings() const {
    return typeParameters_;
  }

 private:
  /// Return whether there is a constraint on the type in the function
  /// signature.
  bool isConstrainedType(const TypeSignature& type) const;

  const TypePtr returnType_;
};

} // namespace facebook::velox::exec
