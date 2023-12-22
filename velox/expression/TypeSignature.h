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

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace facebook::velox::exec {

// Base type (e.g. map) and optional parameters (e.g. K, V).
class TypeSignature {
 public:
  /// @param baseName The base name of the type. Could describe a concrete type
  /// name (e.g. map, bigint, double), or a variable (e.g. K, V).
  /// @param parameters The optional parameters for the type. For example, the
  /// signature "map(K, V)" would have two parameters, "K", and "V". All
  /// parameters must be of the same ParameterType.
  /// @param rowFieldName if this type signature is a field of another parent
  /// row type, it can optionally have a name. E.g. `row(id bigint)` would have
  /// "id" set as rowFieldName in the "bigint" parameter.
  TypeSignature(
      std::string baseName,
      std::vector<TypeSignature> parameters,
      std::optional<std::string> rowFieldName = std::nullopt)
      : baseName_{std::move(baseName)},
        parameters_{std::move(parameters)},
        rowFieldName_(rowFieldName) {}

  const std::string& baseName() const {
    return baseName_;
  }

  const std::vector<TypeSignature>& parameters() const {
    return parameters_;
  }

  const std::optional<std::string>& rowFieldName() const {
    return rowFieldName_;
  }

  std::string toString() const;

  bool operator==(const TypeSignature& rhs) const {
    return baseName_ == rhs.baseName_ && parameters_ == rhs.parameters_ &&
        rowFieldName_ == rhs.rowFieldName_;
  }

 private:
  const std::string baseName_;
  const std::vector<TypeSignature> parameters_;

  // If this object is a field of another parent row type, it can optionally
  // have a name, e.g, `row(id bigint)`
  std::optional<std::string> rowFieldName_;
};

using TypeSignaturePtr = std::shared_ptr<TypeSignature>;

} // namespace facebook::velox::exec
