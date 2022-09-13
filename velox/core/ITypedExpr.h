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

#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox::core {

/* a strongly-typed expression, such as literal, function call, etc... */
class ITypedExpr {
 public:
  explicit ITypedExpr(std::shared_ptr<const Type> type)
      : type_{std::move(type)}, inputs_{} {}

  ITypedExpr(
      std::shared_ptr<const Type> type,
      std::vector<std::shared_ptr<const ITypedExpr>> inputs)
      : type_{std::move(type)}, inputs_{std::move(inputs)} {}

  const std::shared_ptr<const Type>& type() const {
    return type_;
  }

  virtual ~ITypedExpr() = default;

  const std::vector<std::shared_ptr<const ITypedExpr>>& inputs() const {
    return inputs_;
  }

  /// Returns a copy of this expression with input fields renamed according
  /// to specified 'mapping'. Fields specified in the 'mapping are renamed.
  /// Fields not present in 'mapping' are left unmodified.
  ///
  /// Used to bind inputs to lambda functions.
  virtual std::shared_ptr<const ITypedExpr> rewriteInputNames(
      const std::unordered_map<std::string, std::string>& mapping) const = 0;

  virtual std::string toString() const = 0;

  virtual size_t localHash() const = 0;

  size_t hash() const {
    size_t hash = bits::hashMix(type_->hashKind(), localHash());
    for (int32_t i = 0; i < inputs_.size(); ++i) {
      hash = bits::hashMix(hash, inputs_[i]->hash());
    }
    return hash;
  }

  // Returns true if other is recursively equal to 'this'. We do not
  // overload == because this is overloaded in a subclass for a
  // different purpose.
  bool equals(const ITypedExpr& other) const {
    if (type_ != other.type_ || inputs_.size() != other.inputs_.size()) {
      return false;
    }
    if (!equalsNonRecursive(other)) {
      return false;
    }
    for (int32_t i = 0; i < inputs_.size(); ++i) {
      if (*inputs_[i] == *other.inputs_[i]) {
        return false;
      }
    }
    return true;
  }

  virtual bool operator==(const ITypedExpr& other) const = 0;

 protected:
  std::vector<std::shared_ptr<const ITypedExpr>> rewriteInputsRecursive(
      const std::unordered_map<std::string, std::string>& mapping) const {
    std::vector<std::shared_ptr<const ITypedExpr>> newInputs;
    newInputs.reserve(inputs().size());
    for (const auto& input : inputs()) {
      newInputs.emplace_back(input->rewriteInputNames(mapping));
    }
    return newInputs;
  }

 private:
  virtual bool equalsNonRecursive(const ITypedExpr& other) const {
    return false;
  }

  std::shared_ptr<const Type> type_;
  std::vector<std::shared_ptr<const ITypedExpr>> inputs_;
};

using TypedExprPtr = std::shared_ptr<const ITypedExpr>;

} // namespace facebook::velox::core
