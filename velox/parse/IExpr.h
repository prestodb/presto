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

#include <fmt/format.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::core {

class IExpr;
using ExprPtr = std::shared_ptr<const IExpr>;

/// An implicitly-typed expression, such as function call, literal, etc.
class IExpr {
 public:
  enum class Kind : int8_t {
    kInput = 0,
    kFieldAccess = 1,
    kCall = 2,
    kCast = 3,
    kConstant = 4,
    kLambda = 5,
    kSubquery = 6,
  };

  explicit IExpr(
      Kind kind,
      std::vector<ExprPtr> inputs,
      std::optional<std::string> alias = std::nullopt)
      : kind_{kind}, inputs_{std::move(inputs)}, alias_{std::move(alias)} {}

  virtual ~IExpr() = default;

  Kind kind() const {
    return kind_;
  }

  bool is(Kind kind) const {
    return kind_ == kind;
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

  const std::vector<ExprPtr>& inputs() const {
    return inputs_;
  }

  const ExprPtr& input() const {
    VELOX_CHECK_EQ(1, inputs_.size());
    return inputs_.at(0);
  }

  const ExprPtr& inputAt(size_t index) const {
    VELOX_CHECK_LT(index, inputs_.size());
    return inputs_.at(index);
  }

  const std::optional<std::string>& alias() const {
    return alias_;
  }

  virtual std::string toString() const = 0;

  virtual ExprPtr replaceInputs(std::vector<ExprPtr> newInputs) const = 0;

  size_t hash() const {
    size_t hash = localHash();
    for (size_t i = 0; i < inputs_.size(); ++i) {
      hash = bits::hashMix(hash, inputs_[i]->hash());
    }

    if (alias_.has_value()) {
      hash = bits::hashMix(hash, std::hash<std::string>{}(alias_.value()));
    }

    hash = bits::hashMix(hash, std::hash<int8_t>{}(static_cast<int8_t>(kind_)));

    return hash;
  }

  virtual bool operator==(const IExpr& other) const = 0;

 protected:
  // Returns a hash that include values specific to the expression. Doesn't
  // include inputs, kind or alias.
  virtual size_t localHash() const = 0;

  std::string appendAliasIfExists(std::string name) const {
    if (!alias_.has_value()) {
      return name;
    }

    return fmt::format("{} AS {}", std::move(name), alias_.value());
  }

  bool compareAliasAndInputs(const IExpr& other) const {
    if (alias() != other.alias() || inputs().size() != other.inputs().size()) {
      return false;
    }

    for (size_t i = 0; i < inputs().size(); ++i) {
      if (!(*inputs()[i] == *other.inputs()[i])) {
        return false;
      }
    }
    return true;
  }

  const Kind kind_;
  const std::vector<ExprPtr> inputs_;
  const std::optional<std::string> alias_;
};

/// Hash functor for IExpr to use with folly::F14HashMap and other hash
/// containers. This functor can be used with ExprPtr keys.
struct IExprHash {
  size_t operator()(const ExprPtr& expr) const {
    return expr->hash();
  }
};

/// Equal functor for IExpr to use with folly::F14HashMap and other hash
/// containers. This functor can be used with ExprPtr keys.
struct IExprEqual {
  bool operator()(const ExprPtr& lhs, const ExprPtr& rhs) const {
    return *lhs == *rhs;
  }
};

} // namespace facebook::velox::core
