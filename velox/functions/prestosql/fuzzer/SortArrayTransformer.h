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

#include "velox/core/Expressions.h"
#include "velox/exec/fuzzer/ExprTransformer.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::test {

using facebook::velox::TypePtr;
using facebook::velox::core::TypedExprPtr;
using facebook::velox::exec::test::ExprTransformer;

class SortArrayTransformer : public ExprTransformer {
 public:
  ~SortArrayTransformer() override = default;

  /// Wraps 'expr' in a call to array_sort. If the type of 'expr' contains a
  /// map, array_sort doesn't support this type, so we return a constant null
  /// instead.
  TypedExprPtr transform(TypedExprPtr expr) const override {
    facebook::velox::TypePtr type = expr->type();
    if (containsMap(type)) {
      // TODO: support map type by using array_sort with a lambda that casts
      // array elements to JSON before comparison.
      return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
          type, facebook::velox::variant::null(type->kind()));
    } else {
      return std::make_shared<facebook::velox::core::CallTypedExpr>(
          type,
          std::vector<TypedExprPtr>{std::move(expr)},
          "$internal$canonicalize");
    }
  }

  int32_t extraLevelOfNesting() const override {
    return 1;
  }

 private:
  bool containsMap(const TypePtr& type) const {
    if (type->isMap()) {
      return true;
    } else if (type->isArray()) {
      return containsMap(type->asArray().elementType());
    } else if (type->isRow()) {
      for (const auto& child : type->asRow().children()) {
        if (containsMap(child)) {
          return true;
        }
      }
    }
    return false;
  }
};

} // namespace facebook::velox::exec::test
