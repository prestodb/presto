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

#include "velox/core/ITypedExpr.h"

namespace facebook::velox::core {

namespace {
folly::F14FastMap<ExprKind, std::string> exprKindNames() {
  static const folly::F14FastMap<ExprKind, std::string> kNames = {
      {ExprKind::kInput, "INPUT"},
      {ExprKind::kFieldAccess, "FIELD"},
      {ExprKind::kDereference, "DEREFERENCE"},
      {ExprKind::kCall, "CALL"},
      {ExprKind::kCast, "CAST"},
      {ExprKind::kConstant, "CONSTANT"},
      {ExprKind::kConcat, "CONCAT"},
      {ExprKind::kLambda, "LAMBDA"},
  };

  return kNames;
}
} // namespace

VELOX_DEFINE_ENUM_NAME(ExprKind, exprKindNames);
} // namespace facebook::velox::core
