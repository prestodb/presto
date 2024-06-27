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

#include "velox/functions/prestosql/SplitToMap.h"

namespace facebook::velox::functions {

namespace {

core::CallTypedExprPtr asSplitToMapCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    if (call->name() == prefix + "split_to_map") {
      return call;
    }
  }
  return nullptr;
}

} // namespace

core::TypedExprPtr rewriteSplitToMapCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  const auto call = asSplitToMapCall(prefix, expr);
  if (call == nullptr || call->inputs().size() != 4) {
    return nullptr;
  }

  const auto* lambda =
      dynamic_cast<const core::LambdaTypedExpr*>(call->inputs()[3].get());
  VELOX_CHECK_NOT_NULL(lambda);

  const auto v1 = lambda->signature()->nameOf(1);
  const auto v2 = lambda->signature()->nameOf(2);

  const auto& body = lambda->body();
  std::optional<bool> keepFirst;
  if (auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(body)) {
    if (field->isInputColumn()) {
      if (field->name() == v1) {
        keepFirst = true;
      } else if (field->name() == v2) {
        keepFirst = false;
      }
    }
  }

  if (!keepFirst.has_value()) {
    static const std::string kNotSupported =
        "split_to_map with arbitrary lambda is not supported: {}";
    VELOX_USER_FAIL(kNotSupported, lambda->toString())
  }

  return std::make_shared<core::CallTypedExpr>(
      call->type(),
      std::vector<core::TypedExprPtr>{
          call->inputs()[0],
          call->inputs()[1],
          call->inputs()[2],
          std::make_shared<core::ConstantTypedExpr>(
              BOOLEAN(), keepFirst.value())},
      "$internal$split_to_map");
}

} // namespace facebook::velox::functions
