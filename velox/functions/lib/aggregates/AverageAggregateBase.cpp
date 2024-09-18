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

#include "velox/functions/lib/aggregates/AverageAggregateBase.h"

namespace facebook::velox::functions::aggregate {

void checkAvgIntermediateType(const TypePtr& type) {
  VELOX_USER_CHECK(
      type->isRow() || type->isVarbinary(),
      "Input type for final average must be row type or varbinary type.");
  if (type->kind() == TypeKind::VARBINARY) {
    return;
  }
  VELOX_USER_CHECK(
      type->childAt(0)->kind() == TypeKind::DOUBLE ||
          type->childAt(0)->isLongDecimal(),
      "Input type for sum in final average must be double or long decimal type.");
  VELOX_USER_CHECK_EQ(
      type->childAt(1)->kind(),
      TypeKind::BIGINT,
      "Input type for count in final average must be bigint type.");
}

} // namespace facebook::velox::functions::aggregate
