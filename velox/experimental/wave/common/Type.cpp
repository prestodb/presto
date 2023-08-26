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

#include "velox/experimental/wave/common/Type.h"

#include "velox/type/Type.h"

namespace facebook::velox::wave {

PhysicalType fromCpuType(const Type& type) {
  switch (type.kind()) {
    case TypeKind::TINYINT:
      return {.kind = PhysicalType::kInt8};
    case TypeKind::SMALLINT:
      return {.kind = PhysicalType::kInt16};
    case TypeKind::INTEGER:
      return {.kind = PhysicalType::kInt32};
    case TypeKind::BIGINT:
      return {.kind = PhysicalType::kInt64};
    case TypeKind::REAL:
      return {.kind = PhysicalType::kFloat32};
    case TypeKind::DOUBLE:
      return {.kind = PhysicalType::kFloat64};
    case TypeKind::VARCHAR:
      return {.kind = PhysicalType::kString};
    default:
      VELOX_UNSUPPORTED("{}", type.kind());
  }
}

} // namespace facebook::velox::wave
