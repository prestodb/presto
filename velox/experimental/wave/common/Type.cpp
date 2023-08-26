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
  PhysicalType ans{};
  switch (type.kind()) {
    case TypeKind::TINYINT:
      ans.kind = PhysicalType::kInt8;
      break;
    case TypeKind::SMALLINT:
      ans.kind = PhysicalType::kInt16;
      break;
    case TypeKind::INTEGER:
      ans.kind = PhysicalType::kInt32;
      break;
    case TypeKind::BIGINT:
      ans.kind = PhysicalType::kInt64;
      break;
    case TypeKind::REAL:
      ans.kind = PhysicalType::kFloat32;
      break;
    case TypeKind::DOUBLE:
      ans.kind = PhysicalType::kFloat64;
      break;
    case TypeKind::VARCHAR:
      ans.kind = PhysicalType::kString;
      break;
    default:
      VELOX_UNSUPPORTED("{}", type.kind());
  }
  return ans;
}

} // namespace facebook::velox::wave
