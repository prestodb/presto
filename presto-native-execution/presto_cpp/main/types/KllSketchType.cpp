/*
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

#include "presto_cpp/main/types/KllSketchType.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto {

std::shared_ptr<const KllSketchType> KllSketchType::get(
    const velox::TypePtr& dataType) {
  VELOX_CHECK_NOT_NULL(dataType, "KllSketch data type cannot be null");

  // Validate supported types
  VELOX_CHECK(
      dataType->isDouble() || dataType->isBigint() || dataType->isVarchar() ||
          dataType->isBoolean(),
      "KllSketch only supports DOUBLE, BIGINT, VARCHAR, and BOOLEAN types, got: {}",
      dataType->toString());

  return std::make_shared<const KllSketchType>(
      KllSketchType::PrivateTag{}, dataType);
}

} // namespace facebook::presto
