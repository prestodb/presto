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

#include "velox/dwio/common/SelectiveByteRleColumnReader.h"

namespace facebook::velox::dwio::common {

void SelectiveByteRleColumnReader::getValues(
    const RowSet& rows,
    VectorPtr* result) {
  switch (requestedType_->kind()) {
    case TypeKind::BOOLEAN:
      getFlatValues<int8_t, bool>(rows, result, requestedType_);
      break;
    case TypeKind::TINYINT:
      getFlatValues<int8_t, int8_t>(rows, result, requestedType_);
      break;
    case TypeKind::SMALLINT:
      getFlatValues<int8_t, int16_t>(rows, result, requestedType_);
      break;
    case TypeKind::INTEGER:
      getFlatValues<int8_t, int32_t>(rows, result, requestedType_);
      break;
    case TypeKind::BIGINT:
      getFlatValues<int8_t, int64_t>(rows, result, requestedType_);
      break;
    default:
      VELOX_FAIL(
          "Result type not supported in ByteRLE encoding: {}",
          requestedType_->toString());
  }
}

} // namespace facebook::velox::dwio::common
