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
#include "velox/functions/lib/CheckDuplicateKeys.h"

namespace facebook::velox::functions {

void checkDuplicateKeys(
    const MapVectorPtr& mapVector,
    const SelectivityVector& rows,
    exec::EvalCtx& context) {
  static const char* kDuplicateKey = "Duplicate map keys ({}) are not allowed";

  MapVector::canonicalize(mapVector);

  auto offsets = mapVector->rawOffsets();
  auto sizes = mapVector->rawSizes();
  auto mapKeys = mapVector->mapKeys();
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    auto offset = offsets[row];
    auto size = sizes[row];
    for (auto i = 1; i < size; i++) {
      if (mapKeys->equalValueAt(mapKeys.get(), offset + i, offset + i - 1)) {
        auto duplicateKey = mapKeys->wrappedVector()->toString(
            mapKeys->wrappedIndex(offset + i));
        VELOX_USER_FAIL(kDuplicateKey, duplicateKey);
      }
    }
  });
}
} // namespace facebook::velox::functions
