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

#include "velox/functions/lib/CheckNestedNulls.h"

namespace facebook::velox::functions {

/// Checks nested nulls in a complex type vector. Returns true if value at
/// specified index is null. Throws an exception if the base vector contains
/// nulls if 'throwOnNestedNulls' is true.
bool checkNestedNulls(
    const DecodedVector& decoded,
    const vector_size_t* indices,
    vector_size_t index,
    bool throwOnNestedNulls) {
  if (decoded.isNullAt(index)) {
    return true;
  }

  if (throwOnNestedNulls) {
    VELOX_USER_CHECK(
        !decoded.base()->containsNullAt(indices[index]),
        "{} comparison not supported for values that contain nulls",
        mapTypeKindToName(decoded.base()->typeKind()));
  }

  return false;
}
} // namespace facebook::velox::functions
