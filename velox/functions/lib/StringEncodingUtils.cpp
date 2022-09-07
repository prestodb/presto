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

#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/vector/VectorEncoding.h"

namespace facebook::velox::functions {

bool prepareFlatResultsVector(
    VectorPtr& result,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    VectorPtr& argToReuse) {
  if (!result && BaseVector::isReusableFlatVector(argToReuse)) {
    // Move input vector to result
    VELOX_CHECK(
        VectorEncoding::isFlat(argToReuse.get()->encoding()) &&
        argToReuse.get()->typeKind() == TypeKind::VARCHAR);

    result = std::move(argToReuse);
    return true;
  }
  // This will allocate results if not allocated
  BaseVector::ensureWritable(rows, VARCHAR(), context.pool(), result);

  VELOX_CHECK(VectorEncoding::isFlat(result->encoding()));
  return false;
}

} // namespace facebook::velox::functions
