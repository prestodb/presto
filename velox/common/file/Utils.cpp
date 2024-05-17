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

#include "velox/common/file/Utils.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::file::utils {

bool CoalesceIfDistanceLE::operator()(
    const velox::common::Region& a,
    const velox::common::Region& b) const {
  VELOX_CHECK_LE(a.offset, b.offset, "Regions to combine must be sorted.");
  const uint64_t beginGap = a.offset + a.length, endGap = b.offset;

  VELOX_CHECK_LE(beginGap, endGap, "Regions to combine can't overlap.");
  const uint64_t gap = endGap - beginGap;

  const bool shouldCoalesce = gap <= maxCoalescingDistance_;
  if (coalescedBytes_ && shouldCoalesce) {
    *coalescedBytes_ += gap;
  }
  return shouldCoalesce;
}

} // namespace facebook::velox::file::utils
