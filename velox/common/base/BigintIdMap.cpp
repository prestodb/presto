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

#include "velox/common/base/BigintIdMap.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

void BigintIdMap::makeTable(int64_t capacity) {
  VELOX_CHECK_LE(capacity, kMaxCapacity);
  byteSize_ = capacity * kEntrySize + kReadPadding;
  table_ = reinterpret_cast<char*>(pool_.allocate(byteSize_));
  memset(table_, 0, byteSize_);
  capacity_ = capacity;
  sizeMask_ = capacity_ - 1;
  limit_ = capacity_ * kEntrySize;
  maxEntries_ = capacity_ - capacity_ / 4;
}

void BigintIdMap::resize(int64_t newCapacity) {
  VELOX_CHECK_LE(newCapacity, kMaxCapacity);

  auto oldCapacity = capacity_;
  auto oldTable = table_;
  auto oldByteSize = byteSize_;
  makeTable(newCapacity);
  for (auto i = 0; i < oldCapacity; ++i) {
    auto ptr = valuePtr(oldTable, i);
    if (*ptr == kEmptyMarker) {
      continue;
    }
    auto newIndex = indexOfEntry(*ptr);
    auto newPtr = valuePtr(table_, newIndex);
    while (*newPtr != kEmptyMarker) {
      newIndex = (newIndex + 1) & sizeMask_;
      newPtr = valuePtr(table_, newIndex);
    }
    *newPtr = *ptr;
    *idPtr(newPtr) = *idPtr(ptr);
  }
  pool_.free(oldTable, oldByteSize);
}

} // namespace facebook::velox
