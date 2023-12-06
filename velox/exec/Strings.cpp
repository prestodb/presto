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

#include "velox/exec/Strings.h"

namespace facebook::velox::aggregate::prestosql {

StringView Strings::append(StringView value, HashStringAllocator& allocator) {
  VELOX_DCHECK(!value.isInline());

  maxStringSize = std::max(maxStringSize, value.size());

  // Request sufficient amount of memory to store the whole string
  // (value.size()) and allow some memory left for bookkeeping (header + link
  // to next block).
  const int32_t requiredBytes =
      value.size() + HashStringAllocator::Header::kContinuedPtrSize + 8;

  ByteOutputStream stream(&allocator);
  if (firstBlock == nullptr) {
    // Allocate first block.
    currentBlock = allocator.newWrite(stream, requiredBytes);
    firstBlock = currentBlock.header;
  } else {
    allocator.extendWrite(currentBlock, stream);
  }

  // Check if there is enough space left.
  auto& currentRange = stream.ranges().back();
  if (currentRange.size - currentRange.position < requiredBytes) {
    // Not enough space. Allocate new block.
    ByteRange newRange;
    allocator.newContiguousRange(requiredBytes, &newRange);

    stream.setRange(newRange, 0);
  }

  VELOX_DCHECK_LE(requiredBytes, stream.ranges().back().size);

  // Copy the string and return a StringView over the copy.
  char* start = stream.writePosition();
  stream.appendStringView(value);
  currentBlock = allocator.finishWrite(stream, maxStringSize * 4).second;
  return StringView(start, value.size());
}

void Strings::free(HashStringAllocator& allocator) {
  if (firstBlock != nullptr) {
    allocator.free(firstBlock);
    firstBlock = nullptr;
    currentBlock = {nullptr, nullptr};
  }
}
} // namespace facebook::velox::aggregate::prestosql
