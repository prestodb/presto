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
#pragma once

#include "velox/common/memory/HashStringAllocator.h"

namespace facebook::velox::aggregate::prestosql {

/// Stores non-inlined strings in memory blocks managed by
/// HashStringAllocator.
struct Strings {
  /// Header of the first block allocated via HashStringAllocator.
  HashStringAllocator::Header* firstBlock{nullptr};

  /// Header of the last block allocated via HashStringAllocator. Blocks
  /// are forming a singly-linked list via Header::nextContinued.
  /// HashStringAllocator takes care of linking the blocks.
  HashStringAllocator::Position currentBlock{nullptr, nullptr};

  /// Maximum size of a string passed to 'append'. Used to calculate the amount
  /// of space to reserve for future strings.
  size_t maxStringSize = 0;

  /// Copies the string into contiguous memory allocated via
  /// HashStringAllocator. Returns StringView over the copy.
  StringView append(StringView value, HashStringAllocator& allocator);

  /// Frees memory used by the strings. StringViews returned from 'append'
  /// become invalid after this call.
  void free(HashStringAllocator& allocator);
};
} // namespace facebook::velox::aggregate::prestosql
