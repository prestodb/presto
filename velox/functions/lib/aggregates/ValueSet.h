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
#include "velox/vector/BaseVector.h"

namespace facebook::velox::aggregate {

class ValueSet {
 public:
  explicit ValueSet(HashStringAllocator* allocator) : allocator_(allocator) {}

  // Serialize a value in `vector` at `index` to memory allocated via
  // `allocator`. Update `position` to the start address of the written data.
  // Notice that the caller is responsible for freeing up all the space written
  // through this function.
  void write(
      const BaseVector& vector,
      vector_size_t index,
      HashStringAllocator::Position& position) const;

  StringView write(const StringView& value) const;

  // Deserialize the data written in memory at `position` to `vector` at
  // `index`.
  void read(
      BaseVector* vector,
      vector_size_t index,
      const HashStringAllocator::Header* header) const;

  void free(HashStringAllocator::Header* header) const;

  void free(const StringView& value) const;

 private:
  HashStringAllocator* allocator_;
};

} // namespace facebook::velox::aggregate
