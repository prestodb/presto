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
#include "velox/exec/ContainerRowSerde.h"

namespace facebook::velox::functions::aggregate {

// An accumulator for a single variable-width value (a string, a map, an array
// or a struct).
struct SingleValueAccumulator {
  void write(
      const BaseVector* vector,
      vector_size_t index,
      HashStringAllocator* allocator);

  void read(const VectorPtr& vector, vector_size_t index) const;

  bool hasValue() const;

  // Returns 0 if stored and new values are equal; <0 if stored value is less
  // then new value; >0 if stored value is greated than new value
  int32_t compare(const DecodedVector& decoded, vector_size_t index) const;

  void destroy(HashStringAllocator* allocator);

 private:
  static constexpr int kInitialBytes{20};

  HashStringAllocator::Header* begin_{nullptr};
};

} // namespace facebook::velox::functions::aggregate
