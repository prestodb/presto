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

#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"
#include "velox/exec/ContainerRowSerde.h"

namespace facebook::velox::functions::aggregate {

// An accumulator for a single variable-width value (a string, a map, an array
// or a struct).
void SingleValueAccumulator::write(
    const BaseVector* vector,
    vector_size_t index,
    HashStringAllocator* allocator) {
  ByteOutputStream stream(allocator);
  if (start_.header == nullptr) {
    start_ = allocator->newWrite(stream);
  } else {
    allocator->extendWrite(start_, stream);
  }

  static const exec::ContainerRowSerdeOptions options{};
  exec::ContainerRowSerde::serialize(*vector, index, stream, options);
  allocator->finishWrite(stream, 0);
}

void SingleValueAccumulator::read(const VectorPtr& vector, vector_size_t index)
    const {
  VELOX_CHECK_NOT_NULL(start_.header);

  auto stream = HashStringAllocator::prepareRead(start_.header);
  exec::ContainerRowSerde::deserialize(*stream, index, vector.get());
}

bool SingleValueAccumulator::hasValue() const {
  return start_.header != nullptr;
}

std::optional<int32_t> SingleValueAccumulator::compare(
    const DecodedVector& decoded,
    vector_size_t index,
    CompareFlags compareFlags) const {
  VELOX_CHECK_NOT_NULL(start_.header);

  auto stream = HashStringAllocator::prepareRead(start_.header);
  return exec::ContainerRowSerde::compareWithNulls(
      *stream, decoded, index, compareFlags);
}

void SingleValueAccumulator::destroy(HashStringAllocator* allocator) {
  if (start_.header != nullptr) {
    allocator->free(start_.header);
    start_.header = nullptr;
  }
}

} // namespace facebook::velox::functions::aggregate
