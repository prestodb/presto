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

namespace facebook::velox::functions::aggregate {

// An accumulator for a single variable-width value (a string, a map, an array
// or a struct).
void SingleValueAccumulator::write(
    const BaseVector* vector,
    vector_size_t index,
    HashStringAllocator* allocator) {
  if (!begin_) {
    begin_ = allocator->allocate(kInitialBytes);
  }

  ByteStream stream(allocator);
  allocator->extendWrite({begin_, begin_->begin()}, stream);
  exec::ContainerRowSerde::instance().serialize(*vector, index, stream);
  allocator->finishWrite(stream, stream.size());
}

void SingleValueAccumulator::read(const VectorPtr& vector, vector_size_t index)
    const {
  VELOX_CHECK(begin_);

  ByteStream inStream;
  HashStringAllocator::prepareRead(begin_, inStream);
  exec::ContainerRowSerde::instance().deserialize(
      inStream, index, vector.get());
}

bool SingleValueAccumulator::hasValue() const {
  return begin_ != nullptr;
}

// Returns 0 if stored and new values are equal; <0 if stored value is less
// then new value; >0 if stored value is greated than new value
int32_t SingleValueAccumulator::compare(
    const DecodedVector& decoded,
    vector_size_t index) const {
  VELOX_CHECK(begin_);

  ByteStream inStream;
  HashStringAllocator::prepareRead(begin_, inStream);
  return exec::ContainerRowSerde::instance().compare(
      inStream, decoded, index, {true, true, false});
}

void SingleValueAccumulator::destroy(HashStringAllocator* allocator) {
  if (begin_) {
    allocator->free(begin_);
  }
}

} // namespace facebook::velox::functions::aggregate
