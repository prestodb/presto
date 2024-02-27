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
#include "velox/functions/lib/aggregates/ValueSet.h"
#include "velox/exec/ContainerRowSerde.h"

namespace facebook::velox::aggregate {

void ValueSet::write(
    const BaseVector& vector,
    vector_size_t index,
    HashStringAllocator::Position& position) const {
  ByteOutputStream stream(allocator_);
  if (position.header == nullptr) {
    position = allocator_->newWrite(stream);
  } else {
    allocator_->extendWrite(position, stream);
  }

  static const exec::ContainerRowSerdeOptions options{};
  exec::ContainerRowSerde::serialize(vector, index, stream, options);
  allocator_->finishWrite(stream, 0);
}

StringView ValueSet::write(const StringView& value) const {
  if (value.isInline()) {
    return value;
  }

  const auto size = value.size();

  auto* header = allocator_->allocate(size);
  auto* start = header->begin();

  memcpy(start, value.data(), size);
  return StringView(start, size);
}

void ValueSet::read(
    BaseVector* vector,
    vector_size_t index,
    const HashStringAllocator::Header* header) const {
  VELOX_CHECK_NOT_NULL(header);

  auto stream = HashStringAllocator::prepareRead(header);
  exec::ContainerRowSerde::deserialize(stream, index, vector);
}

void ValueSet::free(HashStringAllocator::Header* header) const {
  VELOX_CHECK_NOT_NULL(header);
  allocator_->free(header);
}

void ValueSet::free(const StringView& value) const {
  if (!value.isInline()) {
    auto* header = HashStringAllocator::headerOf(value.data());
    allocator_->free(header);
  }
}

} // namespace facebook::velox::aggregate
