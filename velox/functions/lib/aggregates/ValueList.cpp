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
#include "velox/functions/lib/aggregates/ValueList.h"
#include "velox/exec/ContainerRowSerde.h"

namespace facebook::velox::aggregate {
void ValueList::prepareAppend(HashStringAllocator* allocator) {
  if (!nullsBegin_) {
    nullsBegin_ = allocator->allocate(HashStringAllocator::kMinAlloc);
    nullsCurrent_ = {nullsBegin_, nullsBegin_->begin()};
  }

  if (!dataBegin_) {
    dataBegin_ = allocator->allocate(kInitialSize);
    dataCurrent_ = {dataBegin_, dataBegin_->begin()};
  }

  if (size_ && size_ % 64 == 0) {
    writeLastNulls(allocator);
    lastNulls_ = 0;
    // Make sure there is space for another word of null flags without
    // allocating more space. This is needed for finalize to finish in
    // constant space.
    allocator->ensureAvailable(sizeof(int64_t), nullsCurrent_);
  }
}

void ValueList::writeLastNulls(HashStringAllocator* allocator) {
  ByteOutputStream stream(allocator);
  if (nullsBegin_) {
    allocator->extendWrite(nullsCurrent_, stream);
  } else {
    auto position = allocator->newWrite(stream, kInitialSize);
    nullsBegin_ = position.header;
  }
  stream.appendOne(lastNulls_);
  nullsCurrent_ = allocator->finishWrite(stream, kInitialSize).second;
}

void ValueList::appendNull(HashStringAllocator* allocator) {
  prepareAppend(allocator);
  lastNulls_ |= 1UL << (size_ % 64);
  ++size_;
}

void ValueList::appendNonNull(
    const BaseVector& values,
    vector_size_t index,
    HashStringAllocator* allocator) {
  prepareAppend(allocator);
  ByteOutputStream stream(allocator);
  allocator->extendWrite(dataCurrent_, stream);
  // The stream may have a tail of a previous write.
  const auto initialSize = stream.size();
  static const exec::ContainerRowSerdeOptions options{};
  exec::ContainerRowSerde::serialize(values, index, stream, options);
  ++size_;
  bytes_ += stream.size() - initialSize;

  // Leave space up to half the size appended so far, at least 24 but no more
  // than 1024.
  dataCurrent_ =
      allocator->finishWrite(stream, std::clamp(bytes_ / 2, 24, 1024)).second;
}

void ValueList::appendValue(
    const DecodedVector& decoded,
    vector_size_t index,
    HashStringAllocator* allocator) {
  auto& base = *decoded.base();
  if (decoded.isNullAt(index)) {
    appendNull(allocator);
  } else {
    appendNonNull(base, decoded.index(index), allocator);
  }
}

void ValueList::appendRange(
    const VectorPtr& vector,
    vector_size_t offset,
    vector_size_t size,
    HashStringAllocator* allocator) {
  for (auto index = offset; index < offset + size; ++index) {
    if (vector->isNullAt(index)) {
      appendNull(allocator);
    } else {
      appendNonNull(*vector, index, allocator);
    }
  }
}

ValueListReader::ValueListReader(ValueList& values)
    : size_{values.size()},
      lastNullsStart_{size_ % 64 == 0 ? size_ - 64 : size_ - size_ % 64},
      lastNulls_{values.lastNulls()},
      dataStream_{HashStringAllocator::prepareRead(values.dataBegin())},
      nullsStream_{HashStringAllocator::prepareRead(values.nullsBegin())} {}

bool ValueListReader::next(BaseVector& output, vector_size_t outputIndex) {
  if (pos_ == lastNullsStart_) {
    nulls_ = lastNulls_;
  } else if (pos_ % 64 == 0) {
    nulls_ = nullsStream_.read<uint64_t>();
  }

  if (nulls_ & (1UL << (pos_ % 64))) {
    output.setNull(outputIndex, true);
  } else {
    exec::ContainerRowSerde::deserialize(dataStream_, outputIndex, &output);
  }

  pos_++;
  return pos_ < size_;
}
} // namespace facebook::velox::aggregate
