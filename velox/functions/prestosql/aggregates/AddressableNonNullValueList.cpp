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
#include "velox/functions/prestosql/aggregates/AddressableNonNullValueList.h"
#include "velox/exec/ContainerRowSerde.h"

namespace facebook::velox::aggregate::prestosql {

HashStringAllocator::Position AddressableNonNullValueList::append(
    const DecodedVector& decoded,
    vector_size_t index,
    HashStringAllocator* allocator) {
  ByteStream stream(allocator);
  if (!firstHeader_) {
    // An array_agg or related begins with an allocation of 5 words and
    // 4 bytes for header. This is compact for small arrays (up to 5
    // bigints) and efficient if needs to be extended (stores 4 bigints
    // and a next pointer. This could be adaptive, with smaller initial
    // sizes for lots of small arrays.
    static constexpr int kInitialSize = 44;

    currentPosition_ = allocator->newWrite(stream, kInitialSize);
    firstHeader_ = currentPosition_.header;
  } else {
    allocator->extendWrite(currentPosition_, stream);
  }

  const auto position = currentPosition_;

  // Write hash.
  stream.appendOne(decoded.base()->hashValueAt(decoded.index(index)));
  // Write value.
  exec::ContainerRowSerde::instance().serialize(
      *decoded.base(), decoded.index(index), stream);

  ++size_;

  currentPosition_ = allocator->finishWrite(stream, 1024);
  return position;
}

namespace {

void prepareRead(
    HashStringAllocator::Position position,
    ByteStream& stream,
    bool skipHash) {
  auto header = position.header;
  auto seek = static_cast<int32_t>(position.position - header->begin());

  HashStringAllocator::prepareRead(header, stream);
  stream.seekp(seek);
  if (skipHash) {
    stream.skip(sizeof(uint64_t));
  }
}
} // namespace

// static
bool AddressableNonNullValueList::equalTo(
    HashStringAllocator::Position left,
    HashStringAllocator::Position right,
    const TypePtr& type) {
  ByteStream leftStream;
  prepareRead(left, leftStream, true /*skipHash*/);

  ByteStream rightStream;
  prepareRead(right, rightStream, true /*skipHash*/);

  CompareFlags compareFlags;
  compareFlags.equalsOnly = true;
  return exec::ContainerRowSerde::instance().compare(
             leftStream, rightStream, type.get(), compareFlags) == 0;
}

// static
uint64_t AddressableNonNullValueList::readHash(
    HashStringAllocator::Position position) {
  ByteStream stream;
  prepareRead(position, stream, false /*skipHash*/);

  return stream.read<uint64_t>();
}

// static
void AddressableNonNullValueList::read(
    HashStringAllocator::Position position,
    BaseVector& result,
    vector_size_t index) {
  ByteStream stream;
  prepareRead(position, stream, true /*skipHash*/);

  exec::ContainerRowSerde::instance().deserialize(stream, index, &result);
}

} // namespace facebook::velox::aggregate::prestosql
