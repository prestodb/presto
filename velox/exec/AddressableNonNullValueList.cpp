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
#include "velox/exec/AddressableNonNullValueList.h"
#include "velox/exec/ContainerRowSerde.h"

namespace facebook::velox::aggregate::prestosql {

ByteOutputStream AddressableNonNullValueList::initStream(
    HashStringAllocator* allocator) {
  ByteOutputStream stream(allocator);
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

  return stream;
}

AddressableNonNullValueList::Entry AddressableNonNullValueList::append(
    const DecodedVector& decoded,
    vector_size_t index,
    HashStringAllocator* allocator) {
  return append(*decoded.base(), decoded.index(index), allocator);
}

AddressableNonNullValueList::Entry AddressableNonNullValueList::append(
    const BaseVector& vector,
    vector_size_t index,
    HashStringAllocator* allocator) {
  auto stream = initStream(allocator);

  const auto hash = vector.hashValueAt(index);

  const auto originalSize = stream.size();

  // Write value.
  exec::ContainerRowSerdeOptions options{};
  exec::ContainerRowSerde::serialize(vector, index, stream, options);
  ++size_;

  auto startAndFinish = allocator->finishWrite(stream, 1024);
  currentPosition_ = startAndFinish.second;

  const auto writtenSize = stream.size() - originalSize;

  return {startAndFinish.first, writtenSize, hash};
}

HashStringAllocator::Position AddressableNonNullValueList::appendSerialized(
    const StringView& value,
    HashStringAllocator* allocator) {
  auto stream = initStream(allocator);

  const auto originalSize = stream.size();
  stream.appendStringView(value);
  ++size_;

  auto startAndFinish = allocator->finishWrite(stream, 1024);
  currentPosition_ = startAndFinish.second;
  VELOX_CHECK_EQ(stream.size() - originalSize, value.size());
  return {startAndFinish.first};
}

namespace {

ByteInputStream prepareRead(const AddressableNonNullValueList::Entry& entry) {
  auto header = entry.offset.header;
  auto seek = entry.offset.position - header->begin();

  auto stream = HashStringAllocator::prepareRead(header, entry.size + seek);
  stream.seekp(seek);
  return stream;
}
} // namespace

// static
bool AddressableNonNullValueList::equalTo(
    const Entry& left,
    const Entry& right,
    const TypePtr& type) {
  if (left.hash != right.hash) {
    return false;
  }

  auto leftStream = prepareRead(left);
  auto rightStream = prepareRead(right);

  CompareFlags compareFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);
  return exec::ContainerRowSerde::compare(
             leftStream, rightStream, type.get(), compareFlags) == 0;
}

// static
void AddressableNonNullValueList::read(
    const Entry& position,
    BaseVector& result,
    vector_size_t index) {
  auto stream = prepareRead(position);
  exec::ContainerRowSerde::deserialize(stream, index, &result);
}

// static
void AddressableNonNullValueList::readSerialized(
    const Entry& position,
    char* dest) {
  auto stream = prepareRead(position);
  stream.readBytes(dest, position.size);
}

} // namespace facebook::velox::aggregate::prestosql
