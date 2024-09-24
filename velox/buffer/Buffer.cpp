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

#include "velox/buffer/Buffer.h"

namespace facebook::velox {

namespace {
struct BufferReleaser {
  explicit BufferReleaser(const BufferPtr& parent) : parent_(parent) {}
  void addRef() const {}
  void release() const {}

 private:
  BufferPtr parent_;
};
} // namespace

BufferPtr Buffer::sliceBufferZeroCopy(
    size_t typeSize,
    bool podType,
    const BufferPtr& buffer,
    size_t offset,
    size_t length) {
  auto bytesOffset = checkedMultiply(offset, typeSize);
  auto bytesLength = checkedMultiply(length, typeSize);
  VELOX_CHECK_LE(
      bytesOffset,
      buffer->size(),
      "Offset must be less than or equal to {}.",
      buffer->size() / typeSize);
  VELOX_CHECK_LE(
      bytesLength,
      buffer->size() - bytesOffset,
      "Length must be less than or equal to {}.",
      buffer->size() / typeSize - offset);
  // Cannot use `Buffer::as<uint8_t>()` here because Buffer::podType_ is false
  // when type is OPAQUE.
  auto data =
      reinterpret_cast<const uint8_t*>(buffer->as<void>()) + bytesOffset;
  return BufferView<BufferReleaser>::create(
      data, bytesLength, BufferReleaser(buffer), podType);
}

template <>
BufferPtr Buffer::slice<bool>(
    const BufferPtr& buffer,
    size_t offset,
    size_t length,
    memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(buffer, "Buffer must not be null.");

  if (offset % 8 == 0) {
    return sliceBufferZeroCopy(
        1, true, buffer, bits::nbytes(offset), bits::nbytes(length));
  }
  VELOX_CHECK_NOT_NULL(pool, "Pool must not be null.");
  auto ans = AlignedBuffer::allocate<bool>(length, pool);
  bits::copyBits(
      buffer->as<uint64_t>(), offset, ans->asMutable<uint64_t>(), 0, length);
  return ans;
}
} // namespace facebook::velox
