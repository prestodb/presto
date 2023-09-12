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

#include "velox/experimental/wave/vector/WaveVector.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/experimental/wave/common/StringView.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::wave {

WaveVector::WaveVector(
    const TypePtr& type,
    GpuArena& arena,
    std::vector<std::unique_ptr<WaveVector>> children)
    : type_(type),
      kind_(type_->kind()),
      arena_(&arena),
      children_(std::move(children)) {
  switch (kind_) {
    case TypeKind::ROW:
      encoding_ = VectorEncoding::Simple::ROW;
      break;
    case TypeKind::ARRAY:
      encoding_ = VectorEncoding::Simple::ARRAY;
      break;
    case TypeKind::MAP:
      encoding_ = VectorEncoding::Simple::MAP;
      break;
    default:
      VELOX_UNSUPPORTED("{}", kind_);
  }
  if (!children_.empty()) {
    size_ = children_[0]->size();
  }
}

void WaveVector::resize(vector_size_t size, bool nullable) {
  if (size > size_) {
    int64_t bytes;
    if (type_->kind() == TypeKind::VARCHAR) {
      bytes = sizeof(StringView) * size;
    } else {
      bytes = type_->cppSizeInBytes() * size;
    }
    if (!values_ || bytes > values_->capacity()) {
      values_ = arena_->allocateBytes(bytes);
    }
    if (nullable) {
      if (!nulls_ || nulls_->capacity() < size) {
        nulls_ = arena_->allocateBytes(size);
      }
    } else {
      nulls_.reset();
    }
    size_ = size;
  }
}

void WaveVector::toOperand(Operand* operand) const {
  operand->size = size_;
  operand->nulls = nulls_ ? nulls_->as<uint8_t>() : nullptr;
  if (encoding_ == VectorEncoding::Simple::CONSTANT) {
    operand->indexMask = 0;
    operand->base = values_->as<uint64_t>();
    operand->indices = nullptr;
    return;
  }
  if (encoding_ == VectorEncoding::Simple::FLAT) {
    operand->indexMask = ~0;
    operand->base = values_->as<int64_t>();
    operand->indices = nullptr;
  } else {
    VELOX_UNSUPPORTED();
  }
}

void toBits(uint64_t* words, int32_t numBytes) {
  auto data = reinterpret_cast<uint8_t*>(words);
  auto zero = xsimd::broadcast<uint8_t>(0);
  for (auto i = 0; i < numBytes; i += xsimd::batch<uint8_t>::size) {
    auto flags = xsimd::batch<uint8_t>::load_unaligned(data) != zero;
    uint32_t bits = simd::toBitMask(flags);
    reinterpret_cast<uint32_t*>(words)[i / sizeof(flags)] = bits;
  }
}

template <TypeKind kind>
static VectorPtr toVeloxTyped(
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    const TypePtr& type,
    const WaveBufferPtr& values,
    const WaveBufferPtr& nulls) {
  using T = typename TypeTraits<kind>::NativeType;

  BufferPtr nullsView;
  if (nulls) {
    nullsView = WaveBufferView::create(nulls);
    toBits(
        const_cast<uint64_t*>(nullsView->as<uint64_t>()),
        nullsView->capacity());
  }
  BufferPtr valuesView;
  if (values) {
    valuesView = WaveBufferView::create(values);
  }

  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      std::move(nullsView),
      size,
      std::move(valuesView),
      std::vector<BufferPtr>());
}

VectorPtr WaveVector::toVelox(memory::MemoryPool* pool) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      toVeloxTyped, type_->kind(), size_, pool, type_, values_, nulls_);
}

} // namespace facebook::velox::wave
