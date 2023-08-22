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
#include "velox/vector/FlatVector.h"

namespace facebook::velox::wave {

void WaveVector::resize(vector_size_t size, bool nullable) {
  if (size > size_) {
    int64_t bytes = type_->cppSizeInBytes() * size;
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
  if (encoding_ == VectorEncoding::Simple::CONSTANT) {
    operand->indexMask = 0;
    if (nulls_) {
      operand->nulls = nulls_->as<uint8_t>();
    } else {
      operand->nulls = nullptr;
    }
    operand->base = values_->as<uint64_t>();
    return;
  }
  if (encoding_ == VectorEncoding::Simple::FLAT) {
    operand->indexMask = ~0;
    operand->base = values_->as<int64_t>();
    operand->base = nulls_ ? nulls_->as<uint8_t>() : nullptr;
    operand->indices = nullptr;
  } else {
    VELOX_UNSUPPORTED();
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
