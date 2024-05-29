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
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::wave {

template <TypeKind Kind>
static int32_t kindSize() {
  return sizeof(typename KindToFlatVector<Kind>::WrapperType);
}

int32_t waveTypeKindSize(WaveTypeKind waveKind) {
  TypeKind kind = static_cast<TypeKind>(waveKind);
  if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
    // Wave StringView is 8, not 16 bytes.
    return sizeof(StringView);
  }
  if (kind == TypeKind::UNKNOWN) {
    return sizeof(UnknownValue);
  }

  return VELOX_DYNAMIC_TYPE_DISPATCH(kindSize, kind);
}

WaveVector::WaveVector(
    const TypePtr& type,
    GpuArena& arena,
    std::vector<std::unique_ptr<WaveVector>> children,
    bool notNull)
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

void WaveVector::resize(
    vector_size_t size,
    bool nullable,
    WaveBufferPtr* backing,
    int64_t* backingOffset) {
  auto capacity = values_ ? values_->capacity() : 0;
  size_ = size;
  int32_t bytesNeeded = backingSize(type_, size, nullable);
  if (bytesNeeded > capacity) {
    if (backing) {
      values_ = WaveBufferView<WaveBufferPtr>::create(
          (*backing)->as<uint8_t>() + *backingOffset, bytesNeeded, *backing);
      *backingOffset += bytesNeeded;
    } else {
      values_ = arena_->allocateBytes(bytesNeeded);
    }
  }
  if (nullable) {
    nulls_ = values_->as<uint8_t>() + bytesNeeded - size;
  } else {
    nulls_ = nullptr;
  }
}

void WaveVector::toOperand(Operand* operand) const {
  operand->size = size_;
  operand->nulls = nulls_;
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

namespace {
class NoReleaser {
 public:
  void addRef() const {};
  void release() const {};
};

template <TypeKind kind>
static VectorPtr toVeloxTyped(
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    const TypePtr& type,
    const WaveBufferPtr& values,
    const uint8_t* nulls) {
  using T = typename TypeTraits<kind>::NativeType;

  BufferPtr nullsView;
  if (nulls) {
    nullsView = BufferView<NoReleaser>::create(nulls, size, NoReleaser());
    toBits(
        const_cast<uint64_t*>(nullsView->as<uint64_t>()),
        nullsView->capacity());
  }
  BufferPtr valuesView;
  if (values) {
    valuesView = VeloxWaveBufferView::create(values);
  }

  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      std::move(nullsView),
      size,
      std::move(valuesView),
      std::vector<BufferPtr>());
}

bool isDenselyFilled(const BlockStatus* status, int32_t numBlocks) {
  for (int32_t i = 0; i < numBlocks - 1; ++i) {
    if (status[i].numRows != kBlockSize) {
      return false;
    }
  }
  return true;
}
} // namespace

int32_t statusNumRows(const BlockStatus* status, int32_t numBlocks) {
  int32_t numRows = 0;
  for (auto i = 0; i < numBlocks; ++i) {
    numRows += status[i].numRows;
  }
  return numRows;
}

// static
int32_t WaveVector::alignment(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(void*);
    default:
      return type->cppSizeInBytes();
  }
}

//    static
int64_t
WaveVector::backingSize(const TypePtr& type, int32_t size, bool nullable) {
  int64_t bytes;
  if (type->kind() == TypeKind::VARCHAR) {
    bytes = sizeof(StringView) * size;
  } else {
    bytes = type->cppSizeInBytes() * size;
  }
  return bits::roundUp(bytes, sizeof(void*)) + (nullable ? size : 0);
}

VectorPtr WaveVector::toVelox(
    memory::MemoryPool* pool,
    int32_t numBlocks,
    const BlockStatus* status,
    const Operand* operand) {
  auto base = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      toVeloxTyped, type_->kind(), size_, pool, type_, values_, nulls_);
  if (!status || !operand) {
    return base;
  }

  // Translate the BlockStatus and indices in Operand to a host side dictionary
  // wrap.
  int maxRow = std::min<int32_t>(size_, numBlocks * kBlockSize);
  numBlocks = bits::roundUp(maxRow, kBlockSize) / kBlockSize;
  int numActive = statusNumRows(status, numBlocks);
  auto operandIndices = operand->indices;
  if (!operandIndices) {
    // Vector sizes are >= active in status because they are allocated before
    // the row count in status becomes known.
    VELOX_CHECK_LE(
        numActive,
        size_,
        "If there is no indirection in Operand, vector size must be <= BlockStatus");
    // If all blocks except last are filled we return base without wrap.
    if (isDenselyFilled(status, numBlocks)) {
      return base;
    }
  }
  auto indices = AlignedBuffer::allocate<vector_size_t>(numActive, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  int32_t fill = 0;
  for (auto block = 0; block < numBlocks; ++block) {
    auto blockIndices = operandIndices ? operandIndices[block] : nullptr;
    if (!blockIndices) {
      for (auto i = 0; i < status[block].numRows; ++i) {
        rawIndices[fill++] = block * kBlockSize + i;
      }
    } else {
      memcpy(
          rawIndices + fill,
          blockIndices,
          status[block].numRows * sizeof(int32_t));
      fill += status[block].numRows;
    }
  }
  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, numActive, base);
}

} // namespace facebook::velox::wave
