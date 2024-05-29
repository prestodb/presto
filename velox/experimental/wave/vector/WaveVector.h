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

#pragma once
#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/vector/Operand.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::wave {

class Loader {
 public:
  virtual ~Loader() = default;

  virtual void load(WaveBufferPtr indexBuffer, int32_t begin, int32_t end) = 0;

  /// Notifies 'this' that a load should load, in ddition to the requested rows,
  /// all rows above the last position given in the load indices.
  void loadTailOnLoad() {
    loadTail_ = true;
  }

 protected:
  bool loadTail_{false};
};

int32_t waveTypeKindSize(WaveTypeKind kind);

/// Represents a vector of device side intermediate results. Vector is
/// a host side only structure, the WaveBufferPtrs own the device
/// memory. Unlike Velox vectors, these are statically owned by Wave
/// operators and represent their last computed output. Buffers may be
/// shared between these, as with Velox vectors. the values in buffers
/// become well defined on return of the kernel that computes these.
class WaveVector {
 public:
  static std::unique_ptr<WaveVector> create(
      const TypePtr& type,
      GpuArena& arena) {
    return std::make_unique<WaveVector>(type, arena);
  }

  // Constructs a vector. Resize can be used to create buffers for a given size.
  WaveVector(const TypePtr& type, GpuArena& arena, bool notNull = false)
      : type_(type), kind_(type_->kind()), arena_(&arena), notNull_(notNull) {}

  WaveVector(
      const TypePtr& type,
      GpuArena& arena,
      std::vector<std::unique_ptr<WaveVector>> children,
      bool notNull = false);

  const TypePtr& type() const {
    return type_;
  }

  vector_size_t size() const {
    return size_;
  }

  /// Sets the size to 'size'. Allocates the backing memory from
  /// 'arena_'. If 'backing' is non-nullptr, uses '*backing' for
  /// backing store, starting at offset *backingOffset'. Returns the
  /// offset of the first unused byte in '*backingOffset'. Leaves
  /// contents uninitialized in all cases.
  void resize(
      vector_size_t size,
      bool nullable = true,
      WaveBufferPtr* backing = nullptr,
      int64_t* backingOffset = nullptr);

  /// Returns the needed alignment for backing memory.
  static int32_t alignment(const TypePtr& type);

  /// Returns the size in bytes for 'size' elements of 'type', including nulls
  /// if 'nullable' is true. Does not include string buffers.
  static int64_t backingSize(const TypePtr& type, int32_t size, bool nullable);

  bool mayHaveNulls() const {
    return nulls_ != nullptr;
  }

  // Frees all allocated buffers. resize() can be used to populate the buffers
  // with a selected size.
  void clear();

  /// Starts computation for a kLazy state vector.
  void load();

  WaveVector& childAt(int32_t index) {
    return *children_[index];
  }

  template <typename T>
  T* values() {
    return values_->as<T>();
  }

  uint8_t* nulls() {
    return nulls_;
  }

  /// Returns a Velox vector giving a view on device side data. The device
  /// buffers stay live while referenced by Velox. If there is a selection,
  /// numBlocks is the number of kBlockSize blocks the vector was allocated for,
  /// BlockStatus gives the row counts per block and Operand gives the
  /// dictionary indices representing the selection.
  VectorPtr toVelox(
      memory::MemoryPool* pool,
      int32_t numBlocks = -1,
      const BlockStatus* status = nullptr,
      const Operand* operand = nullptr);

  /// Sets 'operand' to point to the buffers of 'this'.
  void toOperand(Operand* operand) const;

  std::string toString() const;

 private:
  // Type of the content.
  TypePtr type_;
  TypeKind kind_;

  // Encoding. FLAT, CONSTANT, DICTIONARY, ROW, ARRAY, MAP are possible
  // values.
  VectorEncoding::Simple encoding_{VectorEncoding::Simple::FLAT};

  // The arena for allocating buffers.
  GpuArena* arena_;

  std::unique_ptr<Loader> loader_;

  vector_size_t size_{0};

  // Values array, cast to pod type or StringView. If there are nulls, the null
  // flags are in this buffer after the values, starting at 'null_'
  WaveBufferPtr values_;

  // Nulls, points to the tail of 'values'. nullptr if no nulls.
  uint8_t* nulls_{nullptr};

  // If dictionary or if wrapped in a selection, vector of indices into
  // 'values'.
  WaveBufferPtr indices_;

  // Lengths and offsets for array/map elements.
  WaveBufferPtr lengths_;
  WaveBufferPtr offsets_;

  // Members of a array/map/struct vector.
  std::vector<std::unique_ptr<WaveVector>> children_;
  bool notNull_{false};
};

using WaveVectorPtr = std::unique_ptr<WaveVector>;

struct WaveReleaser {
  WaveReleaser(WaveBufferPtr buffer) : buffer(std::move(buffer)) {}

  void addRef() const {}

  void release() const {
    buffer.reset();
  }

  // BufferView inlines the releaser as const, hence this must have const
  // methods but must mutate the controlled object.
  mutable WaveBufferPtr buffer;
};

// A BufferView for velox::BaseVector for a view on unified memory.
class VeloxWaveBufferView : public BufferView<WaveReleaser> {
 public:
  /// Takes an additional reference to buffer. 'offset' and 'size'
  /// allow sharing one allocation for many views. This is done when many
  /// vectors have to be moved as a unit between device and host.
  static BufferPtr
  create(WaveBufferPtr buffer, int64_t offset = 0, int32_t size = -1) {
    return BufferView<WaveReleaser>::create(
        buffer->as<uint8_t>() + offset,
        size == -1 ? buffer->capacity() - offset : size,
        WaveReleaser(buffer));
  }
};

} // namespace facebook::velox::wave
