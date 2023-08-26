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
  WaveVector(const TypePtr& type, GpuArena& arena)
      : type_(type), kind_(type_->kind()), arena_(&arena) {}

  WaveVector(
      const TypePtr& type,
      GpuArena& arena,
      std::vector<std::unique_ptr<WaveVector>> children);

  const TypePtr& type() const {
    return type_;
  }

  vector_size_t size() const {
    return size_;
  }

  void resize(vector_size_t sie, bool nullable = true);

  bool mayHaveNulls() const {
    return nulls_ != nullptr;
  }

  // Makes sure there is space for nulls. Initial value is undefined.
  void ensureNulls();

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
    if (nulls_) {
      return nulls_->as<uint8_t>();
    }
    return nullptr;
  }

  /// Returns a Velox vector giving a view on device side data. The device
  /// buffers stay live while referenced by Velox.
  VectorPtr toVelox(memory::MemoryPool* pool);

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

  // Values array, cast to pod type or StringView
  WaveBufferPtr values_;

  // Nulls buffer, nullptr if no nulls.
  WaveBufferPtr nulls_;

  // If dictionary or if wrapped in a selection, vector of indices into
  // 'values'.
  WaveBufferPtr indices_;

  // Thread block level sizes. For each kBlockSize values, contains
  // one int16_t that indicates how many of 'values' or 'indices' have
  // a value.
  WaveBufferPtr blockSizes_;
  // Thread block level pointers inside 'indices_'. the ith entry is nullptr
  // if the ith thread block has no row number mapping (all rows pass or none
  // pass).
  WaveBufferPtr blockIndices_;

  // Lengths and offsets for array/map elements.
  WaveBufferPtr lengths_;
  WaveBufferPtr offsets_;

  // Members of a array/map/struct vector.
  std::vector<std::unique_ptr<WaveVector>> children_;
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
class WaveBufferView : public BufferView<WaveReleaser> {
 public:
  static BufferPtr create(WaveBufferPtr buffer) {
    return BufferView<WaveReleaser>::create(
        buffer->as<uint8_t>(), buffer->capacity(), WaveReleaser(buffer));
  }
};

} // namespace facebook::velox::wave
