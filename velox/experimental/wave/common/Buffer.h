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
#include <boost/intrusive_ptr.hpp>
#include <atomic>
#include <cstdint>
#include <string>

namespace facebook::velox::wave {

class GpuArena;

/// Each area of device or universal memory in Wave has a unique
/// host side control block with reference and pin counts. If
/// unpinned, the memory is not currently accessed by a kernel and
/// can be moved. When moving, it suffices to update the pointer in
/// Buffer. These are managed via WaveBufferPtr. When the reference count goes
/// to 0, the memory is returned to 'arena' and the Buffer is added to the
/// Buffer free list.
class Buffer {
 public:
  virtual ~Buffer() = default;

  template <typename T>
  T* as() {
    return reinterpret_cast<T*>(ptr_);
  }

  size_t capacity() const {
    return capacity_;
  }

  size_t size() const {
    return size_;
  }

  void setSize(size_t newSize) {
    assert(newSize <= capacity_);
    size_ = newSize;
  }

  void pin() {
    ++pinCount_;
  }

  bool unpin() {
    assert(0 < pinCount_);
    return --pinCount_ == 0;
  }

  bool isPinned() const {
    return 0 != pinCount_;
  }

  void addRef() {
    referenceCount_.fetch_add(1);
  }

  int refCount() const {
    return referenceCount_;
  }

  virtual void release();

  const std::string& debugInfo() const {
    return debugInfo_;
  }

  void setDebugInfo(std::string info) {
    debugInfo_ = std::move(info);
  }

  /// Checks consistency of magic numbers. Throws on error.
  void check() const;

  std::string toString() const;

 protected:
  static constexpr int64_t kMagic = 0xe0be0be0be0be0b;

  int64_t* magicPtr() const {
    return reinterpret_cast<int64_t*>(
        reinterpret_cast<char*>(ptr_) + capacity_);
  }

  void setMagic();

  // Number of WaveBufferPtrs referencing 'this'.
  std::atomic<int32_t> referenceCount_{0};

  // Number of pins. Incremented when passing to a kernel, decremented
  // when the kernel returns. If 0 pins, o compute is proceeding and
  // the memory owned by 'this' can be moved.
  std::atomic<int32_t> pinCount_{0};

  // Pointer to device/universal memory. If 'referenceCount_' is 0, this is the
  // host pointer to the next  free Buffer in 'arena_'.
  void* ptr_{nullptr};

  // Byte size of memory held by 'ptr_'. Undefined if 'referenceCount_' is 0.
  int64_t capacity_{0};

  // Number of bytes used. Must be <= 'capacity_'.
  int64_t size_{0};

  // The containeing arena.
  GpuArena* arena_{nullptr};

  std::string debugInfo_;
  friend class GpuArena;
};

using WaveBufferPtr = boost::intrusive_ptr<Buffer>;

static inline void intrusive_ptr_add_ref(Buffer* buffer) {
  buffer->addRef();
}

static inline void intrusive_ptr_release(Buffer* buffer) {
  buffer->release();
}

template <typename Releaser>
class WaveBufferView : public Buffer {
 public:
  static WaveBufferPtr create(uint8_t* data, size_t size, Releaser releaser) {
    WaveBufferView<Releaser>* view = new WaveBufferView(data, size, releaser);
    WaveBufferPtr result(view);
    return result;
  }

  ~WaveBufferView() override = default;

  void release() override {
    if (referenceCount_.fetch_sub(1) == 1) {
      // Destructs releaser, which should release the hold on the underlying
      // buffer.
      delete this;
    }
  }

 private:
  WaveBufferView(uint8_t* data, size_t size, Releaser releaser)
      : Buffer(), releaser_(releaser) {
    ptr_ = data;
    size_ = size;
    capacity_ = size;
  }

  Releaser const releaser_;
};

} // namespace facebook::velox::wave
