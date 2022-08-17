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

#include <iomanip>

#include <boost/intrusive_ptr.hpp>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Range.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/memory/Memory.h"

namespace facebook {
namespace velox {

class AlignedBuffer;

// Represents vector payloads, like arrays of numbers or strings or
// associated null flags. Buffers are reference counted and must be
// held by BufferPtr. Buffers can either own their memory or can be
// views on externally managed memory, see BufferView. A buffer that
// owns its memory (AlignedBuffer) allocates memory from a MemoryPool,
// which allows tracking and custom memory management. A buffer that
// owns its memory can be in a mutable state if there is a single
// reference to it. Copying the BufferPtr is possible only if the
// Buffer is in the immutable state.
//
// The typical use case has an operator reusing the same buffer for
// consecutive batches of output if the buffer is singly referenced. A
// new Buffer should be made if there are other references to the last
// used Buffer.
//
// NOTE: References to sizes represent number of bytes.
class Buffer {
 public:
  // Even though some buffer elements like StringView or folly::Range have a
  // non-trivial constructor, they don't really allocate any memory and we take
  // liberty of not calling it occasionally - we treat them as an almost POD
  // type. Thus the conditions are: trivial destructor (no resources to release)
  // and trivially copyable (so memcpy works)
  template <typename T>
  static inline constexpr bool is_pod_like_v =
      std::is_trivially_destructible_v<T>&& std::is_trivially_copyable_v<T>;

  virtual ~Buffer(){};

  void addRef() {
    referenceCount_.fetch_add(1);
  }

  int refCount() const {
    return referenceCount_;
  }

  void release() {
    if (referenceCount_.fetch_sub(1) == 1) {
      releaseResources();
      if (pool_) {
        freeToPool();
      } else {
        delete this;
      }
    }
  }

  template <typename T>
  const T* as() const {
    // We can't check actual types, but we can sanity-check POD/non-POD
    // conversion. `void` is special as it's used in type-erased contexts
    VELOX_DCHECK((std::is_same_v<T, void>) || podType_ == is_pod_like_v<T>);
    return reinterpret_cast<const T*>(data_);
  }

  template <typename T>
  Range<T> asRange() {
    return Range<T>(as<T>(), 0, size() / sizeof(T));
  }

  template <typename T>
  T* asMutable() const {
    VELOX_CHECK(mutable_);
    // We can't check actual types, but we can sanity-check POD/non-POD
    // conversion. `void` is special as it's used in type-erased contexts
    VELOX_DCHECK((std::is_same_v<T, void>) || podType_ == is_pod_like_v<T>);
    return reinterpret_cast<T*>(data_);
  }

  template <typename T>
  MutableRange<T> asMutableRange() {
    return MutableRange<T>(asMutable<T>(), 0, size() / sizeof(T));
  }

  size_t size() const {
    return size_;
  }

  // Resizes the "initialized" part of the buffer. For non-POD types, it calls
  // default constructors or destructors if the size expands or shrinks
  // respectively.
  // TODO: `resize` is probably a better name for this method
  virtual void setSize(size_t size) {
    VELOX_CHECK(mutable_);
    VELOX_CHECK_LE(size, capacity_);
    size_ = size;
    checkEndGuard();
  }

  uint64_t capacity() const {
    return capacity_;
  }

  bool unique() const {
    return referenceCount_ == 1;
  }

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  bool isMutable() const {
    return mutable_;
  }

  virtual bool isView() const {
    return false;
  }

  virtual void setIsMutable(bool isMutable) {
    VELOX_CHECK(
        !isMutable || referenceCount_ == 1,
        "A multiply referenced Buffer should not be set to mutable");
    mutable_ = isMutable;
  }

  friend std::ostream& operator<<(std::ostream& os, const Buffer& buffer) {
    std::ios_base::fmtflags f(os.flags());

    os << std::boolalpha << "{ size: " << buffer.size()
       << ", capacity: " << buffer.capacity()
       << ", refCount: " << buffer.refCount() << ", unique: " << buffer.unique()
       << ", isMutable: " << buffer.isMutable()
       << ", isView: " << buffer.isView() << ", data: [ ";

    for (size_t i = 0; i < buffer.capacity(); ++i) {
      // We want to print the entire buffer but give some indicator when we're
      // printing beyond size
      if (i == buffer.size()) {
        os << "|| <-- size | remaining allocated --> || ";
      }

      // the individual chars need to be in int32_t to display correctly.
      os << std::hex << std::setw(2) << std::setfill('0')
         << static_cast<int32_t>(buffer.data_[i]) << " ";
    }
    os << "] }";

    // We are assuming that no exception happened above on using this, but the
    // above should not.
    os.flags(f);

    return os;
  }

 protected:
  // Writes a magic word at 'capacity_'. No-op for a BufferView. The actual
  // logic is inside a separate virtual function, allowing override by derived
  // classes. Because of the virtual function dispatch, it's unlikely the
  // compiler can inline it, so we make it only called in the debug build.
  void setEndGuard() {
#ifndef NDEBUG
    setEndGuardImpl();
#endif
  }

  virtual void setEndGuardImpl() {}

  void checkEndGuard() const {
#ifndef NDEBUG
    checkEndGuardImpl();
#endif
  }

  // Checks the magic number at capacity() to detect overrun. No-op
  // for a BufferView. An overrun is qualitatively a
  // process-terminating memory corruption. We do not kill the process
  // here though but rather throw an error so that the the message and
  // stack propagate to the library user. This may also happen in a
  // ~AlignedBuffer, which will leak the memory but since the process
  // is anyway already compromized this is not an issue.
  virtual void checkEndGuardImpl() const {}

  void setCapacity(size_t capacity) {
    capacity_ = capacity;
    setEndGuard();
  }
  // If 'this' is allocated from a pool, frees the memory, including
  // all padding. This must be overridden by Buffer classes that are
  // allocated from a pool and does not apply to BufferViews.
  virtual void freeToPool() {
    VELOX_FAIL("Buffer::freeToPool() must be overridden by concrete buffers");
  }

  virtual void copyFrom(const Buffer* other, size_t bytes) {
    VELOX_CHECK(mutable_);
    VELOX_CHECK_GE(capacity_, bytes);
    VELOX_CHECK(podType_);
    memcpy(data_, other->data_, bytes);
  }

  virtual void releaseResources() {
    // Overridden in descendants for freeing the data with non-trivial
    // destructors Note that Buffer's destructor may not be called in case of
    // pools, so we have to have a separate method.
  }

  Buffer(
      velox::memory::MemoryPool* pool,
      uint8_t* data,
      size_t capacity,
      bool podType)
      : pool_(pool),
        data_(data),
        capacity_(capacity),
        referenceCount_(0),
        podType_(podType) {}

  velox::memory::MemoryPool* const pool_;
  uint8_t* const data_;
  uint64_t size_ = 0;
  uint64_t capacity_ = 0;
  std::atomic<int32_t> referenceCount_;
  bool mutable_ = true;
  bool podType_ = true;
  // Pad to 64 bytes. If using as int32_t[], guarantee that value at index -1 ==
  // -1.
  uint64_t padding_[2] = {static_cast<uint64_t>(-1), static_cast<uint64_t>(-1)};
  // Needs to use setCapacity() from static method reallocate().
  friend class AlignedBuffer;
};

static_assert(
    sizeof(Buffer) == 64,
    "Buffer is assumed to be 64 bytes to guarantee alignment");

template <>
inline Range<bool> Buffer::asRange<bool>() {
  return Range<bool>(as<uint64_t>(), 0, size() * 8);
}

template <>
inline MutableRange<bool> Buffer::asMutableRange<bool>() {
  return MutableRange<bool>(asMutable<uint64_t>(), 0, size() * 8);
}

using BufferPtr = boost::intrusive_ptr<Buffer>;

static inline void intrusive_ptr_add_ref(Buffer* buffer) {
  buffer->addRef();
}

static inline void intrusive_ptr_release(Buffer* buffer) {
  buffer->release();
}

template <typename T>
class NonPODAlignedBuffer;

class AlignedBuffer : public Buffer {
 public:
  static constexpr int16_t kAlignment = 64;
  // Magic number used to guard against writing past 'capacity_'
  static constexpr uint64_t kEndGuard = 0xbadaddbadadddeadUL;
  // Declare size here and static_assert in
  // constructor. sizeof(AlignedBuffer) is not defined here.
  static constexpr int32_t kSizeofAlignedBuffer = 64;
  static constexpr int32_t kPaddedSize = kSizeofAlignedBuffer + simd::kPadding;

  ~AlignedBuffer() {
    // This may throw, which is expected to signal an error to the
    // user. This is better for distributed debugging than killing the
    // process. In concept this indicates the possibility of memory
    // corruption and the process state should be considered
    // compromized.
    checkEndGuard();
  }

  // It's almost like partial specialization, but we redirect all POD types to
  // the same non-templated class
  template <typename T>
  using ImplClass = typename std::conditional<
      is_pod_like_v<T>,
      AlignedBuffer,
      NonPODAlignedBuffer<T>>::type;

  /**
   * Allocates enough memory to store numElements of type T.  May
   * allocate more memory than strictly necessary. Guarantees that
   * simd::kPadding bytes past capacity() are addressable and asserts that
   * these do not get overrun.
   */
  template <typename T>
  static BufferPtr allocate(
      size_t numElements,
      velox::memory::MemoryPool* pool,
      const std::optional<T>& initValue = std::nullopt) {
    size_t size = checkedMultiply(numElements, sizeof(T));
    size_t preferredSize =
        pool->getPreferredSize(checkedPlus<size_t>(size, kPaddedSize));
    void* memory = pool->allocate(preferredSize);
    auto* buffer = new (memory) ImplClass<T>(pool, preferredSize - kPaddedSize);
    // set size explicitly instead of setSize because `fillNewMemory` already
    // called the constructors
    buffer->size_ = size;
    BufferPtr result(buffer);
    buffer->template fillNewMemory<T>(0, size, initValue);
    return result;
  }

  // Changes the capacity of '*buffer'. The buffer may grow/shrink in
  // place or may change addresses. The content is copied up to the
  // old size() or the new size, whichever is smaller. If the buffer grows, the
  // new elements are initialized to 'initValue' if it's provided
  template <typename T>
  static void reallocate(
      BufferPtr* buffer,
      size_t numElements,
      const std::optional<T>& initValue = std::nullopt) {
    auto size = checkedMultiply(numElements, sizeof(T));
    Buffer* old = buffer->get();
    VELOX_CHECK(old, "Buffer doesn't exist in reallocate");
    old->checkEndGuard();
    VELOX_DCHECK(
        dynamic_cast<ImplClass<T>*>(old) != nullptr,
        "Reallocate tries to change the type");
    auto oldSize = old->size();

    if (size > oldSize && size < old->capacity() && old->unique()) {
      VELOX_CHECK(old->mutable_);
      reinterpret_cast<ImplClass<T>*>(old)->template fillNewMemory<T>(
          oldSize, size, initValue);
      // set size explicitly instead of setSize because `fillNewMemory` already
      // called the constructors
      old->size_ = size;
      return;
    }
    velox::memory::MemoryPool* pool = old->pool();
    if (!is_pod_like_v<T>) {
      // We always take this code path for non-POD types because
      // pool->reallocate below would move memory around without calling move
      // constructor.
      // Calling allocate<T> unnecessarily calls constructor and operator= for
      // non-POD types and can be optimized with just copy constructor. Leaving
      // it for the future.
      auto newBuffer = allocate<T>(numElements, pool, initValue);
      newBuffer->copyFrom(old, std::min(size, old->size()));
      // set size explicitly instead of setSize because `allocate` already
      // called the constructors
      newBuffer->size_ = size;
      *buffer = std::move(newBuffer);
      return;
    }
    if (!old->unique()) {
      auto newBuffer = allocate<T>(numElements, pool);
      newBuffer->copyFrom(old, std::min(size, old->size()));
      reinterpret_cast<AlignedBuffer*>(newBuffer.get())
          ->template fillNewMemory<T>(old->size(), size, initValue);
      newBuffer->size_ = size;
      *buffer = std::move(newBuffer);
      return;
    }
    auto oldCapacity = checkedPlus<size_t>(old->capacity(), kPaddedSize);
    auto preferredSize =
        pool->getPreferredSize(checkedPlus<size_t>(size, kPaddedSize));
    // Make the buffer no longer owned by '*buffer' because reallocate
    // may free the old buffer. Reassigning the new buffer to
    // '*buffer' would be a double free.
    buffer->detach();
    // Decrement the reference count.  No need to check, we just
    // checked old->unique().
    old->referenceCount_.fetch_sub(1);
    void* newPtr;
    try {
      newPtr = pool->reallocate(old, oldCapacity, preferredSize);
    } catch (const std::exception&) {
      *buffer = old;
      throw;
    }
    if (newPtr == reinterpret_cast<void*>(old)) {
      // The pointer did not change. Put the old pointer back in the
      // smart pointer and adjust capacity.
      *buffer = old;
      (*buffer)->setCapacity(preferredSize - kPaddedSize);
      (*buffer)->setSize(size);
      reinterpret_cast<AlignedBuffer*>(buffer->get())
          ->fillNewMemory<T>(oldSize, size, initValue);
      return;
    }
    auto newBuffer =
        new (newPtr) AlignedBuffer(pool, preferredSize - kPaddedSize);
    newBuffer->setSize(size);
    newBuffer->fillNewMemory<T>(oldSize, size, initValue);
    *buffer = newBuffer;
  }

  // Appends bytes starting at 'items' for a length of 'sizeof(T) *
  // numItems'. The data is written into '*buffer' starting at offset
  // size(), after which size() is incremented with the number of
  // bytes copied. The buffer may grow and b copied to a new
  // address. Returns the address of the first copied byte in the
  // buffer.
  template <typename T>
  static T* appendTo(BufferPtr* buffer, const T* items, int32_t numItems) {
    static_assert(
        is_pod_like_v<T>, "Support for non POD types not implemented yet");
    size_t bytes = sizeof(T) * numItems;
    size_t size = (*buffer)->size();
    size_t capacity = (*buffer)->capacity();
    size_t newSize = checkedPlus(size, bytes);
    if (newSize > capacity) {
      reallocate<char>(
          buffer, std::max(checkedMultiply<size_t>(2, capacity), newSize));
    }
    (*buffer)->setSize(newSize);
    auto startOfCopy = (*buffer)->asMutable<uint8_t>() + size;
    memcpy(startOfCopy, items, bytes);
    return reinterpret_cast<T*>(startOfCopy);
  }

  static BufferPtr copy(
      velox::memory::MemoryPool* pool,
      const BufferPtr& bufferPtr) {
    if (bufferPtr == nullptr) {
      return nullptr;
    }

    VELOX_CHECK(
        bufferPtr->podType_, "Support for non POD types not implemented yet");

    // The reason we use uint8_t is because mutableNulls()->size() will return
    // in byte count. We also don't bother initializing since copyFrom will be
    // overwriting anyway.
    auto newBuffer = AlignedBuffer::allocate<uint8_t>(bufferPtr->size(), pool);

    newBuffer->copyFrom(bufferPtr.get(), newBuffer->size());

    return newBuffer;
  }

 protected:
  AlignedBuffer(velox::memory::MemoryPool* pool, size_t capacity)
      : Buffer(
            pool,
            reinterpret_cast<uint8_t*>(this) + sizeof(*this),
            capacity,
            true /*podType*/) {
    static_assert(sizeof(*this) == kAlignment);
    static_assert(sizeof(*this) == kSizeofAlignedBuffer);
    setEndGuard();
  }

  // Fills raw memory with the given value. For non-POD types it calls the copy
  // constructor, so it can't be used for already initialized memory regions
  template <typename RawT>
  void fillNewMemory(
      size_t oldBytes,
      size_t newBytes,
      const std::optional<RawT>& initValue) {
    VELOX_CHECK_LE(newBytes, capacity());
    if (newBytes <= oldBytes) {
      return;
    }
    if (initValue) {
      VELOX_DCHECK(newBytes % sizeof(RawT) == 0);
      VELOX_DCHECK(oldBytes % sizeof(RawT) == 0);
      auto data = asMutable<RawT>();
      std::fill(
          data + (oldBytes / sizeof(RawT)),
          data + (newBytes / sizeof(RawT)),
          *initValue);
    } else {
#ifndef NDEBUG
      // Initialize with unlikely constant value in debug mode to make
      // uninitialized status visible in debugger.
      memset(asMutable<char>() + oldBytes, 0xa1, capacity() - oldBytes);
#endif
    }
  }

 protected:
  void setEndGuardImpl() override {
    *reinterpret_cast<uint64_t*>(data_ + capacity_) = kEndGuard;
  }

  void checkEndGuardImpl() const override {
    if (*reinterpret_cast<uint64_t*>(data_ + capacity_) != kEndGuard) {
      VELOX_FAIL("Write past Buffer capacity() {}", capacity_);
    }
  }

  void freeToPool() override {
    pool_->free(this, checkedPlus<size_t>(kPaddedSize, capacity_));
  }
};

template <>
inline BufferPtr AlignedBuffer::allocate<bool>(
    size_t numElements,
    velox::memory::MemoryPool* pool,
    const std::optional<bool>& initValue) {
  return allocate<char>(
      bits::nbytes(numElements),
      pool,
      initValue ? std::optional<char>(*initValue ? -1 : 0) : std::nullopt);
}

template <>
inline void AlignedBuffer::reallocate<bool>(
    BufferPtr* buffer,
    size_t numElements,
    const std::optional<bool>& initValue) {
  reallocate<char>(
      buffer,
      bits::nbytes(numElements),
      initValue ? std::optional<char>(*initValue ? -1 : 0) : std::nullopt);
}

template <typename T>
class NonPODAlignedBuffer : public Buffer {
 public:
  static_assert(
      !is_pod_like_v<T>,
      "It makes sense to use only with non trivial types");

  void setSize(size_t size) override {
    size_t old = size_;
    VELOX_CHECK_EQ(old % sizeof(T), 0);
    VELOX_CHECK_EQ(size % sizeof(T), 0);
    Buffer::setSize(size);
    int oldN = old / sizeof(T);
    int newN = size / sizeof(T);
    auto data = asMutable<T>();
    // At most one of these for loops runs: if expanding - call default
    // constructors, if shrinking - destructors.
    for (int i = oldN; i < newN; ++i) {
      new (data + i) T();
    }
    for (int i = newN; i < oldN; ++i) {
      data[i].~T();
    }
  }

 protected:
  NonPODAlignedBuffer(velox::memory::MemoryPool* pool, size_t capacity)
      : Buffer(
            pool,
            reinterpret_cast<uint8_t*>(this) + sizeof(*this),
            capacity,
            false /*podType*/) {
    static_assert(sizeof(*this) == AlignedBuffer::kAlignment);
    static_assert(sizeof(*this) == sizeof(AlignedBuffer));
  }

  void releaseResources() override {
    VELOX_CHECK_EQ(size_ % sizeof(T), 0);
    size_t numValues = size_ / sizeof(T);
    // we can't use asMutable because it checks isMutable and we wan't to
    // destroy regardless
    T* ptr = reinterpret_cast<T*>(data_);
    for (int i = 0; i < numValues; ++i) {
      ptr[i].~T();
    }
  }

  void copyFrom(const Buffer* other, size_t bytes) override {
    VELOX_CHECK(mutable_);
    VELOX_CHECK_GE(size_, bytes);
    VELOX_DCHECK(
        dynamic_cast<const NonPODAlignedBuffer<T>*>(other) != nullptr,
        "Types don't match");
    VELOX_CHECK_EQ(bytes % sizeof(T), 0);
    size_t numValues = bytes / sizeof(T);
    const T* from = other->as<T>();
    T* to = asMutable<T>();
    for (int i = 0; i < numValues; ++i) {
      to[i] = from[i];
    }
  }

  template <typename RawT>
  void fillNewMemory(
      size_t oldBytes,
      size_t newBytes,
      const std::optional<RawT>& initValue) {
    static_assert(std::is_same_v<T, RawT>);
    VELOX_CHECK_LE(newBytes, capacity());
    VELOX_CHECK_GE(
        newBytes,
        oldBytes,
        "It probably indicates that destructors won't be called for non-POD types");
    // always initialize (i.e. call constructor)
    VELOX_CHECK_EQ(newBytes % sizeof(T), 0);
    VELOX_CHECK_EQ(oldBytes % sizeof(T), 0);
    int oldNum = oldBytes / sizeof(T);
    int newNum = newBytes / sizeof(T);
    auto data = asMutable<T>();
    for (int i = oldNum; i < newNum; ++i) {
      if (initValue) {
        new (data + i) T(*initValue);
      } else {
        new (data + i) T();
      }
    }
  }

  void freeToPool() override {
    pool_->free(
        this, checkedPlus<size_t>(AlignedBuffer::kPaddedSize, capacity_));
  }

  // Needs to use this class from static methods of AlignedBuffer
  friend class AlignedBuffer;
};

// Immutable view over externally managed memory. When the last reference is
// destroyed, release() is called on 'releaser'.
template <typename Releaser>
class BufferView : public Buffer {
 public:
  static BufferPtr create(
      const uint8_t* data,
      size_t size,
      Releaser releaser,
      bool podType = true) {
    BufferView<Releaser>* view = new BufferView(data, size, releaser, podType);
    BufferPtr result(view);
    return result;
  }

  ~BufferView() override {
    releaser_.release();
  }

  bool isView() const override {
    return true;
  }
  void setIsMutable(bool isMutable) override {
    VELOX_CHECK(!isMutable, "A BufferView cannot be set to mutable");
  }

 private:
  BufferView(const uint8_t* data, size_t size, Releaser releaser, bool podType)
      // A BufferView must be created over the data held by a cache
      // pin, which is typically const. The Buffer enforces const-ness
      // when returning the pointer. We cast away the const here to
      // avoid a separate code path for const and non-const Buffer
      // payloads.
      : Buffer(nullptr, const_cast<uint8_t*>(data), size, podType),
        releaser_(releaser) {
    mutable_ = false;
    size_ = size;
    capacity_ = size;
    releaser_.addRef();
  }

  Releaser const releaser_;
};

} // namespace velox
} // namespace facebook
