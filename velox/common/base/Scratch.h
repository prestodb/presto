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

#include "velox/common/base/RawVector.h"

/// A utility for reusable scoped temporary scratch areas.
namespace facebook::velox {

/// A collection of temporary reusable scratch vectors. The vectors are accessed
/// via the ScratchPtr scoped lease. The vectors are padded so that their last
/// element can be written at full SIMD width, as with raw_vector.
class Scratch {
 public:
  using Item = raw_vector<char>;

  Scratch() = default;
  Scratch(const Scratch& other) = delete;

  ~Scratch() {
    reserve(0);
    ::free(items_);
    items_ = nullptr;
    capacity_ = 0;
    fill_ = 0;
  }
  void operator=(const Scratch& other) = delete;

  /// Returns the next reusable scratch vector or makes a new one.
  Item get() {
    if (fill_ == 0) {
      return Item();
    }
    auto temp = std::move(items_[fill_ - 1]);
    --fill_;
    retainedSize_ -= temp.capacity();
    return temp;
  }

  void release(Item&& item) {
    retainedSize_ += item.capacity();
    if (fill_ == capacity_) {
      reserve(std::max(16, 2 * capacity_));
    }
    items_[fill_++] = std::move(item);
  }

  void trim() {
    reserve(0);
    retainedSize_ = 0;
  }

  size_t retainedSize() {
    return retainedSize_;
  }

 private:
  void reserve(int32_t newCapacity) {
    VELOX_CHECK_LE(fill_, capacity_);
    // Delete the items above the new capacity.
    for (auto i = newCapacity; i < fill_; ++i) {
      std::destroy_at(&items_[i]);
    }
    if (newCapacity > capacity_) {
      Item* newItems =
          reinterpret_cast<Item*>(::malloc(sizeof(Item) * newCapacity));
      if (fill_ > 0) {
        ::memcpy(newItems, items_, fill_ * sizeof(Item));
      }
      ::memset(newItems + fill_, 0, (newCapacity - fill_) * sizeof(Item));
      ::free(items_);
      items_ = newItems;
      capacity_ = newCapacity;
    }
    fill_ = std::min(fill_, newCapacity);
  }

  Item* items_{nullptr};
  int32_t fill_{0};
  int32_t capacity_{0};
  // The total size held. If too large from outlier use cases, 'this' should be
  // trimmed.
  int64_t retainedSize_{0};
};

/// A scoped lease for a scratch area of T. For scratch areas <=
/// 'inlineSize' the scratch area is inlined, typically on stack, and
/// no allocation will ever take place. The inline storage is padded
/// with a trailer of simd::kPadding bytes to allow writing at full
/// SIMD width at the end of the area.
template <typename T, int32_t inlineSize = 0>
class ScratchPtr {
 public:
  ScratchPtr(Scratch& scratch) : scratch_(&scratch) {}

  ScratchPtr(const ScratchPtr& other) = delete;
  ScratchPtr(ScratchPtr&& other) = delete;

  inline ~ScratchPtr() {
    if (data_.data() != nullptr) {
      scratch_->release(std::move(data_));
    }
  }

  void operator=(ScratchPtr&& other) = delete;
  void operator=(const ScratchPtr& other) = delete;

  /// Returns a writable pointer to at least 'size' uninitialized
  /// elements of T. The last element is followed by simd::kPadding
  /// bytes to allow a full width SIMD store for any element. This may
  /// be called once per lifetime.
  T* get(int32_t size) {
    VELOX_CHECK_NULL(ptr_);
    size_ = size;
    if (size <= inlineSize) {
      ptr_ = inline_;
      return ptr_;
    }
    data_ = scratch_->get();
    data_.resize(size * sizeof(T));
    ptr_ = reinterpret_cast<T*>(data_.data());
    return ptr_;
  }

  /// Returns the pointer returned by a previous get(int32_t).
  T* get() const {
    VELOX_DCHECK_NOT_NULL(ptr_);
    return ptr_;
  }

  /// Returns the size of the previous get(int32_t).
  int32_t size() const {
    return size_;
  }

 private:
  Scratch* const scratch_{nullptr};

  raw_vector<char> data_;
  T* ptr_{nullptr};
  int32_t size_{0};
  T inline_[inlineSize];
  char padding_[inlineSize == 0 ? 0 : simd::kPadding];
};

} // namespace facebook::velox
