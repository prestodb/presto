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

#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::functions {

// DynamicFlatVector provides a subset of std::vector's interface and dynamic
// allocation on top of a FlatVector<T>. It's intended to be efficient, but
// will always be less efficient than pre-allocating a FlatVector of an
// appropriate size and using it directly.
//
// This class was built for ArrayBuilder, but is logically independent.
//
// TODO:
//   - Expand coverage of the std::vector API (e.g operator[], resize()).
//   - Smooth buffer management.
//   - Support null elements.
template <typename Tag>
class DynamicFlatVector {
 public:
  using T = typename CppToType<Tag>::NativeType;

  DynamicFlatVector(const DynamicFlatVector&) = delete;
  DynamicFlatVector& operator=(const DynamicFlatVector&) = delete;

  // Vector starts empty (size = 0) but with given capacity.
  DynamicFlatVector(vector_size_t initialCapacity, memory::MemoryPool* pool)
      : pool_(pool), initialCapacity_(initialCapacity) {
    reserve(initialCapacity);
  }

  void push_back(const T& t) {
    emplace_back(t);
  }

  // This is really just for StringView.
  template <typename... Arg>
  void emplace_back(Arg&&... arg) {
    if (UNLIKELY(size_ >= capacity_)) {
      // Double up to 10k elements.
      reserve(capacity_ < 10000 ? 2 * capacity_ : capacity_ + capacity_ / 5);
    }
    if constexpr (std::is_same_v<Tag, bool>) {
      bits::setBit(data_, size_++, arg...);
    } else {
      new (&data_[size_++]) T(std::forward<Arg>(arg)...);
    }
  }

  vector_size_t size() const {
    return size_;
  }

  // Overwrite the string buffers associated with the elements array.
  void setStringBuffers(std::vector<BufferPtr> buffers) {
    static_assert(std::is_same_v<T, StringView>, "Only valid for strings.");
    vector_->setStringBuffers(std::move(buffers));
  }

  // Overwrite the string buffers associated with the elements array.
  void setStringBuffers(const BaseVector* source) {
    static_assert(std::is_same_v<T, StringView>, "Only valid for strings.");
    vector_->acquireSharedStringBuffers(source);
  }

  std::shared_ptr<FlatVector<T>> consumeFlatVector() && {
    vector_->resize(size_);
    data_ = nullptr;
    capacity_ = 0;
    size_ = 0;
    return std::move(vector_);
  }

 private:
  void reserve(vector_size_t newCapacity) {
    if (newCapacity < initialCapacity_) {
      newCapacity = initialCapacity_;
    }
    if (UNLIKELY(!vector_)) {
      auto vector =
          BaseVector::create(CppToType<Tag>::create(), newCapacity, pool_);
      vector_ = std::dynamic_pointer_cast<FlatVector<T>>(vector);
      VELOX_CHECK_NOT_NULL(vector_.get());
    } else {
      vector_->resize(newCapacity);
    }
    capacity_ = newCapacity;
    data_ = vector_->template mutableRawValues<
        std::remove_pointer_t<decltype(data_)>>();
  }

  velox::memory::MemoryPool* const pool_;
  const vector_size_t initialCapacity_;
  vector_size_t capacity_ = 0;
  vector_size_t size_ = 0;
  std::shared_ptr<FlatVector<T>> vector_;
  std::conditional_t<std::is_same_v<Tag, bool>, uint64_t, T>* data_;
};

} // namespace facebook::velox::functions
