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
#include <folly/Likely.h>

#include <velox/common/base/Exceptions.h>
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

template <typename T>
struct VectorReader;

template <typename T, typename B>
struct VectorWriter;

// Lightweight object that can be used as a proxy for array primitive elements.
// It is returned by ArrayProxy::operator()[].
template <typename T>
struct PrimitiveWriterProxy {
  using vector_t = typename TypeToFlatVector<T>::type;
  using element_t = typename CppToType<T>::NativeType;

  PrimitiveWriterProxy(vector_t* flatVector, vector_size_t index)
      : flatVector_(flatVector), index_(index) {}

  void operator=(std::nullopt_t) {
    flatVector_->setNull(index_, true);
  }

  void operator=(element_t value) {
    flatVector_->set(index_, value);
  }

  void operator=(const std::optional<element_t>& value) {
    if (value.has_value()) {
      flatVector_->set(index_, value.value());
    } else {
      flatVector_->setNull(index_, true);
    }
  }

 private:
  vector_t* flatVector_;
  vector_size_t index_;
};

// The object passed to the simple function interface that represent a single
// array entry.
// ## General Interface:
// - add_item()  : Add not null item and return proxy to the value to be
// written.
// - add_null()  : Add null item.
// - size()      : Return the size of the array.

// ## Special std::like interfaces when V is primitive:
// - resize(n)         : Resize to n, nullity not written.
// - operator[](index) : Returns PrimitiveWriterProxy which can be used to write
// value and nullity at index.
// - push_back(std::optional<v> value) : Increase size by 1, adding a value or
// null.
// - back() : Return PrimitiveWriterProxy for the last element in the array.
template <typename V>
class ArrayProxy {
  using child_writer_t = VectorWriter<V, void>;
  using element_t = typename child_writer_t::exec_out_t;

 public:
  static bool constexpr provide_std_interface =
      CppToType<V>::isPrimitiveType && !std::is_same<Varchar, V>::value;

  static bool constexpr requires_commit = !CppToType<V>::isPrimitiveType ||
      std::is_same<Varchar, V>::value || std::is_same<bool, V>::value;

  // Note: size is with respect to the current size of this array being written.
  void reserve(vector_size_t size) {
    auto currentSize = size + valuesOffset_;

    if (UNLIKELY(currentSize > elementsVectorCapacity_)) {
      while (currentSize > elementsVectorCapacity_) {
        elementsVectorCapacity_ = elementsVectorCapacity_ * 2;
      }
      childWriter_->ensureSize(elementsVectorCapacity_);
    }
  }

  // Add a new not null item to the array, increasing its size by 1.
  element_t& add_item() {
    resize(length_ + 1);
    auto index = valuesOffset_ + length_ - 1;

    if constexpr (!requires_commit) {
      VELOX_DCHECK(provide_std_interface);
      elementsVector_->setNull(index, false);
      return childWriter_->data_[index];
    } else {
      needCommit_ = true;
      childWriter_->setOffset(index);
      return childWriter_->current();
    }
  }

  // Add a new null item to the array.
  void add_null() {
    resize(length_ + 1);
    auto index = valuesOffset_ + length_ - 1;
    elementsVector_->setNull(index, true);
    // Note: no need to commit the null item.
  }

  // Should be called by the user (VectorWriter) when writing is done to commit
  // last item if needed.
  void finalize() {
    commitMostRecentChildItem();
    valuesOffset_ += length_;
    length_ = 0;
  }

  vector_size_t size() {
    return length_;
  }

  // Functions below provide an std::like interface, and are enabled only when
  // the array element is primitive that is not string or bool.

  // 'size' is with respect to the current size of the array being written.
  typename std::enable_if<provide_std_interface>::type resize(
      vector_size_t size) {
    commitMostRecentChildItem();
    reserve(size);
    length_ = size;
  }

  typename std::enable_if<provide_std_interface>::type push_back(
      element_t value) {
    resize(length_ + 1);
    back() = value;
  }

  typename std::enable_if<provide_std_interface>::type push_back(
      std::nullopt_t) {
    resize(length_ + 1);
    back() = std::nullopt;
  }

  typename std::enable_if<provide_std_interface>::type push_back(
      const std::optional<element_t>& value) {
    resize(length_ + 1);
    back() = value;
  }

  typename std::enable_if<provide_std_interface, PrimitiveWriterProxy<V>>::type
  operator[](vector_size_t index_) {
    return PrimitiveWriterProxy<V>{elementsVector_, valuesOffset_ + index_};
  }

  typename std::enable_if<provide_std_interface, PrimitiveWriterProxy<V>>::type
  back() {
    return PrimitiveWriterProxy<V>{
        elementsVector_, valuesOffset_ + length_ - 1};
  }

 private:
  // Make sure user do not use those.
  ArrayProxy<V>() = default;
  ArrayProxy<V>(const ArrayProxy<V>&) = default;
  ArrayProxy<V>& operator=(const ArrayProxy<V>&) = default;

  void commitMostRecentChildItem() {
    if constexpr (requires_commit) {
      if (needCommit_) {
        childWriter_->commit(true);
        needCommit_ = false;
      }
    }
  }

  void initialize(VectorWriter<ArrayProxyT<V>, void>* writer) {
    childWriter_ = &writer->childWriter_;
    elementsVector_ = childWriter_->vector_;
    childWriter_->ensureSize(1);
    elementsVectorCapacity_ = elementsVector_->size();
  }

  typename child_writer_t::vector_t* elementsVector_ = nullptr;

  // Pointer to child vector writer.
  child_writer_t* childWriter_ = nullptr;

  // Indicate if commit needs to be called on the childWriter_ before adding a
  // new element or when finalize is called.
  bool needCommit_ = false;

  // Length of the array.
  vector_size_t length_ = 0;

  // The offset within the child vector at which this array starts.
  vector_size_t valuesOffset_ = 0;

  // Tracks the capacity of elements vector.
  vector_size_t elementsVectorCapacity_ = 0;

  template <typename A, typename B>
  friend struct VectorWriter;

  template <typename T>
  friend class SimpleFunctionAdapter;
};
} // namespace facebook::velox::exec
