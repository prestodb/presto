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
#include <iterator>
#include <optional>
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::exec {
template <typename T, typename U>
struct VectorReader;

// Pointer wrapper used to convert r-values to valid return type for operator->.
template <typename T>
class PointerWrapper {
 public:
  explicit PointerWrapper(T&& t) : t_(t) {}

  const T* operator->() const {
    return &t_;
  }

  T* operator->() {
    return &t_;
  }

 private:
  T t_;
};

// Base class for ArrayView::Iterator and MapView::Iterator. The missing parts
// to be implemented by deriving classes are: operator*() and operator->().
template <typename T>
class IndexBasedIterator {
 public:
  using Iterator = IndexBasedIterator<T>;
  using iterator_category = std::input_iterator_tag;
  using value_type = T;
  using difference_type = int;
  using pointer = PointerWrapper<value_type>;
  using reference = T;

  explicit IndexBasedIterator<value_type>(vector_size_t index)
      : index_(index) {}

  bool operator!=(const Iterator& rhs) const {
    return index_ != rhs.index_;
  }

  bool operator==(const Iterator& rhs) const {
    return index_ == rhs.index_;
  }

  bool operator<(const Iterator& rhs) const {
    return index_ < rhs.index_;
  }

  // Implement post increment.
  Iterator operator++(int) {
    Iterator old = *this;
    ++*this;
    return old;
  }

  // Implement pre increment.
  Iterator& operator++() {
    index_++;
    return *this;
  }

 protected:
  vector_size_t index_;
};

// Implements an iterator for values stored in the reader T
// that skip nulls and provides direct access to the value.
template <typename T>
class SkipNullsIterator;

template <typename T>
class SkipNullsIterator {
  using Iterator = SkipNullsIterator<T>;
  using iterator_category = std::input_iterator_tag;
  using value_type = typename T::exec_in_t;
  using difference_type = int;
  using pointer = PointerWrapper<value_type>;
  using reference = T;

 public:
  SkipNullsIterator<T>(
      const T* reader,
      vector_size_t index,
      vector_size_t lasIndex)
      : reader_(reader), index_(index), endIndex_(lasIndex) {}

  // Given an element, return an iterator to the first not-null element starting
  // from the element itself.
  static Iterator initialize(
      const T* reader_,
      vector_size_t startIndex,
      vector_size_t endIndex) {
    auto it = Iterator{reader_, startIndex, endIndex};

    // The container is empty.
    if (startIndex >= endIndex) {
      return it;
    }

    if (reader_->isSet(startIndex)) {
      return it;
    }

    // Move to next not null.
    it++;
    return it;
  }

  value_type operator*() const {
    // Always return a copy, its guaranteed to be cheap object.
    return reader_->operator[](index_);
  }

  PointerWrapper<value_type> operator->() const {
    return PointerWrapper(reader_->operator[](index_));
  }

  bool operator<(const Iterator& rhs) const {
    return index_ < rhs.index_;
  }

  bool operator!=(const Iterator& rhs) const {
    return index_ != rhs.index_;
  }

  bool operator==(const Iterator& rhs) const {
    return index_ == rhs.index_;
  }

  // Implement post increment.
  Iterator operator++(int) {
    Iterator old = *this;
    ++*this;
    return old;
  }

  // Implement pre increment.
  Iterator& operator++() {
    index_++;
    while (index_ != endIndex_) {
      if (reader_->isSet(index_)) {
        break;
      }
      index_++;
    }
    return *this;
  }

 private:
  const T* reader_;
  vector_size_t index_;
  // First index outside the container.
  vector_size_t endIndex_;
};

// TODO: evaluate wrapping primitives with lazy access using benchmarks
template <typename T>
using VectorValueAccessor = typename T::exec_in_t;

// Given a vectorReader T, this class represents a lazy access optional wrapper
// around an element in the vectorReader with interface similar to
// std::optional<T::exec_in_t>. This is used to represent elements of ArrayView
// and values of MapView. VectorOptionalValueAccessor can be compared with and
// assigned to std::optional.
template <typename T>
class VectorOptionalValueAccessor {
 public:
  using element_t = typename T::exec_in_t;

  explicit operator bool() const {
    return has_value();
  }

  // Enable to be assigned to std::optional<element_t>.
  operator std::optional<element_t>() const {
    if (!has_value()) {
      return std::nullopt;
    }
    return {value()};
  }

  // Disable all other implicit casts to avoid odd behaviors.
  template <typename B>
  operator B() const = delete;

  bool operator==(const VectorOptionalValueAccessor& other) const {
    if (other.has_value() != has_value()) {
      return false;
    }

    if (has_value()) {
      return value() == other.value();
    }
    // Both are nulls.
    return true;
  }

  bool operator!=(const VectorOptionalValueAccessor& other) const {
    return !(*this == other);
  }

  bool has_value() const {
    return reader_->isSet(index_);
  }

  element_t value() const {
    return (*reader_)[index_];
  }

  element_t value_or(const element_t& defaultValue) const {
    return has_value() ? value() : defaultValue;
  }

  element_t operator*() const {
    return value();
  }

  PointerWrapper<element_t> operator->() const {
    return PointerWrapper(value());
  }

 private:
  VectorOptionalValueAccessor<T>(const T* reader, vector_size_t index)
      : reader_(reader), index_(index) {}
  const T* reader_;
  // Index of element within the reader.
  vector_size_t index_;

  template <typename V>
  friend class ArrayView;

  template <typename K, typename V>
  friend class MapView;
};

template <typename T, typename U>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator==(
    const std::optional<T>& lhs,
    const VectorOptionalValueAccessor<U>& rhs) {
  if (lhs.has_value() != rhs.has_value()) {
    return false;
  }

  if (lhs.has_value()) {
    return lhs.value() == rhs.value();
  }
  // Both are nulls.
  return true;
}

template <typename U, typename T>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator==(
    const VectorOptionalValueAccessor<U>& lhs,
    const std::optional<T>& rhs) {
  return rhs == lhs;
}

template <typename T, typename U>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator!=(
    const std::optional<T>& lhs,
    const VectorOptionalValueAccessor<U>& rhs) {
  return !(lhs == rhs);
}

template <typename U, typename T>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator!=(
    const VectorOptionalValueAccessor<U>& lhs,
    const std::optional<T>& rhs) {
  return !(lhs == rhs);
}

template <typename T>
bool operator==(std::nullopt_t, const VectorOptionalValueAccessor<T>& rhs) {
  return !rhs.has_value();
}

template <typename T>
bool operator!=(std::nullopt_t, const VectorOptionalValueAccessor<T>& rhs) {
  return rhs.has_value();
}

template <typename T>
bool operator==(const VectorOptionalValueAccessor<T>& lhs, std::nullopt_t) {
  return !lhs.has_value();
}

template <typename T>
bool operator!=(const VectorOptionalValueAccessor<T>& lhs, std::nullopt_t) {
  return lhs.has_value();
}

// Allow comparing VectorOptionalValueAccessor<T> with T::exec_t.
template <typename T, typename U>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator==(const T& lhs, const VectorOptionalValueAccessor<U>& rhs) {
  return rhs.has_value() && (*rhs == lhs);
}

template <typename U, typename T>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator==(const VectorOptionalValueAccessor<U>& lhs, const T& rhs) {
  return rhs == lhs;
}

template <typename T, typename U>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator!=(const T& lhs, const VectorOptionalValueAccessor<U>& rhs) {
  return !(lhs == rhs);
}

template <typename U, typename T>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator!=(const VectorOptionalValueAccessor<U>& lhs, const T& rhs) {
  return !(lhs == rhs);
}

// Represents an array of elements with an interface similar to std::vector.
template <typename V>
class ArrayView {
  using reader_t = VectorReader<V, void>;
  using element_t = typename reader_t::exec_in_t;

 public:
  ArrayView(const reader_t* reader, vector_size_t offset, vector_size_t size)
      : reader_(reader), offset_(offset), size_(size) {}

  // The previous doLoad protocol creates a value and then assigns to it.
  // TODO: this should deprecated once  we deprecate the doLoad protocol.
  ArrayView() : reader_(nullptr), offset_(0), size_(0) {}

  using Element = VectorOptionalValueAccessor<reader_t>;

  class Iterator : public IndexBasedIterator<Element> {
   public:
    Iterator(const reader_t* reader, vector_size_t index)
        : IndexBasedIterator<Element>(index), reader_(reader) {}

    PointerWrapper<Element> operator->() const {
      return PointerWrapper(Element{reader_, this->index_});
    }

    Element operator*() const {
      return Element{reader_, this->index_};
    }

   private:
    const reader_t* reader_;
  };

  Iterator begin() const {
    return Iterator{reader_, offset_};
  }

  Iterator end() const {
    return Iterator{reader_, offset_ + size_};
  }

  struct SkipNullsContainer {
    using Iterator = SkipNullsIterator<reader_t>;
    explicit SkipNullsContainer(const ArrayView* array_) : array_(array_) {}

    Iterator begin() {
      auto endIndex = array_->offset_ + array_->size_;
      return Iterator::initialize(array_->reader_, array_->offset_, endIndex);
    }

    Iterator end() {
      auto endIndex = array_->offset_ + array_->size_;
      return Iterator{array_->reader_, endIndex, endIndex};
    }

   private:
    const ArrayView* array_;
  };

  // Returns true if any of the arrayViews in the vector might have null
  // element.
  bool mayHaveNulls() const {
    return reader_->mayHaveNulls();
  }

  Element operator[](vector_size_t index) const {
    return Element{reader_, index + offset_};
  }

  Element at(vector_size_t index) const {
    return (*this)[index];
  }

  vector_size_t size() const {
    return size_;
  }

  SkipNullsContainer skipNulls() {
    return SkipNullsContainer{this};
  }

 private:
  const reader_t* reader_;
  vector_size_t offset_;
  vector_size_t size_;
};

// This class is used to represent map inputs in simple functions with an
// interface similar to std::map.
template <typename K, typename V>
class MapView {
 public:
  using key_reader_t = VectorReader<K, void>;
  using value_reader_t = VectorReader<V, void>;
  using key_element_t = typename key_reader_t::exec_in_t;

  MapView(
      const key_reader_t* keyReader,
      const value_reader_t* valueReader,
      vector_size_t offset,
      vector_size_t size)
      : keyReader_(keyReader),
        valueReader_(valueReader),
        offset_(offset),
        size_(size) {}

  MapView()
      : keyReader_(nullptr), valueReader_(nullptr), offset_(0), size_(0) {}

  using ValueAccessor = VectorOptionalValueAccessor<value_reader_t>;
  using KeyAccessor = VectorValueAccessor<key_reader_t>;

  class Element {
   public:
    Element(
        const key_reader_t* keyReader,
        const value_reader_t* valueReader,
        vector_size_t index)
        : first((*keyReader)[index]),
          second(valueReader, index),
          keyReader_(keyReader),
          index_(index) {}
    const KeyAccessor first;
    const ValueAccessor second;

    bool operator==(const Element& other) const {
      return first == other.first && second == other.second;
    }

    // T is pair like object.
    // TODO: compare is not defined for view types yet
    template <typename T>
    bool operator==(const T& other) const {
      return first == other.first && second == other.second;
    }

    template <typename T>
    bool operator!=(const T& other) const {
      return !(*this == other);
    }

   private:
    const key_reader_t* keyReader_;
    vector_size_t index_;
  };

  class Iterator : public IndexBasedIterator<Element> {
   public:
    Iterator(
        const key_reader_t* keyReader,
        const value_reader_t* valueReader,
        vector_size_t index)
        : IndexBasedIterator<Element>(index),
          keyReader_(keyReader),
          valueReader_(valueReader) {}

    PointerWrapper<Element> operator->() const {
      return PointerWrapper(Element{keyReader_, valueReader_, this->index_});
    }

    Element operator*() const {
      return Element{keyReader_, valueReader_, this->index_};
    }

   private:
    const key_reader_t* keyReader_;
    const value_reader_t* valueReader_;
  };

  Iterator begin() const {
    return Iterator{keyReader_, valueReader_, offset_};
  }

  Iterator end() const {
    return Iterator{keyReader_, valueReader_, size_ + offset_};
  }

  const Element operator[](vector_size_t index) const {
    return Element{keyReader_, valueReader_, index + offset_};
  }

  vector_size_t size() const {
    return size_;
  }

  Iterator find(const key_element_t& key) const {
    return std::find_if(begin(), end(), [&key](const auto& current) {
      return current.first == key;
    });
  }

  ValueAccessor at(const key_element_t& key) const {
    auto it = find(key);
    VELOX_USER_CHECK(it != end(), "accessed key is not found in the map");
    return it->second;
  }

 private:
  const key_reader_t* keyReader_;
  const value_reader_t* valueReader_;
  vector_size_t offset_;
  vector_size_t size_;
};
} // namespace facebook::velox::exec
