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
#include <optional>
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::exec {
template <typename T, typename U>
struct VectorReader;

// Implements an iterator for T that moves by calling incrementIndex(). T must
// implement index() and incrementIndex(). Two iterators from the same
// "container" points to the same element if they have the same index.
template <typename T>
class IndexBasedIterator
    : public std::iterator<std::input_iterator_tag, T, size_t> {
 public:
  using Iterator = IndexBasedIterator<T>;

  explicit IndexBasedIterator<T>(const T& element) : element_(element) {}

  bool operator!=(const Iterator& rhs) const {
    return element_.index() != rhs.element_.index();
  }

  bool operator==(const Iterator& rhs) const {
    return element_.index() == rhs.element_.index();
  }

  const T& operator*() const {
    return element_;
  }

  const T* operator->() const {
    return &element_;
  }

  bool operator<(const Iterator& rhs) const {
    return element_.index() < rhs.element_.index();
  }

  // Implement post increment.
  Iterator operator++(int) {
    Iterator old = *this;
    ++*this;
    return old;
  }

  // Implement pre increment.
  Iterator& operator++() {
    element_.incrementIndex();
    return *this;
  }

 protected:
  T element_;
};

// This class represents a lazy access wrapper around the T members at a
// specific index.
template <typename T>
struct VectorValueAccessor {
  using element_t = typename T::exec_in_t;
  operator element_t() const {
    return (*reader_)[index_];
  }

  bool operator==(const VectorValueAccessor<T>& other) const {
    return element_t(other) == element_t(*this);
  }

  vector_size_t index() const {
    return index_;
  }

  void setIndex(vector_size_t index) const {
    index_ = index;
  }

 private:
  VectorValueAccessor(const T* reader, vector_size_t index)
      : reader_(reader), index_(index) {}

  const T* reader_;
  mutable vector_size_t index_;
  template <typename K, typename V>
  friend class MapView;
};

// Given a vectorReader T, this class represents a lazy access optional wrapper
// around an element in the vectorReader with interface similar to
// std::optional<T::exec_in_t>. This is used to represent elements of ArrayView
// and values of MapView. VectorOptionalValueAccessor can be compared with and
// assigned to std::optional.
template <typename T>
class VectorOptionalValueAccessor final {
 public:
  using element_t = typename T::exec_in_t;

  operator bool() const {
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

  void incrementIndex() const {
    index_++;
  }

  vector_size_t index() const {
    return index_;
  }

  void setIndex(vector_size_t index) const {
    index_ = index;
  }

 private:
  VectorOptionalValueAccessor<T>(const T* reader, vector_size_t index)
      : reader_(reader), index_(index) {}

  const T* reader_;
  // Index of element within the reader.
  mutable vector_size_t index_;

  template <typename V>
  friend class ArrayView;

  template <typename K, typename V>
  friend class MapView;
};

// Allow comparing VectorOptionalValueAccessor with std::optional.
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
  // TODO: this should deprecated once we deprecate the doLoad protocol.
  ArrayView() : reader_(nullptr), offset_(0), size_(0) {}

  using Element = VectorOptionalValueAccessor<reader_t>;

  using Iterator = IndexBasedIterator<Element>;

  Iterator begin() const {
    return Iterator{Element{reader_, offset_}};
  }

  Iterator end() const {
    return Iterator{Element{reader_, offset_ + size_}};
  }

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

  size_t size() const {
    return size_;
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

  class Element;
  using ValueAccessor = VectorOptionalValueAccessor<value_reader_t>;

  // Lazy access wrapper around the key.
  using KeyAccessor = VectorValueAccessor<key_reader_t>;

  class Element {
   public:
    Element(
        const key_reader_t* keyReader,
        const value_reader_t* valueReader,
        vector_size_t index)
        : first(keyReader, index), second(valueReader, index), index_(index) {}
    const KeyAccessor first;
    const ValueAccessor second;

    bool operator==(const Element& other) const {
      return first == other.first && second == other.second;
    }

    // T is pair like object.
    template <typename T>
    bool operator==(const T& other) const {
      return first == other.first && second == other.second;
    }

    template <typename T>
    bool operator!=(const T& other) const {
      return !(*this == other);
    }

    void incrementIndex() {
      index_++;
      first.setIndex(index_);
      second.setIndex(index_);
    }

    vector_size_t index() const {
      return index_;
    }

   private:
    vector_size_t index_;
  };

  using Iterator = IndexBasedIterator<Element>;

  Iterator begin() const {
    return Iterator{Element{keyReader_, valueReader_, 0 + offset_}};
  }

  Iterator end() const {
    return Iterator{Element{keyReader_, valueReader_, size_ + offset_}};
  }

  const Element operator[](vector_size_t index) const {
    return Element{keyReader_, valueReader_, index + offset_};
  }

  size_t size() const {
    return size_;
  }

  Iterator find(const key_element_t& key) const {
    auto it = begin();
    while (it != end()) {
      if (it->first == key) {
        break;
      }
      it++;
    }
    return it;
  }

  ValueAccessor at(const key_element_t& key) const {
    auto it = find(key);
    VELOX_USER_CHECK(it != end(), "accessed key is not found in the map");
    return it->second;
  }

  // Cast to SlowMapVal<K,V> used to allow this to be written to current writer.
  // This should change once we implement writer proxies.
  operator core::SlowMapVal<K, V>() const {
    core::SlowMapVal<K, V> map;
    for (const auto& entry : map) {
      // Note: This does not handle nested maps, but since it's just workaround
      // its ok for now.
      map.appendNullable(entry.first) = entry.second;
    }
    return map;
  }

 private:
  const key_reader_t* keyReader_;
  const value_reader_t* valueReader_;
  const vector_size_t offset_;
  const vector_size_t size_;
};
} // namespace facebook::velox::exec
