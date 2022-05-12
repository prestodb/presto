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
#include <fmt/format.h>
#include <iterator>
#include <optional>

#include "folly/container/F14Map.h"
#include "velox/common/base/CompareFlags.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::exec {

template <typename T>
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

// Base class for ArrayView::Iterator, MapView::Iterator, and
// VariadicView::Iterator.
// TElementAccessor is a class that supplies the following:
// index_t: the type to use for indexes into the container. It should be signed
// to allow, for example, loops to iterate backwards and end at a point before
// the beginning of the container.
// element_t: the type returned when the iterator is dereferenced.
// element_t operator()(index_t index): returns the element at the provided
// index in the container being iterated over.
template <typename TElementAccessor>
class IndexBasedIterator {
 public:
  using Iterator = IndexBasedIterator<TElementAccessor>;
  using index_t = typename TElementAccessor::index_t;
  using element_t = typename TElementAccessor::element_t;
  using iterator_category = std::random_access_iterator_tag;
  using value_type = element_t;
  using difference_type = index_t;
  using pointer = PointerWrapper<element_t>;
  using reference = element_t;

  explicit IndexBasedIterator(
      index_t index,
      index_t containerStartIndex,
      index_t containerEndIndex,
      const TElementAccessor& elementAccessor)
      : index_(index),
        containerStartIndex_(containerStartIndex),
        containerEndIndex_(containerEndIndex),
        elementAccessor_(elementAccessor) {}

  PointerWrapper<element_t> operator->() const {
    validateBounds(index_);

    return PointerWrapper(elementAccessor_(index_));
  }

  element_t operator*() const {
    validateBounds(index_);

    return elementAccessor_(index_);
  }

  element_t operator[](difference_type n) const {
    return elementAccessor_(index_ + n);
  }

  bool operator!=(const Iterator& rhs) const {
    return index_ != rhs.index_;
  }

  bool operator==(const Iterator& rhs) const {
    return index_ == rhs.index_;
  }

  bool operator<(const Iterator& rhs) const {
    return index_ < rhs.index_;
  }

  bool operator>(const Iterator& rhs) const {
    return index_ > rhs.index_;
  }

  bool operator<=(const Iterator& rhs) const {
    return index_ <= rhs.index_;
  }

  bool operator>=(const Iterator& rhs) const {
    return index_ >= rhs.index_;
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

  // Implement post decrement.
  Iterator operator--(int) {
    Iterator old = *this;
    --*this;
    return old;
  }

  // Implement pre decrement.
  Iterator& operator--() {
    index_--;
    return *this;
  }

  Iterator& operator+=(const difference_type& rhs) {
    index_ += rhs;
    return *this;
  }

  Iterator& operator-=(const difference_type& rhs) {
    index_ -= rhs;
    return *this;
  }

  // Iterators +/- ints.
  Iterator operator+(difference_type rhs) const {
    return Iterator(
        this->index_ + rhs,
        containerStartIndex_,
        containerEndIndex_,
        elementAccessor_);
  }

  Iterator operator-(difference_type rhs) const {
    return Iterator(
        this->index_ - rhs,
        containerStartIndex_,
        containerEndIndex_,
        elementAccessor_);
  }

  // Subtract iterators.
  difference_type operator-(const Iterator& rhs) const {
    return this->index_ - rhs.index_;
  }

  friend Iterator operator+(
      typename Iterator::difference_type lhs,
      const Iterator& rhs) {
    return rhs + lhs;
  }

 protected:
  index_t index_;

  // Every instance of Iterator for the same container should have the same
  // values for these two fields.
  // The first index in the container. When begin() is called on a container
  // the returned iterator should have index_ == containerStartIndex_.
  index_t containerStartIndex_;

  // Last index in the container + 1. When end() is called on a
  // container the returned iterator should have index_ == containerEndIndex_.
  index_t containerEndIndex_;
  TElementAccessor elementAccessor_;

  inline void validateBounds(index_t index) const {
    VELOX_DCHECK_LT(
        index,
        containerEndIndex_,
        "Iterator has index {} beyond the length of the container {}.",
        index,
        containerEndIndex_);
    VELOX_DCHECK_GE(
        index,
        containerStartIndex_,
        "Iterator has index {} before the start of the container {}.",
        index,
        containerStartIndex_);
  }
};

// Implements an iterator for values that skips nulls and provides direct access
// to those values by wrapping another iterator of type BaseIterator.
//
// BaseIterator must implement the following functions:
//   hasValue() : Returns whether the current value pointed at by the iterator
//                is a null.
//   value()    : Returns the non-null value pointed at by the iterator.
template <typename BaseIterator>
class SkipNullsIterator {
  using Iterator = SkipNullsIterator<BaseIterator>;

  // SkipNullsIterator cannot meet the requirements of
  // random_access_iterator_tag as moving between elements is a linear time
  // operation.
  using iterator_category = std::input_iterator_tag;
  using value_type = typename std::
      invoke_result<decltype(&BaseIterator::value), BaseIterator>::type;
  using difference_type = int;
  using pointer = PointerWrapper<value_type>;
  using reference = value_type;

 public:
  SkipNullsIterator<BaseIterator>(
      const BaseIterator& begin,
      const BaseIterator& end)
      : iter_(begin), end_(end) {}

  // Given an element, return an iterator to the first not-null element starting
  // from the element itself.
  static Iterator initialize(
      const BaseIterator& begin,
      const BaseIterator& end) {
    auto it = Iterator{begin, end};

    // The container is empty.
    if (begin >= end) {
      return it;
    }

    if (begin.hasValue()) {
      return it;
    }

    // Move to next not null.
    it++;
    return it;
  }

  value_type operator*() const {
    // Always return a copy, it's guaranteed to be cheap object.
    return iter_.value();
  }

  PointerWrapper<value_type> operator->() const {
    return PointerWrapper(iter_.value());
  }

  bool operator<(const Iterator& rhs) const {
    return iter_ < rhs.iter_;
  }

  bool operator!=(const Iterator& rhs) const {
    return iter_ != rhs.iter_;
  }

  bool operator==(const Iterator& rhs) const {
    return iter_ == rhs.iter_;
  }

  // Implement post increment.
  Iterator operator++(int) {
    Iterator old = *this;
    ++*this;
    return old;
  }

  // Implement pre increment.
  Iterator& operator++() {
    iter_++;
    while (iter_ != end_) {
      if (iter_.hasValue()) {
        break;
      }
      iter_++;
    }
    return *this;
  }

 private:
  BaseIterator iter_;
  // Iterator pointing just beyond the range to expose.
  const BaseIterator end_;
};

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

  VectorOptionalValueAccessor<T>(const T* reader, int64_t index)
      : reader_(reader), index_(index) {}

 private:
  const T* reader_;
  // Index of element within the reader.
  int64_t index_;

  template <bool nullable, typename V>
  friend class ArrayView;

  template <bool nullable, typename K, typename V>
  friend class MapView;

  template <bool nullable, typename... U>
  friend class RowView;

  template <bool nullable, typename U>
  friend class VariadicView;
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

// Helper function that calls materialize on element if it's not primitive.
template <typename VeloxType, typename T>
auto materializeElement(const T& element) {
  if constexpr (MaterializeType<VeloxType>::requiresMaterialization) {
    return element.materialize();
  } else if constexpr (util::is_shared_ptr<VeloxType>::value) {
    return *element;
  } else {
    return element;
  }
}

// Represents an array of elements with an interface similar to std::vector.
// When returnsOptionalValues is true, the interface is like
// std::vector<std::optional<V>>.
// When returnsOptionalValues is false, the interface is like std::vector<V>.
template <bool returnsOptionalValues, typename V>
class ArrayView {
  using reader_t = VectorReader<V>;
  using element_t = typename std::conditional<
      returnsOptionalValues,
      typename reader_t::exec_in_t,
      typename reader_t::exec_null_free_in_t>::type;

 public:
  ArrayView(const reader_t* reader, vector_size_t offset, vector_size_t size)
      : reader_(reader), offset_(offset), size_(size) {}

  using Element = typename std::conditional<
      returnsOptionalValues,
      VectorOptionalValueAccessor<reader_t>,
      element_t>::type;

  class ElementAccessor {
   public:
    using element_t = Element;
    using index_t = vector_size_t;

    explicit ElementAccessor(const reader_t* reader) : reader_(reader) {}

    Element operator()(vector_size_t index) const {
      if constexpr (returnsOptionalValues) {
        return Element{reader_, index};
      } else {
        return reader_->readNullFree(index);
      }
    }

   private:
    const reader_t* reader_;
  };

  using Iterator = IndexBasedIterator<ElementAccessor>;

  Iterator begin() const {
    return Iterator{
        offset_, offset_, offset_ + size_, ElementAccessor(reader_)};
  }

  Iterator end() const {
    return Iterator{
        offset_ + size_, offset_, offset_ + size_, ElementAccessor(reader_)};
  }

  struct SkipNullsContainer {
    class SkipNullsBaseIterator : public Iterator {
     public:
      SkipNullsBaseIterator(
          const reader_t* reader,
          vector_size_t index,
          vector_size_t startIndex,
          vector_size_t endIndex)
          : Iterator(index, startIndex, endIndex, ElementAccessor(reader)),
            reader_(reader) {}

      bool hasValue() const {
        return reader_->isSet(this->index_);
      }

      element_t value() const {
        return (*reader_)[this->index_];
      }

     private:
      const reader_t* reader_;
    };

    explicit SkipNullsContainer(const ArrayView* array_) : array_(array_) {}

    SkipNullsIterator<SkipNullsBaseIterator> begin() {
      return SkipNullsIterator<SkipNullsBaseIterator>::initialize(
          SkipNullsBaseIterator{
              array_->reader_,
              array_->offset_,
              array_->offset_,
              array_->offset_ + array_->size_},
          SkipNullsBaseIterator{
              array_->reader_,
              array_->offset_ + array_->size_,
              array_->offset_,
              array_->offset_ + array_->size_});
    }

    SkipNullsIterator<SkipNullsBaseIterator> end() {
      return SkipNullsIterator<SkipNullsBaseIterator>{
          SkipNullsBaseIterator{
              array_->reader_,
              array_->offset_ + array_->size_,
              array_->offset_,
              array_->offset_ + array_->size_},
          SkipNullsBaseIterator{
              array_->reader_,
              array_->offset_ + array_->size_,
              array_->offset_,
              array_->offset_ + array_->size_}};
    }

   private:
    const ArrayView* array_;
  };

  // Returns true if any of the arrayViews in the vector might have null
  // element.
  bool mayHaveNulls() const {
    if constexpr (returnsOptionalValues) {
      return reader_->mayHaveNulls();
    } else {
      return false;
    }
  }

  using materialize_t = typename std::conditional<
      returnsOptionalValues,
      typename MaterializeType<Array<V>>::nullable_t,
      typename MaterializeType<Array<V>>::null_free_t>::type;

  materialize_t materialize() const {
    materialize_t result;

    for (const auto& element : *this) {
      if constexpr (returnsOptionalValues) {
        if (element.has_value()) {
          result.push_back({materializeElement<V>(element.value())});
        } else {
          result.push_back(std::nullopt);
        }
      } else {
        result.push_back(materializeElement<V>(element));
      }
    }
    return result;
  }

  Element operator[](vector_size_t index) const {
    if constexpr (returnsOptionalValues) {
      return Element{reader_, index + offset_};
    } else {
      return reader_->readNullFree(index + offset_);
    }
  }

  Element at(vector_size_t index) const {
    return (*this)[index];
  }

  vector_size_t size() const {
    return size_;
  }

  SkipNullsContainer skipNulls() const {
    if constexpr (returnsOptionalValues) {
      return SkipNullsContainer{this};
    }

    VELOX_UNSUPPORTED(
        "ArrayViews over NULL-free data do not support skipNulls().  It's "
        "already been checked that this object contains no NULLs, it's more "
        "efficient to use the standard iterator interface.");
  }

  const BaseVector* elementsVector() const {
    return reader_->baseVector();
  }

  vector_size_t offset() const {
    return offset_;
  }

 private:
  const reader_t* reader_;
  vector_size_t offset_;
  vector_size_t size_;
};

// This class is used to represent map inputs in simple functions with an
// interface similar to std::map.
// When returnsOptionalValues is true, the interface is like std::map<K,
// std::optional<V>>.
// When returnsOptionalValues is false, the interface is like std::map<K, V>.
template <bool returnsOptionalValues, typename K, typename V>
class MapView {
 public:
  using key_reader_t = VectorReader<K>;
  using value_reader_t = VectorReader<V>;
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

  using ValueAccessor = typename std::conditional<
      returnsOptionalValues,
      VectorOptionalValueAccessor<value_reader_t>,
      typename value_reader_t::exec_null_free_in_t>::type;

  using KeyAccessor = typename std::conditional<
      returnsOptionalValues,
      typename key_reader_t::exec_in_t,
      typename key_reader_t::exec_null_free_in_t>::type;

  class Element {
   public:
    Element(
        const key_reader_t* keyReader,
        const value_reader_t* valueReader,
        int64_t index)
        : first(getFirst(keyReader, index)),
          second(getSecond(valueReader, index)) {}
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
    // Helper functions to allow us to use "if constexpr" when initializing
    // the values.
    KeyAccessor getFirst(const key_reader_t* keyReader, int64_t index) {
      if constexpr (returnsOptionalValues) {
        return (*keyReader)[index];
      } else {
        return keyReader->readNullFree(index);
      }
    }

    ValueAccessor getSecond(const value_reader_t* valueReader, int64_t index) {
      if constexpr (returnsOptionalValues) {
        return ValueAccessor(valueReader, index);
      } else {
        return valueReader->readNullFree(index);
      }
    }
  };

  class ElementAccessor {
   public:
    using element_t = Element;
    using index_t = vector_size_t;

    ElementAccessor(
        const key_reader_t* keyReader,
        const value_reader_t* valueReader)
        : keyReader_(keyReader), valueReader_(valueReader) {}

    Element operator()(vector_size_t index) const {
      return Element{keyReader_, valueReader_, index};
    }

   private:
    const key_reader_t* keyReader_;
    const value_reader_t* valueReader_;
  };

  using Iterator = IndexBasedIterator<ElementAccessor>;

  Iterator begin() const {
    return Iterator{
        offset_,
        offset_,
        offset_ + size_,
        ElementAccessor(keyReader_, valueReader_)};
  }

  Iterator end() const {
    return Iterator{
        offset_ + size_,
        offset_,
        offset_ + size_,
        ElementAccessor(keyReader_, valueReader_)};
  }

  // Index-based access for the map elements.
  const Element atIndex(vector_size_t index) const {
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

  // Beware!! runtime is O(N)!!
  ValueAccessor operator[](const key_element_t& key) const {
    return at(key);
  }

  using materialize_t = typename std::conditional<
      returnsOptionalValues,
      typename MaterializeType<Map<K, V>>::nullable_t,
      typename MaterializeType<Map<K, V>>::null_free_t>::type;

  materialize_t materialize() const {
    materialize_t result;
    for (const auto& [key, value] : *this) {
      if constexpr (returnsOptionalValues) {
        if (value.has_value()) {
          result.emplace(
              materializeElement<K>(key), materializeElement<V>(value.value()));
        } else {
          result.emplace(materializeElement<K>(key), std::nullopt);
        }
      } else {
        result.emplace(
            materializeElement<K>(key), materializeElement<V>(value));
      }
    }
    return result;
  }

  const BaseVector* keysVector() const {
    return keyReader_->baseVector();
  }

  const BaseVector* valuesVector() const {
    return valueReader_->baseVector();
  }

  vector_size_t offset() const {
    return offset_;
  }

 private:
  const key_reader_t* keyReader_;
  const value_reader_t* valueReader_;
  vector_size_t offset_;
  vector_size_t size_;
};

template <bool returnsOptionalValues, typename... T>
class RowView {
  using reader_t = std::tuple<std::unique_ptr<VectorReader<T>>...>;
  template <size_t N>
  using elem_n_t = typename std::conditional<
      returnsOptionalValues,
      VectorOptionalValueAccessor<
          typename std::tuple_element<N, reader_t>::type::element_type>,
      typename std::tuple_element<N, reader_t>::type::element_type::
          exec_null_free_in_t>::type;

  vector_size_t offset() const {
    return offset_;
  }

  vector_size_t childVectorAt() const {
    return offset_;
  }

 public:
  RowView(const reader_t* childReaders, vector_size_t offset)
      : childReaders_{childReaders}, offset_{offset} {}

  template <size_t I>
  elem_n_t<I> at() const {
    if constexpr (returnsOptionalValues) {
      return elem_n_t<I>{std::get<I>(*childReaders_).get(), offset_};
    } else {
      return std::get<I>(*childReaders_)->readNullFree(offset_);
    }
  }

  using materialize_t = typename std::conditional<
      returnsOptionalValues,
      typename MaterializeType<Row<T...>>::nullable_t,
      typename MaterializeType<Row<T...>>::null_free_t>::type;

  materialize_t materialize() const {
    materialize_t result;
    materializeImpl(result, std::index_sequence_for<T...>());

    return result;
  }

 private:
  void initialize() {
    initializeImpl(std::index_sequence_for<T...>());
  }

  using children_types = std::tuple<T...>;
  template <std::size_t... Is>
  void materializeImpl(materialize_t& result, std::index_sequence<Is...>)
      const {
    (
        [&]() {
          using child_t = typename std::tuple_element_t<Is, children_types>;
          const auto& element = at<Is>();
          if constexpr (returnsOptionalValues) {
            if (element.has_value()) {
              std::get<Is>(result) = {materializeElement<child_t>(*element)};
            } else {
              std::get<Is>(result) = std::nullopt;
            }
          } else {
            std::get<Is>(result) = materializeElement<child_t>(element);
          }
        }(),
        ...);
  }
  const reader_t* childReaders_;
  vector_size_t offset_;
};

template <size_t I, bool returnsOptionalValues, class... Types>
inline auto get(const RowView<returnsOptionalValues, Types...>& row) {
  return row.template at<I>();
}

template <typename T>
using reader_ptr_t = VectorReader<T>*;

template <typename T>
struct HasGeneric {
  static constexpr bool value() {
    return false;
  }
};

template <typename T>
struct HasGeneric<Generic<T>> {
  static constexpr bool value() {
    return true;
  }
};

template <typename K, typename V>
struct HasGeneric<Map<K, V>> {
  static constexpr bool value() {
    return HasGeneric<K>::value() || HasGeneric<V>::value();
  }
};

template <typename V>
struct HasGeneric<Array<V>> {
  static constexpr bool value() {
    return HasGeneric<V>::value();
  }
};

template <typename... T>
struct HasGeneric<Row<T...>> {
  static constexpr bool value() {
    return (HasGeneric<T>::value() || ...);
  }
};

// This is basically Array<Any>, Map<Any,Any>, Row<Any....>.
template <typename T>
struct AllGenericExceptTop {
  static constexpr bool value() {
    return false;
  }
};

template <typename V>
struct AllGenericExceptTop<Array<V>> {
  static constexpr bool value() {
    return isGenericType<V>::value;
  }
};

template <typename K, typename V>
struct AllGenericExceptTop<Map<K, V>> {
  static constexpr bool value() {
    return isGenericType<K>::value && isGenericType<V>::value;
  }
};

template <typename... T>
struct AllGenericExceptTop<Row<T...>> {
  static constexpr bool value() {
    return (isGenericType<T>::value && ...);
  }
};

class GenericView {
 public:
  GenericView(
      const DecodedVector& decoded,
      std::array<std::shared_ptr<void>, 3>& castReaders,
      TypePtr& castType,
      vector_size_t index)
      : decoded_(decoded),
        castReaders_(castReaders),
        castType_(castType),
        index_(index) {}

  uint64_t hash() const {
    return decoded_.base()->hashValueAt(index_);
  }

  bool operator==(const GenericView& other) const {
    return decoded_.base()->equalValueAt(
        other.decoded_.base(), index_, other.index_);
  }

  std::optional<int64_t> compare(
      const GenericView& other,
      const CompareFlags flags) const {
    return decoded_.base()->compare(
        other.decoded_.base(), index_, other.index_, flags);
  }

  TypeKind kind() const {
    return decoded_.base()->typeKind();
  }

  const TypePtr type() const {
    return decoded_.base()->type();
  }

  // If conversion is invalid, behavior is undefined. However, debug time
  // checks will throw an exception.
  template <typename ToType>
  typename VectorReader<ToType>::exec_in_t castTo() const {
    VELOX_DCHECK(
        CastTypeChecker<ToType>::check(type()),
        fmt::format(
            "castTo type is not compatible with type of vector, vector type is {}, casted to type is {}",
            type()->toString(),
            CppToType<ToType>::create()->toString()));

    // TODO: We can distinguish if this is a null-free or not null-free
    // generic. And based on that determine if we want to call operator[] or
    // readNullFree. For now we always return nullable.
    return ensureReader<ToType>()->operator[](index_);
  }

  template <typename ToType>
  std::optional<typename VectorReader<ToType>::exec_in_t> tryCastTo() const {
    if (!CastTypeChecker<ToType>::check(type())) {
      return std::nullopt;
    }

    return ensureReader<ToType>()->operator[](index_);
  }

 private:
  // Utility class that checks that vectorType matches T.
  template <typename T>
  struct CastTypeChecker {
    static bool check(const TypePtr& vectorType) {
      return CppToType<T>::typeKind == vectorType->kind();
    }
  };

  template <typename T>
  struct CastTypeChecker<Generic<T>> {
    static bool check(const TypePtr&) {
      return true;
    }
  };

  template <typename T>
  struct CastTypeChecker<Array<T>> {
    static bool check(const TypePtr& vectorType) {
      return TypeKind::ARRAY == vectorType->kind() &&
          CastTypeChecker<T>::check(vectorType->childAt(0));
    }
  };

  template <typename K, typename V>
  struct CastTypeChecker<Map<K, V>> {
    static bool check(const TypePtr& vectorType) {
      return TypeKind::MAP == vectorType->kind() &&
          CastTypeChecker<K>::check(vectorType->childAt(0)) &&
          CastTypeChecker<V>::check(vectorType->childAt(1));
    }
  };

  template <typename... T>
  struct CastTypeChecker<Row<T...>> {
    static bool check(const TypePtr& vectorType) {
      int index = 0;
      return TypeKind::ROW == vectorType->kind() &&
          (CastTypeChecker<T>::check(vectorType->childAt(index++)) && ... &&
           true);
    }
  };

  template <typename B>
  VectorReader<B>* ensureReader() const {
    static_assert(
        !isGenericType<B>::value && !isVariadicType<B>::value,
        "That does not make any sense! You cant cast to Generic or Variadic");

    // This is an optimization to avoid checking dynamically for every row that
    // the user is always casting to the same type.
    // Types are divided into three sets, for 1, and 2 we do not do the check,
    // since no two types can ever refer to the same vector.

    if constexpr (!HasGeneric<B>::value()) {
      // Two types with no generic can never represent same vector.
      return ensureReaderImpl<B, 0>();
    } else {
      if constexpr (AllGenericExceptTop<B>::value()) {
        // This is basically Array<Any>, Map<Any,Any>, Row<Any....>.
        return ensureReaderImpl<B, 1>();
      } else {
        auto requestedType = CppToType<B>::create();
        if (castType_) {
          VELOX_USER_CHECK(
              castType_->operator==(*requestedType),
              fmt::format(
                  "Not allowed to cast to the two types {} and {} within the same batch."
                  "Consider creating a new type set to allow it.",
                  castType_->toString(),
                  requestedType->toString()));
        } else {
          castType_ = std::move(requestedType);
        }
        return ensureReaderImpl<B, 2>();
      }
    }
  }

  template <typename B, size_t I>
  VectorReader<B>* ensureReaderImpl() const {
    auto* reader = static_cast<VectorReader<B>*>(castReaders_[I].get());
    if (LIKELY(reader != nullptr)) {
      return reader;
    } else {
      castReaders_[I] = std::make_shared<VectorReader<B>>(&decoded_);
      return static_cast<VectorReader<B>*>(castReaders_[I].get());
    }
  }

  const DecodedVector& decoded_;
  std::array<std::shared_ptr<void>, 3>& castReaders_;
  TypePtr& castType_;
  vector_size_t index_;
};

} // namespace facebook::velox::exec

namespace std {
template <>
struct hash<facebook::velox::exec::GenericView> {
  size_t operator()(const facebook::velox::exec::GenericView& x) const {
    return x.hash();
  }
};
} // namespace std
