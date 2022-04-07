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

#include "folly/container/F14Map.h"
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

// Base class for ArrayView::Iterator and MapView::Iterator. The missing parts
// to be implemented by deriving classes are: operator*() and operator->().
template <typename T, typename TIndex>
class IndexBasedIterator {
 public:
  using Iterator = IndexBasedIterator<T, TIndex>;
  using iterator_category = std::input_iterator_tag;
  using value_type = T;
  using difference_type = int;
  using pointer = PointerWrapper<value_type>;
  using reference = T;

  explicit IndexBasedIterator<value_type, TIndex>(int64_t index)
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

 protected:
  TIndex index_;
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

  class Iterator : public IndexBasedIterator<Element, vector_size_t> {
   public:
    Iterator(const reader_t* reader, vector_size_t index)
        : IndexBasedIterator<Element, vector_size_t>(index), reader_(reader) {}

    PointerWrapper<Element> operator->() const {
      if constexpr (returnsOptionalValues) {
        return PointerWrapper(Element{reader_, this->index_});
      } else {
        return PointerWrapper(reader_->readNullFree(this->index_));
      }
    }

    Element operator*() const {
      if constexpr (returnsOptionalValues) {
        return Element{reader_, this->index_};
      } else {
        return reader_->readNullFree(this->index_);
      }
    }

   protected:
    const reader_t* reader_;
  };

  Iterator begin() const {
    return Iterator{reader_, offset_};
  }

  Iterator end() const {
    return Iterator{reader_, offset_ + size_};
  }

  struct SkipNullsContainer {
    class SkipNullsBaseIterator : public Iterator {
     public:
      SkipNullsBaseIterator(const reader_t* reader, vector_size_t index)
          : Iterator(reader, index) {}

      bool hasValue() const {
        return this->reader_->isSet(this->index_);
      }

      element_t value() const {
        return (*this->reader_)[this->index_];
      }
    };

    explicit SkipNullsContainer(const ArrayView* array_) : array_(array_) {}

    SkipNullsIterator<SkipNullsBaseIterator> begin() {
      return SkipNullsIterator<SkipNullsBaseIterator>::initialize(
          SkipNullsBaseIterator{array_->reader_, array_->offset_},
          SkipNullsBaseIterator{
              array_->reader_, array_->offset_ + array_->size_});
    }

    SkipNullsIterator<SkipNullsBaseIterator> end() {
      return SkipNullsIterator<SkipNullsBaseIterator>{
          SkipNullsBaseIterator{
              array_->reader_, array_->offset_ + array_->size_},
          SkipNullsBaseIterator{
              array_->reader_, array_->offset_ + array_->size_}};
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

  class Iterator : public IndexBasedIterator<Element, vector_size_t> {
   public:
    Iterator(
        const key_reader_t* keyReader,
        const value_reader_t* valueReader,
        vector_size_t index)
        : IndexBasedIterator<Element, vector_size_t>(index),
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

 public:
  RowView(const reader_t* childReaders, vector_size_t offset)
      : childReaders_{childReaders}, offset_{offset} {}

  template <size_t N>
  elem_n_t<N> at() const {
    if constexpr (returnsOptionalValues) {
      return elem_n_t<N>{std::get<N>(*childReaders_).get(), offset_};
    } else {
      return std::get<N>(*childReaders_)->readNullFree(offset_);
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

class GenericView {
 public:
  GenericView(const BaseVector* baseVector, vector_size_t index)
      : baseVector_(baseVector), index_(index) {}

  uint64_t hash() const {
    return baseVector_->hashValueAt(index_);
  }

  bool operator==(const GenericView& other) const {
    return baseVector_->equalValueAt(other.baseVector_, index_, other.index_);
  }

 private:
  // Represent the decoded vector of the represented element.
  const BaseVector* baseVector_;
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
