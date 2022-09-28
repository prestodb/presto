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
#include <optional>
#include <tuple>
#include <utility>
#include <variant>

#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/UdfTypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

template <typename T, typename B>
struct VectorWriter;

// Lightweight object that can be used as a proxy for array primitive elements.
template <typename T, bool allowNull = true>
struct PrimitiveWriter {
  using vector_t = typename TypeToFlatVector<T>::type;
  using element_t = typename CppToType<T>::NativeType;

  PrimitiveWriter(vector_t* flatVector, vector_size_t index)
      : flatVector_(flatVector), index_(index) {}

  void operator=(std::nullopt_t) {
    static_assert(allowNull, "not allowed to write null to this primitive");
    flatVector_->setNull(index_, true);
  }

  void operator=(element_t value) {
    flatVector_->set(index_, value);
  }

  void operator=(const std::optional<element_t>& value) {
    static_assert(allowNull, "not allowed to write null to this primitive");

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

template <typename V>
bool constexpr provide_std_interface =
    CppToType<V>::isPrimitiveType && !std::is_same_v<Varchar, V> &&
    !std::is_same_v<Varbinary, V> && !std::is_same_v<Any, V>;

// bool is an exception, it requires commit but also provides std::interface.
template <typename V>
bool constexpr requires_commit =
    !provide_std_interface<V> || std::is_same_v<bool, V>;

// The object passed to the simple function interface that represent a single
// array entry.
template <typename V>
class ArrayWriter {
  using child_writer_t = VectorWriter<V, void>;
  using element_t = typename child_writer_t::exec_out_t;

 public:
  // Note: size is with respect to the current size of this array being written.
  void reserve(vector_size_t size) {
    auto currentSize = size + valuesOffset_;

    if (UNLIKELY(currentSize > elementsVectorCapacity_)) {
      elementsVectorCapacity_ = std::pow(2, std::ceil(std::log2(currentSize)));
      childWriter_->ensureSize(elementsVectorCapacity_);
    }
  }

  // Add a new not null item to the array, increasing its size by 1.
  element_t& add_item() {
    resize(length_ + 1);
    auto index = valuesOffset_ + length_ - 1;

    if constexpr (!requires_commit<V>) {
      VELOX_DCHECK(provide_std_interface<V>);
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

  // Should be called by the user (VectorWriter) when null is committed to
  // pretect against user miss-use (writing to the writer then committing null).
  void finalizeNull() {
    // No need to commit last written items and innerOffset_ stays the same for
    // the next item.
    length_ = 0;
  }

  vector_size_t size() {
    return length_;
  }

  // 'size' is with respect to the current size of the array being written.
  void resize(vector_size_t size) {
    commitMostRecentChildItem();
    reserve(size);
    length_ = size;
  }

  // Functions below provide an std::like interface, and are enabled only when
  // the array element is primitive that is not string or bool.

  void push_back(element_t value) {
    static_assert(
        provide_std_interface<V>, "push_back not allowed for this array");
    resize(length_ + 1);
    back() = value;
  }

  void push_back(std::nullopt_t) {
    static_assert(
        provide_std_interface<V>, "push_back not allowed for this array");
    resize(length_ + 1);
    back() = std::nullopt;
  }

  void push_back(const std::optional<element_t>& value) {
    static_assert(
        provide_std_interface<V>, "push_back not allowed for this array");
    resize(length_ + 1);
    back() = value;
  }

  template <typename T = V>
  typename std::enable_if_t<provide_std_interface<T>, PrimitiveWriter<T>>
  operator[](vector_size_t index) {
    VELOX_DCHECK_LT(index, length_, "out of bound access");
    return PrimitiveWriter<V>{elementsVector_, valuesOffset_ + index};
  }

  template <typename T = V>
  typename std::enable_if_t<provide_std_interface<T>, PrimitiveWriter<T>>
  back() {
    return PrimitiveWriter<V>{elementsVector_, valuesOffset_ + length_ - 1};
  }

  // Any vector type with std-like optional-free interface.
  template <typename T>
  void copy_from(const T& data) {
    length_ = 0;
    add_items(data);
  }

  // Any vector type with std-like optional-free interface.
  template <typename VectorType>
  void add_items(const VectorType& data) {
    if constexpr (provide_std_interface<V>) {
      // TODO: accelerate this with memcpy.
      auto start = length_;
      resize(length_ + data.size());
      for (auto i = 0; i < data.size(); i++) {
        this->operator[](i + start) = data[i];
      }
    } else {
      for (const auto& item : data) {
        auto& writer = add_item();
        writer.copy_from(item);
      }
    }
  }

  // Copy from null-free ArrayView.
  void add_items(
      const typename VectorExec::template resolver<Array<V>>::null_free_in_type&
          arrayView) {
    // If the null buffer is allocated this will read every null bit.
    // TODO: create a copy version that avoids null checks (assumes not null)
    // even when null buffer is allocated.

    // The add_items above works for null-free ArrayView, but calling copy on
    // the vector directly uses memcpy which is more efficient.
    auto start = length_;
    resize(length_ + arrayView.size());
    elementsVector_->copy(
        arrayView.elementsVector(),
        valuesOffset_ + start,
        arrayView.offset(),
        arrayView.size());
  }

  // Copy from nullable ArrayView.
  void add_items(
      const typename VectorExec::template resolver<Array<V>>::in_type&
          arrayView) {
    auto start = length_;
    resize(length_ + arrayView.size());
    elementsVector_->copy(
        arrayView.elementsVector(),
        valuesOffset_ + start,
        arrayView.offset(),
        arrayView.size());
  }

 private:
  // Make sure user do not use those.
  ArrayWriter<V>() = default;
  ArrayWriter<V>(const ArrayWriter<V>&) = default;
  ArrayWriter<V>& operator=(const ArrayWriter<V>&) = default;

  void commitMostRecentChildItem() {
    if constexpr (requires_commit<V>) {
      if (needCommit_) {
        childWriter_->commit(true);
        needCommit_ = false;
      }
    }
  }

  void initialize(VectorWriter<Array<V>, void>* writer) {
    childWriter_ = &writer->childWriter_;
    elementsVector_ = &childWriter_->vector();
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

  template <typename... T>
  friend class RowWriter;

  friend class GenericWriter;

  template <typename T>
  friend class SimpleFunctionAdapter;
};

// The object passed to the simple function interface that represent a single
// output map entry.
template <typename K, typename V>
class MapWriter {
  using key_writer_t = VectorWriter<K, void>;
  using value_writer_t = VectorWriter<V, void>;

  using key_element_t = typename key_writer_t::exec_out_t;
  using value_element_t = typename value_writer_t::exec_out_t;

  // Provide std::interface only if both key and value are eligible types.
  static constexpr bool std_interface =
      provide_std_interface<K> && provide_std_interface<V>;

 public:
  // `size` is with respect to the current size of the map being written.
  void reserve(vector_size_t size) {
    auto currentSize = size + innerOffset_;

    if (UNLIKELY(currentSize > capacity_)) {
      capacity_ = std::pow(2, std::ceil(std::log2(currentSize)));

      keysWriter_->ensureSize(capacity_);
      valuesWriter_->ensureSize(capacity_);
    }
  }

  // Add a new not null item to the map, increasing its size by 1.
  std::tuple<key_element_t&, value_element_t&> add_item() {
    resize(length_ + 1);
    return std::tie(lastKeyWriter(), lastValueWriter());
  }

  // Add a new null value, return the key writer.
  key_element_t& add_null() {
    resize(length_ + 1);
    valuesVector_->setNull(indexOfLast(), true);
    return lastKeyWriter();
  }

  vector_size_t size() {
    return length_;
  }

  // Any map type iteratable in tuple like manner.
  template <typename MapType>
  void copy_from(const MapType& data) {
    resize(0);
    // TODO: acceletare this with memcpy.
    for (const auto& [key, value] : data) {
      auto [keyWriter, valueWriter] = add_item();
      // copy key
      if constexpr (provide_std_interface<K>) {
        keyWriter = key;
      } else {
        keyWriter.copy_from(key);
      }

      // copy value
      if constexpr (provide_std_interface<V>) {
        valueWriter = value;
      } else {
        valueWriter.copy_from(value);
      }
    }
  }

  // Copy from nullable MapView.
  void copy_from(
      const typename VectorExec::template resolver<Map<K, V>>::in_type&
          mapView) {
    resize(mapView.size());
    // TODO: replace with a copy that avoids null checking for keys.
    keysVector_->copy(
        mapView.keysVector(), innerOffset_, mapView.offset(), mapView.size());

    valuesVector_->copy(
        mapView.valuesVector(), innerOffset_, mapView.offset(), mapView.size());
  }

  // Copy from null-free MapView.
  void copy_from(const typename VectorExec::template resolver<
                 Map<K, V>>::null_free_in_type& mapView) {
    resize(mapView.size());
    // TODO: replace with a copy that avoids null checking for both values and
    // keys.
    keysVector_->copy(
        mapView.keysVector(), innerOffset_, mapView.offset(), mapView.size());

    valuesVector_->copy(
        mapView.valuesVector(), innerOffset_, mapView.offset(), mapView.size());
  }

  // 'size' is with respect to the current size of the array being written.
  void resize(vector_size_t size) {
    commitMostRecentChildItem();
    reserve(size);
    length_ = size;
  }

  std::tuple<PrimitiveWriter<K, false>, PrimitiveWriter<V>> operator[](
      vector_size_t index) {
    static_assert(std_interface, "operator [] not allowed for this map");
    VELOX_DCHECK_LT(index, length_, "out of bound access");
    return {
        PrimitiveWriter<K, false>{keysVector_, innerOffset_ + index},
        PrimitiveWriter<V>{valuesVector_, innerOffset_ + index}};
  }

  void emplace(key_element_t key, const std::optional<value_element_t>& value) {
    static_assert(std_interface, "emplace not allowed for this map");
    if (value.has_value()) {
      add_item() = std::make_tuple(key, *value);
    } else {
      add_null() = key;
    }
  }

  void emplace(key_element_t key, value_element_t value) {
    static_assert(std_interface, "emplace not allowed for this map");
    add_item() = std::make_tuple(key, value);
  }

  void emplace(key_element_t key, std::nullopt_t) {
    static_assert(std_interface, "emplace not allowed for this map");
    add_null() = key;
  }

 private:
  // Make sure user do not use those.
  MapWriter<K, V>() = default;

  MapWriter<K, V>(const MapWriter<K, V>&) = default;

  MapWriter<K, V>& operator=(const MapWriter<K, V>&) = default;

  vector_size_t indexOfLast() {
    return innerOffset_ + length_ - 1;
  }

  // Should be called by the user (VectorWriter) when writing is done to
  // commit last item if needed.
  void finalize() {
    commitMostRecentChildItem();
    innerOffset_ += length_;
    length_ = 0;
  }

  // Should be called by the user (VectorWriter) when null is committed to
  // pretect against user miss-use (writing to the writer then committing null).
  void finalizeNull() {
    // No need to commit last written items and innerOffset_ stays the same for
    // the next item.
    length_ = 0;
  }

  void commitMostRecentChildItem() {
    if constexpr (requires_commit<K>) {
      if (keyNeedsCommit_) {
        keysWriter_->commit(true);
        keyNeedsCommit_ = false;
      }
    }

    if constexpr (requires_commit<V>) {
      if (valueNeedsCommit_) {
        valuesWriter_->commit(true);
        valueNeedsCommit_ = false;
      }
    }
  }

  void initialize(VectorWriter<Map<K, V>, void>* writer) {
    keysWriter_ = &writer->keyWriter_;
    valuesWriter_ = &writer->valWriter_;

    keysVector_ = &keysWriter_->vector();
    valuesVector_ = &valuesWriter_->vector();

    // Keys can never be null.
    keysVector_->resetNulls();

    keysWriter_->ensureSize(1);
    valuesWriter_->ensureSize(1);

    VELOX_DCHECK(
        keysVector_->size() == valuesVector_->size(),
        "expect map keys and value vector sized to be synchronized");
    capacity_ = keysVector_->size();
  }

  key_element_t& lastKeyWriter() {
    auto index = indexOfLast();
    if constexpr (!requires_commit<K>) {
      VELOX_DCHECK(provide_std_interface<K>);
      return keysWriter_->data_[index];
    } else {
      keyNeedsCommit_ = true;
      keysWriter_->setOffset(index);
      return keysWriter_->current();
    }
  }

  value_element_t& lastValueWriter() {
    auto index = indexOfLast();
    if constexpr (!requires_commit<V>) {
      VELOX_DCHECK(provide_std_interface<V>);
      valuesVector_->setNull(index, false);
      return valuesWriter_->data_[index];
    } else {
      valueNeedsCommit_ = true;
      valuesWriter_->setOffset(index);
      return valuesWriter_->current();
    }
  }

  typename key_writer_t::vector_t* keysVector_ = nullptr;
  typename value_writer_t::vector_t* valuesVector_ = nullptr;

  key_writer_t* keysWriter_ = nullptr;
  value_writer_t* valuesWriter_ = nullptr;

  // Tracks the capacity of keys and values vectors.
  vector_size_t capacity_ = 0;

  bool keyNeedsCommit_ = false;
  bool valueNeedsCommit_ = false;

  // Length of the current map being written.
  vector_size_t length_ = 0;

  // The offset within the children vectors at which this map starts.
  vector_size_t innerOffset_ = 0;

  template <typename A, typename B>
  friend struct VectorWriter;

  template <typename... T>
  friend class RowWriter;

  friend class GenericWriter;

  template <typename T>
  friend class SimpleFunctionAdapter;
};

// The object passed to the simple function interface that represent a single
// output row entry.

template <typename... T>
class RowWriter;

template <size_t I, class... Types>
inline auto get(const RowWriter<Types...>& writer) {
  using type = std::tuple_element_t<I, std::tuple<Types...>>;
  static_assert(
      provide_std_interface<type>,
      "operation not supported, use general interface instead");

  return PrimitiveWriter<type>(
      std::get<I>(writer.childrenVectors_), writer.offset_);
}

template <typename... T>
class RowWriter {
 public:
  using writers_t = std::tuple<VectorWriter<T, void>...>;
  static constexpr bool std_interface =
      (true && ... && provide_std_interface<T>);

  template <vector_size_t I>
  void set_null_at() {
    std::get<I>(childrenVectors_)->setNull(offset_, true);
    std::get<I>(needCommit_) = false;
  }

  template <size_t I>
  typename std::tuple_element_t<I, writers_t>::exec_out_t& get_writer_at() {
    using Type = std::tuple_element_t<I, std::tuple<T...>>;
    if constexpr (!requires_commit<Type>) {
      VELOX_DCHECK(provide_std_interface<Type>);
      std::get<I>(childrenVectors_)->setNull(offset_, false);
      return std::get<I>(childrenWriters_).data_[offset_];
    } else {
      std::get<I>(needCommit_) = true;
      std::get<I>(childrenWriters_).setOffset(offset_);
      return std::get<I>(childrenWriters_).current();
    }
  }

  void operator=(const std::tuple<T...>& inputs) {
    static_assert(
        std_interface,
        "operation not supported, use general interface instead");
    assignImpl(inputs, std::index_sequence_for<T...>{});
  }

  void operator=(const std::tuple<std::optional<T>...>& inputs) {
    static_assert(
        std_interface,
        "operation not supported, use general interface instead");
    assignImpl(inputs, std::index_sequence_for<T...>{});
  }

  template <typename... K>
  void copy_from(const std::tuple<K...>& inputs) {
    copyFromImpl(inputs, std::index_sequence_for<T...>{});
  }

 private:
  // Make sure user do not use those.
  RowWriter() = default;

  RowWriter(const RowWriter&) = default;

  RowWriter& operator=(const RowWriter&) = default;

  void initialize() {
    initializeImpl(std::index_sequence_for<T...>());
  }

  template <std::size_t... Is>
  void initializeImpl(std::index_sequence<Is...>) {
    (
        [&]() {
          std::get<Is>(needCommit_) = false;
          std::get<Is>(childrenVectors_) =
              &std::get<Is>(childrenWriters_).vector();
        }(),
        ...);
  }

  template <typename... K, std::size_t... Is>
  void copyFromImpl(
      const std::tuple<K...>& inputs,
      std::index_sequence<Is...>) {
    using children_types = std::tuple<T...>;
    (
        [&]() {
          if constexpr (provide_std_interface<
                            std::tuple_element_t<Is, children_types>>) {
            exec::get<Is>(*this) = std::get<Is>(inputs);
          } else {
            auto& writer = get_writer_at<Is>();
            writer.copy_from(std::get<Is>(inputs));
          }
        }(),
        ...);
  }

  template <std::size_t... Is>
  void assignImpl(const std::tuple<T...>& inputs, std::index_sequence<Is...>) {
    ((std::get<Is>(childrenVectors_)->set(offset_, std::get<Is>(inputs))), ...);
  }

  template <std::size_t... Is>
  void assignImpl(
      const std::tuple<std::optional<T>...>& inputs,
      std::index_sequence<Is...>) {
    ((exec::get<Is>(*this) = std::get<Is>(inputs)), ...);
  }

  void finalize() {
    finalizeImpl(std::index_sequence_for<T...>{});
  }

  template <std::size_t... Is>
  void finalizeImpl(std::index_sequence<Is...>) {
    using children_types = std::tuple<T...>;
    (
        [&]() {
          if constexpr (requires_commit<
                            std::tuple_element_t<Is, children_types>>) {
            if (std::get<Is>(needCommit_)) {
              // Commit not null.
              std::get<Is>(childrenWriters_).commit(true);
              std::get<Is>(needCommit_) = false;
            }
          }
        }(),
        ...);
  }

  void finalizeNull() {
    finalizeNullImpl(std::index_sequence_for<T...>{});
  }

  template <std::size_t... Is>
  void finalizeNullImpl(std::index_sequence<Is...>) {
    using children_types = std::tuple<T...>;
    (
        [&]() {
          using current_t = std::tuple_element_t<Is, children_types>;
          if constexpr (
              !provide_std_interface<current_t> &&
              !isOpaqueType<current_t>::value) {
            if (UNLIKELY(std::get<Is>(needCommit_))) {
              std::get<Is>(childrenWriters_).current().finalizeNull();
              std::get<Is>(needCommit_) = false;
            }
          }
        }(),
        ...);
  }

  writers_t childrenWriters_;

  std::tuple<typename VectorWriter<T, void>::vector_t*...> childrenVectors_;

  template <typename>
  using Bool = bool;

  std::tuple<Bool<T>...> needCommit_;

  vector_size_t offset_;

  template <typename A, typename B>
  friend struct VectorWriter;

  template <typename... A>
  friend class RowWriter;

  friend class GenericWriter;

  template <size_t I, class... Types>
  friend auto get(const RowWriter<Types...>& writer);
};

// GenericWriter represents a writer of any type. It has to be casted to one
// specific type first in order to write values to a vector. A GenericWriter
// must be casted to the same type throughout its lifetime, or an exception will
// throw. Right now, only casting to the types in writer_variant_t is supported.
// Casting to unsupported types causes compilation error.
class GenericWriter {
 public:
  // Make sure user do not use these.
  GenericWriter() = delete;

  GenericWriter(const GenericWriter&) = delete;

  GenericWriter& operator=(const GenericWriter&) = delete;

  template <typename T>
  using writer_ptr_t = std::shared_ptr<VectorWriter<T, void>>;

  using writer_variant_t = std::variant<
      writer_ptr_t<bool>,
      writer_ptr_t<int8_t>,
      writer_ptr_t<int16_t>,
      writer_ptr_t<int32_t>,
      writer_ptr_t<int64_t>,
      writer_ptr_t<float>,
      writer_ptr_t<double>,
      writer_ptr_t<Varchar>,
      writer_ptr_t<Varbinary>,
      writer_ptr_t<Array<Any>>,
      writer_ptr_t<Map<Any, Any>>,
      writer_ptr_t<Row<Any>>,
      writer_ptr_t<Row<Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any, Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any, Any, Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any, Any, Any, Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any, Any, Any, Any, Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any, Any, Any, Any, Any, Any, Any>>,
      writer_ptr_t<Row<Any, Any, Any, Any, Any, Any, Any, Any, Any, Any>>,
      writer_ptr_t<DynamicRow>>;

  GenericWriter(writer_variant_t& castWriter, TypePtr& castType, size_t& index)
      : castWriter_{castWriter}, castType_{castType}, index_{index} {}

  TypeKind kind() const {
    return vector_->typeKind();
  }

  const TypePtr type() const {
    return vector_->type();
  }

  template <typename ToType>
  typename VectorWriter<ToType, void>::exec_out_t& castTo() {
    VELOX_USER_CHECK(
        CastTypeChecker<ToType>::check(type()),
        "castTo type is not compatible with type of vector, vector type is {}, casted to type is {}",
        type()->toString(),
        std::is_same_v<ToType, DynamicRow>
            ? "DynamicRow"
            : CppToType<ToType>::create()->toString());

    return *castToImpl<ToType, false>();
  }

  template <typename ToType>
  typename VectorWriter<ToType, void>::exec_out_t* tryCastTo() {
    if (!CastTypeChecker<ToType>::check(type())) {
      return nullptr;
    }

    return castToImpl<ToType, true>();
  }

  template <typename T>
  struct isRowWriter : public std::false_type {};

  template <typename... T>
  struct isRowWriter<writer_ptr_t<Row<T...>>> : public std::false_type {};

  template <typename T>
  void finalizeNullDispatch(T& writer) {
    if constexpr (
        std::is_same_v<T, writer_ptr_t<Array<Any>>> ||
        std::is_same_v<T, writer_ptr_t<Map<Any, Any>>> ||
        std::is_same_v<T, writer_ptr_t<DynamicRow>> || isRowWriter<T>::value) {
      writer->current().finalizeNull();
    }
  }

  void finalizeNull() {
    if (castType_) {
      std::visit(
          [&](auto&& castedWriter) { finalizeNullDispatch(castedWriter); },
          castWriter_);
    }
  }

 private:
  void initialize(BaseVector* vector) {
    vector_ = vector;
  }

  template <typename ToType, bool tryCast>
  typename VectorWriter<ToType, void>::exec_out_t* castToImpl() {
    writer_ptr_t<ToType>* writer = nullptr;

    if (castType_) {
      writer = retrieveCastedWriter<ToType>();
      if (!writer) {
        if constexpr (tryCast) {
          return nullptr;
        } else {
          VELOX_USER_FAIL(
              "Not allowed to cast to two different types {} and {} within the same batch.",
              castType_->toString(),
              std::is_same_v<ToType, DynamicRow>
                  ? "DynamicRow"
                  : CppToType<ToType>::create()->toString());
        }
      }
    } else {
      writer = ensureWriter<ToType>();
    }

    auto& typedWriter = *writer;
    typedWriter->setOffset(index_);
    return &typedWriter->current();
  }

  // Assuming the writer has been casted before and castType_ is not null,
  // return a pointer to the casted writer if B matches with the previous
  // cast type exactly. Return nullptr otherwise.
  template <typename B>
  writer_ptr_t<B>* retrieveCastedWriter() {
    DCHECK(castType_);

    if (!std::holds_alternative<writer_ptr_t<B>>(castWriter_)) {
      return nullptr;
    }
    return &std::get<writer_ptr_t<B>>(castWriter_);
  }

  template <typename B>
  writer_ptr_t<B>* ensureWriter() {
    static_assert(
        !isGenericType<B>::value && !isVariadicType<B>::value,
        "Cannot cast to VectorWriter of Generic or Variadic");

    if constexpr (std::is_same_v<B, DynamicRow>) {
      castType_ = vector_->type();
    } else {
      auto requestedType = CppToType<B>::create();
      castType_ = std::move(requestedType);
    }

    castWriter_ = std::make_shared<VectorWriter<B, void>>();
    auto& writer = std::get<writer_ptr_t<B>>(castWriter_);
    writer->init(*vector_->as<typename TypeToFlatVector<B>::type>());
    return &writer;
  }

  BaseVector* vector_;
  writer_variant_t& castWriter_;
  TypePtr& castType_;
  size_t& index_;

  template <typename A, typename B>
  friend struct VectorWriter;
};

} // namespace facebook::velox::exec
