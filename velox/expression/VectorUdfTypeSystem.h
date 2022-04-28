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

#include <velox/vector/BaseVector.h>
#include <velox/vector/TypeAliases.h>
#include <algorithm>
#include <array>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>

#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/ComplexViewTypes.h"
#include "velox/expression/ComplexWriterTypes.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VariadicView.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

template <bool reuseInput = false>
class StringWriter;

template <typename T>
struct VectorReader;

namespace detail {
template <typename T>
struct resolver {
  using in_type = typename CppToType<T>::NativeType;
  using out_type = typename CppToType<T>::NativeType;
};

template <typename K, typename V>
struct resolver<Map<K, V>> {
  using in_type = MapView<true, K, V>;
  using null_free_in_type = MapView<false, K, V>;
  using out_type = MapWriter<K, V>;
};

template <typename... T>
struct resolver<Row<T...>> {
  using in_type = RowView<true, T...>;
  using null_free_in_type = RowView<false, T...>;
  using out_type = RowWriter<T...>;
};

template <typename V>
struct resolver<Array<V>> {
  using in_type = ArrayView<true, V>;
  using null_free_in_type = ArrayView<false, V>;
  using out_type = ArrayWriter<V>;
};

template <>
struct resolver<Varchar> {
  using in_type = StringView;
  using out_type = StringWriter<>;
};

template <>
struct resolver<Varbinary> {
  using in_type = StringView;
  using out_type = StringWriter<>;
};

template <typename T>
struct resolver<std::shared_ptr<T>> {
  using in_type = std::shared_ptr<T>;
  using out_type = std::shared_ptr<T>;
};

template <typename T>
struct resolver<Variadic<T>> {
  using in_type = VariadicView<true, T>;
  using null_free_in_type = VariadicView<false, T>;
  // Variadic cannot be used as an out_type
};

template <typename T>
struct resolver<Generic<T>> {
  using in_type = GenericView;
  using out_type = void; // Not supported as output type yet.
};
} // namespace detail

struct VectorExec {
  template <typename T>
  using resolver = typename detail::template resolver<T>;
};

template <typename T, typename = void>
struct VectorWriter {
  // this default is for scalar types
  // todo(youknowjack): remove bounds checking
  using exec_out_t = typename VectorExec::template resolver<T>::out_type;
  using vector_t = typename TypeToFlatVector<T>::type;

  void init(vector_t& vector) {
    vector_ = &vector;
    data_ = vector.mutableRawValues();
  }

  void ensureSize(size_t size) {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
      data_ = vector_->mutableRawValues();
    }
  }

  VectorWriter() {}

  FOLLY_ALWAYS_INLINE exec_out_t& current() {
    return data_[offset_];
  }

  void copyCommit(const exec_out_t& data) {
    // this code path is called for copying nested structures to slices
    // in future, we want to eliminate this so all writes go directly to their
    // slice.
    data_[offset_] = data;
    vector_->setNull(offset_, false);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (!isSet) {
      vector_->setNull(offset_, true);
    } else {
      vector_->setNull(offset_, false);
    }
  }

  FOLLY_ALWAYS_INLINE void setOffset(int32_t offset) {
    offset_ = offset;
  }

  FOLLY_ALWAYS_INLINE vector_t& vector() {
    return *vector_;
  }

  vector_t* vector_;
  exec_out_t* data_;
  size_t offset_ = 0;
};

template <typename T>
struct VectorReader {
  using exec_in_t = typename VectorExec::template resolver<T>::in_type;
  // Types without views cannot contain null, they can only be null, so they're
  // in_type is already null_free.
  using exec_null_free_in_t =
      typename VectorExec::template resolver<T>::in_type;

  explicit VectorReader(const DecodedVector* decoded) : decoded_(*decoded) {}

  explicit VectorReader(const VectorReader<T>&) = delete;
  VectorReader<T>& operator=(const VectorReader<T>&) = delete;

  exec_in_t operator[](size_t offset) const {
    return decoded_.template valueAt<exec_in_t>(offset);
  }

  exec_null_free_in_t readNullFree(size_t offset) const {
    return decoded_.template valueAt<exec_null_free_in_t>(offset);
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  bool mayHaveNulls() const {
    return decoded_.mayHaveNulls();
  }

  // These functions can be used to check if any elements in a given row are
  // NULL. They are not especially fast, so they should only be used when
  // necessary, and other options, e.g. calling mayHaveNullsRecursive() on the
  // vector, have already been exhausted.
  inline bool containsNull(vector_size_t index) const {
    return decoded_.isNullAt(index);
  }

  bool containsNull(vector_size_t startIndex, vector_size_t endIndex) const {
    // Note: This can be optimized for the special case where the underlying
    // vector is flat using bit operations on the nulls buffer.
    for (auto index = startIndex; index < endIndex; ++index) {
      if (containsNull(index)) {
        return true;
      }
    }

    return false;
  }

  inline bool mayHaveNullsRecursive() const {
    return decoded_.mayHaveNulls();
  }

  // Scalars don't have children, so this is a no-op.
  void setChildrenMayHaveNulls() {}

  const DecodedVector& decoded_;
};

namespace detail {

template <typename TOut>
const TOut& getDecoded(const DecodedVector& decoded) {
  auto base = decoded.base();
  return *base->template as<TOut>();
}

inline DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}
} // namespace detail

template <typename K, typename V>
struct VectorReader<Map<K, V>> {
  using in_vector_t = typename TypeToFlatVector<Map<K, V>>::type;
  using exec_in_t = typename VectorExec::template resolver<Map<K, V>>::in_type;
  using exec_null_free_in_t =
      typename VectorExec::template resolver<Map<K, V>>::null_free_in_type;

  explicit VectorReader(const DecodedVector* decoded)
      : decoded_{*decoded},
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        offsets_(vector_.rawOffsets()),
        lengths_(vector_.rawSizes()),
        keyReader_{detail::decode(decodedKeys_, *vector_.mapKeys())},
        valReader_{detail::decode(decodedVals_, *vector_.mapValues())} {}

  explicit VectorReader(const VectorReader<Map<K, V>>&) = delete;
  VectorReader<Map<K, V>>& operator=(const VectorReader<Map<K, V>>&) = delete;

  exec_in_t operator[](size_t offset) const {
    auto index = decoded_.index(offset);
    return {&keyReader_, &valReader_, offsets_[index], lengths_[index]};
  }

  exec_null_free_in_t readNullFree(size_t offset) const {
    auto index = decoded_.index(offset);
    return {&keyReader_, &valReader_, offsets_[index], lengths_[index]};
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  bool containsNull(vector_size_t index) const {
    VELOX_DCHECK(
        keysMayHaveNulls_.has_value() && valuesMayHaveNulls_.has_value(),
        "setChildrenMayHaveNulls() should be called before containsNull()");

    auto decodedIndex = decoded_.index(index);

    return decoded_.isNullAt(index) ||
        (*keysMayHaveNulls_ &&
         keyReader_.containsNull(
             offsets_[decodedIndex],
             offsets_[decodedIndex] + lengths_[decodedIndex])) ||
        (*valuesMayHaveNulls_ &&
         valReader_.containsNull(
             offsets_[decodedIndex],
             offsets_[decodedIndex] + lengths_[decodedIndex]));
  }

  bool containsNull(vector_size_t startIndex, vector_size_t endIndex) const {
    for (auto index = startIndex; index < endIndex; ++index) {
      if (containsNull(index)) {
        return true;
      }
    }

    return false;
  }

  inline bool mayHaveNullsRecursive() const {
    VELOX_DCHECK(
        keysMayHaveNulls_.has_value() && valuesMayHaveNulls_.has_value(),
        "setChildrenMayHaveNulls() should be called before mayHaveNullsRecursive()");
    return decoded_.mayHaveNulls() || *keysMayHaveNulls_ ||
        *valuesMayHaveNulls_;
  }

  void setChildrenMayHaveNulls() {
    keyReader_.setChildrenMayHaveNulls();
    valReader_.setChildrenMayHaveNulls();

    keysMayHaveNulls_ = keyReader_.mayHaveNullsRecursive();
    valuesMayHaveNulls_ = valReader_.mayHaveNullsRecursive();
  }

  const DecodedVector& decoded_;
  const MapVector& vector_;
  DecodedVector decodedKeys_;
  DecodedVector decodedVals_;

  const vector_size_t* offsets_;
  const vector_size_t* lengths_;
  VectorReader<K> keyReader_;
  VectorReader<V> valReader_;

  std::optional<bool> keysMayHaveNulls_;
  std::optional<bool> valuesMayHaveNulls_;
};

template <typename V>
struct VectorReader<Array<V>> {
  using in_vector_t = typename TypeToFlatVector<Array<V>>::type;
  using exec_in_t = typename VectorExec::template resolver<Array<V>>::in_type;
  using exec_null_free_in_t =
      typename VectorExec::template resolver<Array<V>>::null_free_in_type;
  using exec_in_child_t = typename VectorExec::template resolver<V>::in_type;

  explicit VectorReader(const DecodedVector* decoded)
      : decoded_(*decoded),
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        offsets_{vector_.rawOffsets()},
        lengths_{vector_.rawSizes()},
        childReader_{detail::decode(arrayValuesDecoder_, *vector_.elements())} {
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  exec_in_t operator[](size_t offset) const {
    auto index = decoded_.index(offset);
    return {&childReader_, offsets_[index], lengths_[index]};
  }

  exec_null_free_in_t readNullFree(size_t offset) const {
    auto index = decoded_.index(offset);
    return {&childReader_, offsets_[index], lengths_[index]};
  }

  inline bool containsNull(vector_size_t index) const {
    VELOX_DCHECK(
        valuesMayHaveNulls_.has_value(),
        "setChildrenMayHaveNulls() should be called before containsNull()");

    auto decodedIndex = decoded_.index(index);

    return decoded_.isNullAt(index) ||
        (*valuesMayHaveNulls_ &&
         childReader_.containsNull(
             offsets_[decodedIndex],
             offsets_[decodedIndex] + lengths_[decodedIndex]));
  }

  bool containsNull(vector_size_t startIndex, vector_size_t endIndex) const {
    for (auto index = startIndex; index < endIndex; ++index) {
      if (containsNull(index)) {
        return true;
      }
    }

    return false;
  }

  inline bool mayHaveNullsRecursive() const {
    VELOX_DCHECK(
        valuesMayHaveNulls_.has_value(),
        "setChildrenMayHaveNulls() should be called before mayHaveNullsRecursive()");

    return decoded_.mayHaveNulls() || *valuesMayHaveNulls_;
  }

  void setChildrenMayHaveNulls() {
    childReader_.setChildrenMayHaveNulls();

    valuesMayHaveNulls_ = childReader_.mayHaveNullsRecursive();
  }

  DecodedVector arrayValuesDecoder_;
  const DecodedVector& decoded_;
  const ArrayVector& vector_;
  const vector_size_t* offsets_;
  const vector_size_t* lengths_;
  VectorReader<V> childReader_;
  std::optional<bool> valuesMayHaveNulls_;
};

template <typename V>
struct VectorWriter<Array<V>> {
  using vector_t = typename TypeToFlatVector<Array<V>>::type;
  using child_vector_t = typename TypeToFlatVector<V>::type;
  using exec_out_t = ArrayWriter<V>;

  void init(vector_t& vector) {
    arrayVector_ = &vector;
    childWriter_.init(static_cast<child_vector_t&>(*vector.elements().get()));
    writer_.initialize(this);
  }

  // This should be called once all rows are processed.
  void finish() {
    writer_.elementsVector_->resize(writer_.valuesOffset_);
    arrayVector_ = nullptr;
  }

  VectorWriter() = default;

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *arrayVector_;
  }

  void ensureSize(size_t size) {
    if (size > arrayVector_->size()) {
      arrayVector_->resize(size);
      childWriter_.init(
          static_cast<child_vector_t&>(*arrayVector_->elements()));
    }
  }

  // Commit a not null value.
  void commit() {
    arrayVector_->setOffsetAndSize(
        offset_, writer_.valuesOffset_, writer_.length_);
    arrayVector_->setNull(offset_, false);
    // Will reset length to 0 and prepare elementWriter_ for the next item.
    writer_.finalize();
  }

  // Commit a null value.
  void commitNull() {
    writer_.finalizeNull();
    arrayVector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    if (LIKELY(isSet)) {
      commit();
    } else {
      commitNull();
    }
  }

  // Set the index being written.
  void setOffset(vector_size_t offset) {
    offset_ = offset;
  }

  void reset() {
    writer_.valuesOffset_ = 0;
  }

  vector_t* arrayVector_ = nullptr;

  exec_out_t writer_;

  VectorWriter<V> childWriter_;

  // The index being written in the array vector.
  vector_size_t offset_ = 0;
};

template <typename K, typename V>
struct VectorWriter<Map<K, V>> {
  using vector_t = typename TypeToFlatVector<Map<K, V>>::type;
  using key_vector_t = typename TypeToFlatVector<K>::type;
  using val_vector_t = typename TypeToFlatVector<V>::type;
  using exec_out_t = MapWriter<K, V>;

  void init(vector_t& vector) {
    mapVector_ = &vector;
    keyWriter_.init(static_cast<key_vector_t&>(*vector.mapKeys()));
    valWriter_.init(static_cast<val_vector_t&>(*vector.mapValues()));
    writer_.initialize(this);
  }

  // This should be called once all rows are processed.
  void finish() {
    // Downsize to actual used size.
    writer_.keysVector_->resize(writer_.innerOffset_);
    writer_.valuesVector_->resize(writer_.innerOffset_);
    mapVector_ = nullptr;
  }

  VectorWriter() = default;

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *mapVector_;
  }

  void ensureSize(size_t size) {
    if (size > mapVector_->size()) {
      mapVector_->resize(size);
      init(vector());
    }
  }

  // Commit a not null value.
  void commit() {
    mapVector_->setOffsetAndSize(
        offset_, writer_.innerOffset_, writer_.length_);
    mapVector_->setNull(offset_, false);
    // Will reset length to 0 and prepare proxy_.valuesOffset_ for the next
    // item.
    writer_.finalize();
  }

  // Commit a null value.
  void commitNull() {
    writer_.finalizeNull();
    mapVector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    if (LIKELY(isSet)) {
      commit();
    } else {
      commitNull();
    }
  }

  // Set the index being written.
  void setOffset(vector_size_t offset) {
    offset_ = offset;
  }

  void reset() {
    writer_.innerOffset_ = 0;
  }

  exec_out_t writer_;

  vector_t* mapVector_;
  VectorWriter<K> keyWriter_;
  VectorWriter<V> valWriter_;
  size_t offset_ = 0;
};

template <typename... T>
struct VectorReader<Row<T...>> {
  using in_vector_t = typename TypeToFlatVector<Row<T...>>::type;
  using exec_in_t = typename VectorExec::resolver<Row<T...>>::in_type;
  using exec_null_free_in_t =
      typename VectorExec::template resolver<Row<T...>>::null_free_in_type;

  explicit VectorReader(const DecodedVector* decoded)
      : decoded_(*decoded),
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        childrenDecoders_{vector_.childrenSize()},
        childReaders_{prepareChildReaders(
            vector_,
            std::make_index_sequence<sizeof...(T)>{})} {}

  exec_in_t operator[](size_t offset) const {
    auto index = decoded_.index(offset);
    return {&childReaders_, index};
  }

  exec_null_free_in_t readNullFree(size_t offset) const {
    auto index = decoded_.index(offset);
    return {&childReaders_, index};
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  bool containsNull(vector_size_t index) const {
    if (decoded_.isNullAt(index)) {
      return true;
    }

    bool fieldsContainNull = false;
    auto decodedIndex = decoded_.index(index);
    std::apply(
        [&](const auto&... reader) {
          fieldsContainNull |= (reader->containsNull(decodedIndex) || ...);
        },
        childReaders_);

    return fieldsContainNull;
  }

  bool containsNull(vector_size_t startIndex, vector_size_t endIndex) const {
    for (auto index = startIndex; index < endIndex; ++index) {
      if (containsNull(index)) {
        return true;
      }
    }

    return false;
  }

  inline bool mayHaveNullsRecursive() const {
    return decoded_.mayHaveNullsRecursive();
  }

  void setChildrenMayHaveNulls() {
    std::apply(
        [](auto&... reader) { (reader->setChildrenMayHaveNulls(), ...); },
        childReaders_);
  }

 private:
  template <size_t... I>
  std::tuple<std::unique_ptr<VectorReader<T>>...> prepareChildReaders(
      const in_vector_t& vector,
      std::index_sequence<I...>) {
    return {std::make_unique<VectorReader<T>>(
        detail::decode(childrenDecoders_[I], *vector_.childAt(I)))...};
  }

  const DecodedVector& decoded_;
  const in_vector_t& vector_;
  std::vector<DecodedVector> childrenDecoders_;
  std::tuple<std::unique_ptr<VectorReader<T>>...> childReaders_;
};

template <typename... T>
struct VectorWriter<Row<T...>> {
  using children_types = std::tuple<T...>;
  using vector_t = typename TypeToFlatVector<Row<T...>>::type;
  using exec_out_t = typename VectorExec::resolver<Row<T...>>::out_type;

  VectorWriter() {}

  void init(vector_t& vector) {
    rowVector_ = &vector;
    // Ensure that children vectors are at least of same size as top row vector.
    initVectorWriters<0>();
    resizeVectorWriters<0>(rowVector_->size());
    writer_.initialize();
  }

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *rowVector_;
  }

  void ensureSize(size_t size) {
    if (size > rowVector_->size()) {
      rowVector_->resize(size, /*setNotNull*/ false);
      resizeVectorWriters<0>(rowVector_->size());
    }
  }

  void commitNull() {
    rowVector_->setNull(writer_.offset_, true);
  }

  void commit(bool isSet = true) {
    VELOX_DCHECK(rowVector_->size() > writer_.offset_);

    if (LIKELY(isSet)) {
      rowVector_->setNull(writer_.offset_, false);
      writer_.finalize();
    } else {
      commitNull();
    }
  }

  void setOffset(size_t offset) {
    writer_.offset_ = offset;
  }

  void reset() {
    writer_.offset_ = 0;
  }

 private:
  template <size_t I>
  void resizeVectorWriters(size_t size) {
    if constexpr (I == sizeof...(T)) {
      return;
    } else {
      std::get<I>(writer_.childrenWriters_).ensureSize(size);
      resizeVectorWriters<I + 1>(size);
    }
  }

  template <size_t I>
  void initVectorWriters() {
    if constexpr (I == sizeof...(T)) {
      return;
    } else {
      using type = std::tuple_element_t<I, children_types>;
      std::get<I>(writer_.childrenWriters_)
          .init(static_cast<typename TypeToFlatVector<type>::type&>(
              *rowVector_->childAt(I).get()));

      initVectorWriters<I + 1>();
    }
  }

  RowWriter<T...> writer_{};
  vector_t* rowVector_ = nullptr;
};

template <typename T>
struct VectorReader<Variadic<T>> {
  using exec_in_t = typename VectorExec::resolver<Variadic<T>>::in_type;
  using exec_null_free_in_t =
      typename VectorExec::template resolver<Variadic<T>>::null_free_in_type;

  explicit VectorReader(const DecodedArgs& decodedArgs, int32_t startPosition)
      : childReaders_{prepareChildReaders(decodedArgs, startPosition)} {}

  exec_in_t operator[](vector_size_t offset) const {
    return {&childReaders_, offset};
  }

  exec_null_free_in_t readNullFree(vector_size_t offset) const {
    return {&childReaders_, offset};
  }

  bool isSet(size_t /*unused*/) const {
    // The Variadic itself can never be null, only the values of the underlying
    // Types
    return true;
  }

  bool containsNull(vector_size_t index) const {
    for (const auto& childReader : childReaders_) {
      if (childReader->containsNull(index)) {
        return true;
      }
    }

    return false;
  }

  bool containsNull(vector_size_t startIndex, vector_size_t endIndex) const {
    for (const auto& childReader : childReaders_) {
      if (childReader->containsNull(startIndex, endIndex)) {
        return true;
      }
    }

    return false;
  }

  inline bool mayHaveNullsRecursive() const {
    for (const auto& childReader : childReaders_) {
      if (childReader->mayHaveNullsRecursive()) {
        return true;
      }
    }

    return false;
  }

  void setChildrenMayHaveNulls() {
    for (auto& childReader : childReaders_) {
      childReader->setChildrenMayHaveNulls();
    }
  }

 private:
  std::vector<std::unique_ptr<VectorReader<T>>> prepareChildReaders(
      const DecodedArgs& decodedArgs,
      int32_t startPosition) {
    std::vector<std::unique_ptr<VectorReader<T>>> childReaders;
    childReaders.reserve(decodedArgs.size() - startPosition);

    for (int i = startPosition; i < decodedArgs.size(); ++i) {
      childReaders.emplace_back(
          std::make_unique<VectorReader<T>>(decodedArgs.at(i)));
    }

    return childReaders;
  }

  std::vector<std::unique_ptr<VectorReader<T>>> childReaders_;
};

template <>
class StringWriter<false /*reuseInput*/> : public UDFOutputString {
 public:
  StringWriter() : vector_(nullptr), offset_(-1) {}

  // Used to initialize top-level strings and allow zero-copy writes.
  StringWriter(FlatVector<StringView>* vector, int32_t offset)
      : vector_(vector), offset_(offset) {}

  // Used to initialize nested strings and requires a copy on write.
  /* implicit */ StringWriter(StringView value)
      : vector_(nullptr), offset_(-1), value_{value.str()} {}

  // Returns true if initialized for zero-copy write. False, otherwise.
  bool initialized() const {
    return offset_ >= 0;
  }

  // If not initialized for zero-copy write, returns a string to copy into the
  // target vector on commit.
  const std::string& value() const {
    return value_;
  }

  /// Reserve a space for the output string with size of at least newCapacity
  void reserve(size_t newCapacity) override {
    if (newCapacity <= capacity()) {
      return;
    }

    auto* newDataBuffer = vector_->getBufferWithSpace(newCapacity);
    // If the new allocated space is on the same buffer no need to copy content
    // or reassign start address
    if (dataBuffer_ == newDataBuffer) {
      setCapacity(newCapacity);
      return;
    }

    auto newStartAddress =
        newDataBuffer->asMutable<char>() + newDataBuffer->size();

    if (size() != 0) {
      std::memcpy(newStartAddress, data(), size());
    }

    setCapacity(newCapacity);
    setData(newStartAddress);
    dataBuffer_ = newDataBuffer;
  }

  /// Not called by the UDF Implementation. Should be called at the end to
  /// finalize the allocation and the string writing
  void finalize() {
    if (!finalized_) {
      VELOX_CHECK(size() == 0 || data());
      if (dataBuffer_) {
        dataBuffer_->setSize(dataBuffer_->size() + size());
      }
      vector_->setNoCopy(offset_, StringView(data(), size()));
    }
  }

  void setEmpty() {
    static const StringView kEmpty("");
    vector_->setNoCopy(offset_, kEmpty);
    finalized_ = true;
  }

  void setNoCopy(const StringView& value) {
    vector_->setNoCopy(offset_, value);
    finalized_ = true;
  }

  template <typename T>
  void operator+=(const T& input) {
    append(input);
  }

  void operator+=(const char* input) {
    append(std::string_view(input));
  }

  template <typename T>
  void append(const T& input) {
    DCHECK(!finalized_);
    auto oldSize = size();
    resize(this->size() + input.size());
    if (input.size() != 0) {
      DCHECK(data());
      DCHECK(input.data());
      std::memcpy(data() + oldSize, input.data(), input.size());
    }
  }

  void append(const char* input) {
    append(std::string_view(input));
  }

  template <typename T>
  void copy_from(const T& input) {
    VELOX_DCHECK(initialized());
    append(input);
  }

  void copy_from(const char* input) {
    append(std::string_view(input));
  }

 private:
  bool finalized_{false};

  /// The buffer that the output string uses for its allocation set during
  /// reserve() call
  Buffer* dataBuffer_ = nullptr;

  FlatVector<StringView>* vector_;

  int32_t offset_;

  std::string value_;
};

// A string writer with UDFOutputString semantics that utilizes a pre-allocated
// input string for the output allocation, if inPlace is true in the constructor
// the string will be initialized with the input string value.
template <>
class StringWriter<true /*reuseInput*/> : public UDFOutputString {
 public:
  StringWriter() : vector_(nullptr), offset_(-1) {}

  StringWriter(
      FlatVector<StringView>* vector,
      int32_t offset,
      const StringView& stringToReuse,
      bool inPlace = false)
      : vector_(vector), offset_(offset), stringToReuse_(stringToReuse) {
    setData(const_cast<char*>(stringToReuse_.data()));
    setCapacity(stringToReuse_.size());

    if (inPlace) {
      // The string should be intialized with the input value
      setSize(stringToReuse_.size());
    }
  }

  void reserve(size_t newCapacity) override {
    VELOX_CHECK(
        newCapacity <= capacity() && "String writer max capacity extended");
  }

  /// Not called by the UDF Implementation. Should be called at the end to
  /// finalize the allocation and the string writing
  void finalize() {
    VELOX_CHECK(size() == 0 || data());
    vector_->setNoCopy(offset_, StringView(data(), size()));
  }

 private:
  /// The output vector that this string is being written to
  FlatVector<StringView>* vector_;

  /// The offset the string writes to within vector_
  int32_t offset_;

  /// The input string that is reused, held locally to assert the validity of
  /// the data pointer throughout the proxy lifetime. More specifically when
  /// the string is inlined.
  StringView stringToReuse_;
};

template <typename T>
struct VectorWriter<
    T,
    std::enable_if_t<
        std::is_same_v<T, Varchar> | std::is_same_v<T, Varbinary>>> {
  using vector_t = typename TypeToFlatVector<T>::type;
  using exec_out_t = StringWriter<>;

  void init(vector_t& vector) {
    vector_ = &vector;
  }

  void ensureSize(size_t size) {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
    }
  }

  VectorWriter() {}

  exec_out_t& current() {
    proxy_ = exec_out_t(vector_, offset_);
    return proxy_;
  }

  void copyCommit(const exec_out_t& data) {
    // If not initialized for zero-copy writes, copy the value into the vector.
    if (!proxy_.initialized()) {
      vector_->set(offset_, StringView(data.value()));
    } else {
      // Not really copying.
      proxy_.finalize();
    }
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (isSet) {
      copyCommit(proxy_);
    } else {
      commitNull();
    }
  }

  void setOffset(int32_t offset) {
    offset_ = offset;
  }

  vector_t& vector() {
    return *vector_;
  }

  exec_out_t proxy_;
  vector_t* vector_;
  size_t offset_ = 0;
};

template <typename T>
struct VectorWriter<T, std::enable_if_t<std::is_same_v<T, bool>>> {
  using vector_t = typename TypeToFlatVector<T>::type;
  using exec_out_t = bool;

  void init(vector_t& vector) {
    vector_ = &vector;
  }

  void ensureSize(size_t size) {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
    }
  }

  VectorWriter() {}

  bool& current() {
    return proxy_;
  }

  void copyCommit(const bool& data) {
    vector_->set(offset_, data);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (isSet) {
      copyCommit(proxy_);
    } else {
      commitNull();
    }
  }

  void setOffset(int32_t offset) {
    offset_ = offset;
  }

  vector_t& vector() {
    return *vector_;
  }

  bool proxy_;
  vector_t* vector_;
  size_t offset_ = 0;
};

template <typename T>
struct VectorWriter<std::shared_ptr<T>> {
  using exec_out_t =
      typename VectorExec::template resolver<std::shared_ptr<T>>::out_type;
  using vector_t = typename TypeToFlatVector<std::shared_ptr<void>>::type;

  void init(vector_t& vector) {
    vector_ = &vector;
  }

  void ensureSize(size_t size) {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
    }
  }

  vector_t& vector() {
    return *vector_;
  }

  VectorWriter() {}

  exec_out_t& current() {
    return proxy_;
  }

  void copyCommit(const exec_out_t& data) {
    vector_->set(offset_, data);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (isSet) {
      copyCommit(proxy_);
    } else {
      commitNull();
    }
  }

  void setOffset(int32_t offset) {
    offset_ = offset;
  }

  exec_out_t proxy_;
  vector_t* vector_;
  size_t offset_ = 0;
};

template <typename T>
struct VectorReader<Generic<T>> {
  using exec_in_t = GenericView;
  using exec_null_free_in_t = exec_in_t;

  explicit VectorReader(const DecodedVector* decoded) : decoded_(*decoded) {}

  explicit VectorReader(const VectorReader<Generic<T>>&) = delete;

  VectorReader<Generic<T>>& operator=(const VectorReader<Generic<T>>&) = delete;

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  exec_in_t operator[](size_t offset) const {
    auto index = decoded_.index(offset);
    return GenericView{decoded_, castReaders_, castType_, index};
  }

  exec_null_free_in_t readNullFree(vector_size_t offset) const {
    return operator[](offset);
  }

  inline bool containsNull(vector_size_t /* index */) const {
    // This function is only called if callNullFree is defined.
    // TODO (kevinwilfong): Add support for Generics in callNullFree.
    VELOX_UNSUPPORTED(
        "Calling callNullFree with Generic arguments is not yet supported.");
  }

  bool containsNull(
      vector_size_t /* startIndex */,
      vector_size_t /* endIndex */) const {
    // This function is only called if callNullFree is defined.
    // TODO (kevinwilfong): Add support for Generics in callNullFree.
    VELOX_UNSUPPORTED(
        "Calling callNullFree with Generic arguments is not yet supported.");
  }

  inline bool mayHaveNullsRecursive() const {
    // This function is only called if callNullFree is defined.
    // TODO (kevinwilfong): Add support for Generics in callNullFree.
    VELOX_UNSUPPORTED(
        "Calling callNullFree with Generic arguments is not yet supported.");
  }

  inline void setChildrenMayHaveNulls() {
    // This function is only called if callNullFree is defined.
    // TODO (kevinwilfong): Add support for Generics in callNullFree.
    VELOX_UNSUPPORTED(
        "Calling callNullFree with Generic arguments is not yet supported.");
  }

  const DecodedVector& decoded_;

  // Those two variables are mutated by the GenericView during cast operations,
  // and are shared across GenericViews constructed by the reader.
  mutable std::array<std::shared_ptr<void>, 3> castReaders_;
  mutable TypePtr castType_ = nullptr;
};

} // namespace facebook::velox::exec
