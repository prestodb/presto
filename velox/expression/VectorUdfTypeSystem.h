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

#include <algorithm>
#include <cstring>
#include <string_view>
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/ComplexViewTypes.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VariadicView.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

template <typename VectorType = FlatVector<StringView>, bool reuseInput = false>
class StringProxy;

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
  using in_type = MapView<K, V>;
  using out_type = core::SlowMapWriter<
      typename resolver<K>::out_type,
      typename resolver<V>::out_type>;
};

template <typename... T>
struct resolver<Row<T...>> {
  using in_type = RowView<T...>;
  using out_type = core::RowWriter<typename resolver<T>::out_type...>;
};

template <typename V>
struct resolver<Array<V>> {
  using in_type = ArrayView<V>;
  using out_type = core::ArrayValWriter<typename resolver<V>::out_type>;
};

template <>
struct resolver<Varchar> {
  using in_type = StringView;
  using out_type = StringProxy<>;
};

template <>
struct resolver<Varbinary> {
  using in_type = StringView;
  using out_type = StringProxy<>;
};

template <typename T>
struct resolver<std::shared_ptr<T>> {
  using in_type = std::shared_ptr<T>;
  using out_type = std::shared_ptr<T>;
};

template <typename T>
struct resolver<Variadic<T>> {
  using in_type = VariadicView<T>;
  // Variadic cannot be used as an out_type
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
      init(*vector_);
    }
  }

  VectorWriter() {}

  exec_out_t& current() {
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

  void setOffset(int32_t offset) {
    offset_ = offset;
  }

  vector_t& vector() {
    return *vector_;
  }

  vector_t* vector_;
  exec_out_t* data_;
  size_t offset_ = 0;
};

template <typename T>
struct VectorReader {
  using exec_in_t = typename VectorExec::template resolver<T>::in_type;

  explicit VectorReader(const DecodedVector* decoded) : decoded_(*decoded) {}

  explicit VectorReader(const VectorReader<T>&) = delete;
  VectorReader<T>& operator=(const VectorReader<T>&) = delete;

  exec_in_t operator[](size_t offset) const {
    return decoded_.template valueAt<exec_in_t>(offset);
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  bool mayHaveNulls() const {
    return decoded_.mayHaveNulls();
  }

  const DecodedVector& decoded_;
};

template <typename K, typename V>
struct VectorWriter<Map<K, V>> {
  using vector_t = typename TypeToFlatVector<Map<K, V>>::type;
  using key_vector_t = typename TypeToFlatVector<K>::type;
  using val_vector_t = typename TypeToFlatVector<V>::type;
  using exec_out_t =
      typename VectorExec::template resolver<Map<K, V>>::out_type;

  void init(vector_t& vector) {
    vector_ = &vector;
    keyWriter_.init(static_cast<key_vector_t&>(*vector.mapKeys()));
    valWriter_.init(static_cast<val_vector_t&>(*vector.mapValues()));
  }

  VectorWriter() {}

  exec_out_t& current() {
    // todo(youknowjack): support writing directly into a slice
    return m_;
  }

  vector_t& vector() {
    return *vector_;
  }

  void ensureSize(size_t size) {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
      init(*vector_);
    }
  }

  void copyCommit(const exec_out_t& data) {
    vector_size_t childSize = keyWriter_.vector().size();
    auto newSize = childSize + data.size();
    keyWriter_.ensureSize(newSize);
    valWriter_.ensureSize(newSize);
    vector_->setOffsetAndSize(offset_, childSize, data.size());

    for (auto& pair : data) {
      keyWriter_.setOffset(childSize);
      valWriter_.setOffset(childSize);
      keyWriter_.copyCommit(pair.first);
      if (pair.second.has_value()) {
        valWriter_.copyCommit(pair.second.value());
      } else {
        valWriter_.commitNull();
      }
      ++childSize;
    }
    vector_->setNull(offset_, false);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    if (LIKELY(isSet)) {
      copyCommit(m_);
    } else {
      commitNull();
    }
    m_.clear();
  }

  void setOffset(size_t offset) {
    offset_ = offset;
  }

  void reset() {
    offset_ = 0;
    m_.clear();
    keyWriter_.reset();
    valWriter_.reset();
  }

  vector_t* vector_;
  exec_out_t m_{};
  VectorWriter<K> keyWriter_;
  VectorWriter<V> valWriter_;
  size_t offset_ = 0;
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
    return MapView{&keyReader_, &valReader_, offsets_[index], lengths_[index]};
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  const DecodedVector& decoded_;
  const MapVector& vector_;
  DecodedVector decodedKeys_;
  DecodedVector decodedVals_;

  const vector_size_t* offsets_;
  const vector_size_t* lengths_;
  VectorReader<K> keyReader_;
  VectorReader<V> valReader_;
};

template <typename V>
struct VectorReader<Array<V>> {
  using in_vector_t = typename TypeToFlatVector<Array<V>>::type;
  using child_vector_t = typename TypeToFlatVector<V>::type;
  using exec_in_t = typename VectorExec::template resolver<Array<V>>::in_type;
  using exec_in_child_t = typename VectorExec::template resolver<V>::in_type;

  explicit VectorReader(const DecodedVector* decoded)
      : decoded_(*decoded),
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        offsets_{vector_.rawOffsets()},
        lengths_{vector_.rawSizes()},
        childReader_{detail::decode(arrayValuesDecoder_, *vector_.elements())} {
  }

  explicit VectorReader(const VectorReader<Array<V>>&) = delete;
  VectorReader<Array<V>>& operator=(const VectorReader<Array<V>>&) = delete;

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  exec_in_t operator[](size_t offset) const {
    auto index = decoded_.index(offset);
    return ArrayView{&childReader_, offsets_[index], lengths_[index]};
  }

  DecodedVector arrayValuesDecoder_;
  const DecodedVector& decoded_;
  const ArrayVector& vector_;
  const vector_size_t* offsets_;
  const vector_size_t* lengths_;
  VectorReader<V> childReader_;
};

template <typename V>
struct VectorWriter<Array<V>> {
  using vector_t = typename TypeToFlatVector<Array<V>>::type;
  using child_vector_t = typename TypeToFlatVector<V>::type;
  using exec_out_t = typename VectorExec::template resolver<Array<V>>::out_type;

  void init(vector_t& vector) {
    vector_ = &vector;
    childWriter_.init(static_cast<child_vector_t&>(*vector.elements().get()));
  }

  VectorWriter() {}

  exec_out_t& current() {
    // todo(youknowjack): support writing directly into a slice
    return m_;
  }

  vector_t& vector() {
    return *vector_;
  }

  void ensureSize(size_t size) {
    // todo(youknowjack): optimize the excessive ensureSize calls
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
      init(*vector_);
    }
  }

  void copyCommit(const exec_out_t& data) {
    vector_size_t childSize = childWriter_.vector().size();
    childWriter_.ensureSize(childSize + data.size());
    vector_->setOffsetAndSize(offset_, childSize, data.size());
    for (auto& val : data) {
      childWriter_.setOffset(childSize);
      ++childSize;
      if (val.has_value()) {
        childWriter_.copyCommit(val.value());
      } else {
        childWriter_.commitNull();
      }
    }
    vector_->setNull(offset_, false);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    if (LIKELY(isSet)) {
      copyCommit(m_);
    } else {
      commitNull();
    }
    m_.clear();
  }

  void setOffset(int32_t offset) {
    offset_ = offset;
  }

  void reset() {
    offset_ = 0;
    m_.clear();
    childWriter_.reset();
  }

  vector_t* vector_ = nullptr;
  exec_out_t m_{};

  VectorWriter<V> childWriter_;
  size_t offset_ = 0;
};

template <typename... T>
struct VectorReader<Row<T...>> {
  using in_vector_t = typename TypeToFlatVector<Row<T...>>::type;
  using exec_in_t = typename VectorExec::resolver<Row<T...>>::in_type;

  explicit VectorReader(const DecodedVector* decoded)
      : decoded_(*decoded),
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        childrenDecoders_{vector_.childrenSize()},
        childReaders_{prepareChildReaders(
            vector_,
            std::make_index_sequence<sizeof...(T)>{})} {}

  exec_in_t operator[](size_t offset) const {
    auto index = decoded_.index(offset);
    return RowView{&childReaders_, index};
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
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
  using vector_t = typename TypeToFlatVector<Row<T...>>::type;
  using exec_out_t = typename VectorExec::resolver<Row<T...>>::out_type;

  VectorWriter() {}

  void init(vector_t& vector) {
    vector_ = &vector;
    // Ensure that children vectors are at least of same size as top row vector.
    initVectorWritersInternal<0, T...>();
    resizeVectorWritersInternal<0>(vector_->size());
  }

  exec_out_t& current() {
    // todo(youknowjack): support writing directly into a slice
    return execOut_;
  }

  vector_t& vector() {
    return *vector_;
  }

  void ensureSize(size_t size) {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
      resizeVectorWritersInternal<0>(vector_->size());
    }
  }

  void copyCommit(const exec_out_t& data) {
    copyCommitInternal<0>(data);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
    commitNullInternal<0>();
  }

  void commit(bool isSet) {
    ensureSize(offset_ + 1);
    if (LIKELY(isSet)) {
      copyCommit(execOut_);
    } else {
      commitNull();
    }
    execOut_.clear();
  }

  void setOffset(size_t offset) {
    offset_ = offset;
    setOffsetVectorWritersInternal<0>(offset);
  }

  void reset() {
    offset_ = 0;
    execOut_.clear();
    resetVectorWritersInternal<0>();
  }

 private:
  template <size_t N, typename Type, typename... Types>
  void initVectorWritersInternal() {
    std::get<N>(writers_).init(
        static_cast<typename TypeToFlatVector<Type>::type&>(
            *vector_->childAt(N).get()));

    if constexpr (sizeof...(Types) > 0) {
      initVectorWritersInternal<N + 1, Types...>();
    }
  }

  template <size_t N>
  void resetVectorWritersInternal() {
    std::get<N>(writers_).reset();
    if constexpr (N + 1 < sizeof...(T)) {
      resetVectorWritersInternal<N + 1>();
    }
  }

  template <size_t N>
  void resizeVectorWritersInternal(size_t size) {
    std::get<N>(writers_).ensureSize(size);
    if constexpr (N + 1 < sizeof...(T)) {
      resizeVectorWritersInternal<N + 1>(size);
    }
  }

  template <size_t N>
  void setOffsetVectorWritersInternal(size_t offset) {
    std::get<N>(writers_).setOffset(offset);
    if constexpr (N + 1 < sizeof...(T)) {
      setOffsetVectorWritersInternal<N + 1>(offset);
    }
  }

  template <size_t N>
  void copyCommitInternal(const exec_out_t& data) {
    const auto& value = data.template at<N>();
    if (value.has_value()) {
      std::get<N>(writers_).copyCommit(value.value());
    } else {
      std::get<N>(writers_).commitNull();
    }

    if constexpr (N + 1 < sizeof...(T)) {
      copyCommitInternal<N + 1>(data);
    }
  }

  template <size_t N>
  void commitNullInternal() {
    std::get<N>(writers_).commitNull();
    if constexpr (N + 1 < sizeof...(T)) {
      commitNullInternal<N + 1>();
    }
  }

  vector_t* vector_ = nullptr;
  std::tuple<VectorWriter<T>...> writers_;
  exec_out_t execOut_{};
  size_t offset_ = 0;
};

template <typename T>
struct VectorReader<Variadic<T>> {
  using in_vector_t = typename TypeToFlatVector<T>::type;
  using exec_in_t = VariadicView<T>;

  explicit VectorReader(const DecodedArgs& decodedArgs, int32_t startPosition)
      : childReaders_{prepareChildReaders(decodedArgs, startPosition)} {}

  exec_in_t operator[](vector_size_t offset) const {
    return VariadicView<T>{&childReaders_, offset};
  }

  bool isSet(size_t /*unused*/) const {
    // The Variadic itself can never be null, only the values of the underlying
    // Types
    return true;
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
class StringProxy<FlatVector<StringView>, false /*reuseInput*/>
    : public UDFOutputString {
 public:
  StringProxy() : vector_(nullptr), offset_(-1) {}

  // Used to initialize top-level strings and allow zero-copy writes.
  StringProxy(FlatVector<StringView>* vector, int32_t offset)
      : vector_(vector), offset_(offset) {}

  // Used to initialize nested strings and requires a copy on write.
  explicit StringProxy(StringView value)
      : vector_(nullptr), offset_(-1), value_{value.str()} {}

 public:
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

  void append(const StringView& input) {
    DCHECK(!finalized_);
    auto oldSize = size();
    resize(this->size() + input.size());
    if (input.size() != 0) {
      DCHECK(data());
      DCHECK(input.data());
      std::memcpy(data() + oldSize, input.data(), input.size());
    }
  }

  void operator+=(const StringView& input) {
    append(input);
  }

  void append(const std::string_view input) {
    DCHECK(!finalized_);
    auto oldSize = size();
    resize(this->size() + input.size());
    if (input.size() != 0) {
      DCHECK(data());
      DCHECK(input.data());
      std::memcpy(data() + oldSize, input.data(), input.size());
    }
  }

  void operator+=(const std::string_view input) {
    append(input);
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

// A string proxy with UDFOutputString semantics that utilizes a pre-allocated
// input string for the output allocation, if inPlace is true in the constructor
// the string will be initialized with the input string value.
template <>
class StringProxy<FlatVector<StringView>, true /*reuseInput*/>
    : public UDFOutputString {
 public:
  StringProxy() : vector_(nullptr), offset_(-1) {}

  StringProxy(
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
        newCapacity <= capacity() && "String proxy max capacity extended");
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
  using proxy_t = StringProxy<FlatVector<StringView>, false>;

  void init(vector_t& vector) {
    vector_ = &vector;
  }

  void ensureSize(size_t size) {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
    }
  }

  VectorWriter() {}

  proxy_t& current() {
    proxy_ = proxy_t(vector_, offset_);
    return proxy_;
  }

  void copyCommit(const proxy_t& data) {
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

  proxy_t proxy_;
  vector_t* vector_;
  size_t offset_ = 0;
};

template <typename T>
struct VectorWriter<T, std::enable_if_t<std::is_same_v<T, bool>>> {
  using vector_t = typename TypeToFlatVector<T>::type;

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

} // namespace facebook::velox::exec
