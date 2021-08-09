/*
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

#include "velox/core/CoreTypeSystem.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/type/Type.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

template <typename VectorType = FlatVector<StringView>, bool reuseInput = false>
class StringProxy;

namespace detail {
template <typename T>
struct resolver {
  using in_type = typename CppToType<T>::NativeType;
  using out_type = typename CppToType<T>::NativeType;

  static variant toVariant(const in_type& t) {
    return variant{t};
  }

  static in_type fromVariant(const variant& v) {
    return v.value<in_type>();
  }
};

template <typename K, typename V>
struct resolver<Map<K, V>> {
  using in_type = core::
      SlowMapVal<typename resolver<K>::in_type, typename resolver<V>::in_type>;
  using out_type = core::SlowMapWriter<
      typename resolver<K>::out_type,
      typename resolver<V>::out_type>;

  static variant toVariant(const in_type& t) {
    VELOX_NYI();
  }

  static variant toVariant(const out_type& t) {
    VELOX_NYI();
  }

  static in_type fromVariant(const variant& v) {
    VELOX_NYI();
  }
};

template <typename... T>
struct resolver<Row<T...>> {
  using in_type = core::RowReader<typename resolver<T>::in_type...>;
  using out_type = core::RowWriter<typename resolver<T>::out_type...>;

  static variant toVariant(const in_type& t) {
    VELOX_NYI();
  }

  static variant toVariant(const out_type& t) {
    VELOX_NYI();
  }

  static in_type fromVariant(const variant& t) {
    VELOX_NYI();
  }
};

template <typename V>
struct resolver<Array<V>> {
  using in_type = core::ArrayValReader<typename resolver<V>::in_type>;
  using out_type = core::ArrayValWriter<typename resolver<V>::out_type>;

  static variant toVariant(const in_type& t) {
    using childType = typename resolver<V>::in_type;
    std::vector<variant> v(t.size());
    std::transform(
        t.begin(), t.end(), v.begin(), [](const folly::Optional<childType>& v) {
          return v.hasValue()
              ? resolver<childType>::toVariant(v)
              : variant::null(in_type::koskiType()->childAt(0)->kind());
        });

    return variant::array(std::move(v));
  }

  static variant toVariant(const out_type& t) {
    VELOX_NYI();
  }

  static in_type fromVariant(const variant& t) {
    using childType = typename resolver<V>::in_type;
    VELOX_CHECK_EQ(t.kind(), TypeKind::ARRAY);
    const auto& values = t.array();
    in_type retVal;
    for (auto& v : values) {
      auto convertedVal = v.isNull()
          ? folly::Optional<childType>{}
          : folly::Optional<childType>{resolver<V>::fromVariant(v)};
      retVal.append(std::move(convertedVal));
    }

    return retVal;
  }
};

template <>
struct resolver<Varchar> {
  using in_type = StringView;
  using out_type = StringProxy<>;

  static variant toVariant(const in_type& t) {
    VELOX_NYI();
  }

  static variant toVariant(const out_type& t) {
    VELOX_NYI();
  }

  static in_type fromVariant(const variant& v) {
    VELOX_NYI();
  }
};

template <>
struct resolver<Varbinary> {
  using in_type = StringView;
  using out_type = StringProxy<>;

  static variant toVariant(const in_type& t) {
    VELOX_NYI();
  }

  static variant toVariant(const out_type& t) {
    VELOX_NYI();
  }

  static in_type fromVariant(const variant& v) {
    VELOX_NYI();
  }
};

template <typename T>
struct resolver<std::shared_ptr<T>> {
  using in_type = std::shared_ptr<T>;
  using out_type = std::shared_ptr<T>;

  static variant toVariant(const in_type& t) {
    return variant::opaque(t);
  }

  static in_type fromVariant(const variant& v) {
    return v.opaque<T>();
  }
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
    if (size != vector_->size()) {
      vector_->resize(size);
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
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (!isSet) {
      vector_->setNull(offset_, true);
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

template <typename T, typename = void>
struct VectorReader {
  using exec_in_t = typename VectorExec::template resolver<T>::in_type;

  VectorReader(const DecodeResult<exec_in_t>& decoded) : decoded_{decoded} {}

  const exec_in_t& operator[](size_t offset) const {
    return decoded_[offset];
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  bool doLoad(size_t offset, exec_in_t& v) const {
    if (isSet(offset)) {
      v = decoded_[offset];
      return true;
    } else {
      return false;
    }
  }

  const DecodeResult<exec_in_t> decoded_;
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
    if (size != vector_->size()) {
      vector_->resize(size);
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
      if (pair.second.hasValue()) {
        valWriter_.copyCommit(pair.second.value());
      } else {
        valWriter_.commitNull();
      }
      ++childSize;
    }
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

template <typename TOut, typename TIn>
const TOut& getDecoded(const DecodeResult<TIn>& decoded) {
  auto base = decoded.base();
  return *base->template as<TOut>();
}

template <typename T>
DecodeResult<T> decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return decoder.as<T>();
}

}; // namespace detail

template <typename K, typename V>
struct VectorReader<Map<K, V>> {
  // todo(youknowjack): in future, directly expose the slice data

  using in_vector_t = typename TypeToFlatVector<Map<K, V>>::type;
  using key_vector_t = typename TypeToFlatVector<K>::type;
  using val_vector_t = typename TypeToFlatVector<V>::type;
  using exec_in_t = typename VectorExec::template resolver<Map<K, V>>::in_type;
  using exec_in_key_t = typename VectorExec::template resolver<K>::in_type;
  using exec_in_val_t = typename VectorExec::template resolver<V>::in_type;

  explicit VectorReader(const DecodeResult<exec_in_t>& decoded)
      : decoded_{decoded},
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        offsets_(vector_.rawOffsets()),
        lengths_(vector_.rawSizes()),
        keyReader_{
            detail::decode<exec_in_key_t>(decodedKeys_, *vector_.mapKeys())},
        valReader_{
            detail::decode<exec_in_val_t>(decodedVals_, *vector_.mapValues())} {
  }

  bool doLoad(size_t outerOffset, exec_in_t& target) const {
    if (UNLIKELY(!isSet(outerOffset))) {
      return false;
    }
    vector_size_t offset = decoded_.decodedIndex(outerOffset);
    const size_t offsetStart = offsets_[offset];
    const size_t offsetEnd = offsetStart + lengths_[offset];

    target.reserve(target.size() + offsetEnd - offsetStart);

    for (size_t i = offsetStart; i != offsetEnd; ++i) {
      exec_in_key_t key{};
      exec_in_val_t val{};
      if (UNLIKELY(!keyReader_.doLoad(i, key))) {
        VELOX_USER_CHECK(false, "null map key not allowed");
      }
      const bool isSet = valReader_.doLoad(i, val);
      if (LIKELY(isSet)) {
        target.emplace(key, folly::Optional<exec_in_val_t>{std::move(val)});
      } else {
        target.emplace(key, folly::Optional<exec_in_val_t>{});
      }
    }
    return true;
  }

  // note: because it uses a cached map; it is only valid until a new offset
  // is fetched!! scary
  exec_in_t& operator[](size_t offset) const {
    m_.clear();
    doLoad(offset, m_);
    return m_;
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  DecodeResult<exec_in_t> decoded_;
  const MapVector& vector_;
  DecodedVector decodedKeys_;
  DecodedVector decodedVals_;
  mutable exec_in_t m_{};

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

  explicit VectorReader(const DecodeResult<exec_in_t>& decoded)
      : decoded_(decoded),
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        offsets_{vector_.rawOffsets()},
        lengths_{vector_.rawSizes()},
        childReader_{
            detail::decode<exec_in_child_t>(decoder_, *vector_.elements())} {}

  bool doLoad(size_t outerOffset, exec_in_t& results) const {
    if (!isSet(outerOffset)) {
      return false;
    }
    vector_size_t offset = decoded_.decodedIndex(outerOffset);
    const size_t offsetStart = offsets_[offset];
    const size_t offsetEnd = offsetStart + lengths_[offset];

    results.reserve(results.size() + offsetEnd - offsetStart);

    for (size_t i = offsetStart; i < offsetEnd; ++i) {
      exec_in_child_t childval{};
      if (childReader_.doLoad(i, childval)) {
        results.append(std::move(childval));
      } else {
        results.appendNullable();
      }
    }

    return true;
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

  exec_in_t& operator[](size_t offset) const {
    returnval_.clear();
    doLoad(offset, returnval_);
    return returnval_;
  }

  DecodedVector decoder_;
  DecodeResult<exec_in_t> decoded_;
  const ArrayVector& vector_;
  const vector_size_t* offsets_;
  const vector_size_t* lengths_;
  VectorReader<V> childReader_;
  mutable exec_in_t returnval_;
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
    if (size != vector_->size()) {
      vector_->resize(size);
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
      if (val.hasValue()) {
        childWriter_.copyCommit(val.value());
      } else {
        childWriter_.commitNull();
      }
    }
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

  explicit VectorReader(const DecodeResult<exec_in_t>& decoded)
      : decoded_(decoded),
        vector_(detail::getDecoded<in_vector_t>(decoded_)),
        decoders_{vector_.childrenSize()},
        childReader_{prepareChildReaders(
            vector_,
            std::make_index_sequence<sizeof...(T)>{})} {}

  exec_in_t& operator[](size_t offset) const {
    returnval_.clear();
    doLoad(offset, returnval_);
    return returnval_;
  }

  bool isSet(size_t offset) const {
    return !decoded_.isNullAt(offset);
  }

 private:
  template <size_t... I>
  std::tuple<VectorReader<T>...> prepareChildReaders(
      const in_vector_t& vector,
      std::index_sequence<I...>) {
    return {childReader<T, I>()...};
  }

  template <typename CHILD_T, size_t I>
  VectorReader<CHILD_T> childReader() {
    auto& decoder = decoders_[I];
    return VectorReader<CHILD_T>(
        detail::decode<
            typename VectorExec::template resolver<CHILD_T>::in_type>(
            decoder, *vector_.childAt(I)));
  }

  bool doLoad(size_t offset, exec_in_t& results) const {
    if (!isSet(offset)) {
      return false;
    }

    doLoadInternal<0, T...>(offset, results);
    return true;
  }

  template <size_t N, typename Type, typename... Types>
  void doLoadInternal(size_t offset, exec_in_t& results) const {
    using exec_child_in_t = typename VectorExec::resolver<Type>::in_type;

    exec_child_in_t childval{};

    if (std::get<N>(childReader_).doLoad(offset, childval)) {
      results.template at<N>() = childval;
    }

    if constexpr (sizeof...(Types) > 0) {
      doLoadInternal<N + 1, Types...>(offset, results);
    }
  }

  DecodeResult<exec_in_t> decoded_;
  const in_vector_t& vector_;
  std::vector<DecodedVector> decoders_;
  std::tuple<VectorReader<T>...> childReader_;
  mutable exec_in_t returnval_;
};

template <typename... T>
struct VectorWriter<Row<T...>> {
  using vector_t = typename TypeToFlatVector<Row<T...>>::type;
  using exec_out_t = typename VectorExec::resolver<Row<T...>>::out_type;

  VectorWriter() {}

  void init(vector_t& vector) {
    vector_ = &vector;
    initVectorWritersInternal<0, T...>();
  }

  exec_out_t& current() {
    // todo(youknowjack): support writing directly into a slice
    return execOut_;
  }

  vector_t& vector() {
    return *vector_;
  }

  void ensureSize(size_t size) {
    if (size != vector_->size()) {
      vector_->resize(size);
      init(*vector_);
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
    if (LIKELY(isSet)) {
      copyCommit(execOut_);
    } else {
      commitNull();
    }
    execOut_.clear();
  }

  void setOffset(size_t offset) {
    offset_ = offset;
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
  };

  template <size_t N>
  void resetVectorWritersInternal() {
    std::get<N>(writers_).reset();
    if constexpr (N + 1 < sizeof...(T)) {
      resetVectorWritersInternal<N + 1>();
    }
  };

  template <size_t N>
  void copyCommitInternal(const exec_out_t& data) {
    const auto& value = data.template at<N>();
    if (value.hasValue()) {
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

template <>
class StringProxy<FlatVector<StringView>, false /*reuseInput*/>
    : public UDFOutputString {
 public:
  StringProxy() : vector_(nullptr), offset_(-1) {}

  StringProxy(FlatVector<StringView>* vector, int32_t offset)
      : vector_(vector), offset_(offset) {}

 public:
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
    VELOX_CHECK(size() == 0 || data());
    if (dataBuffer_) {
      dataBuffer_->setSize(dataBuffer_->size() + size());
    }
    vector_->setNoCopy(offset_, StringView(data(), size()));
  }

 private:
  /// The buffer that the output string uses for its allocation set during
  /// reserve() call
  Buffer* dataBuffer_ = nullptr;

  FlatVector<StringView>* vector_;

  int32_t offset_;
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
    if (size != vector_->size()) {
      vector_->resize(size);
      init(*vector_);
    }
  }

  VectorWriter() {}

  proxy_t& current() {
    proxy_ = proxy_t(vector_, offset_);
    return proxy_;
  }

  void copyCommit(const proxy_t& data) {
    // Not really copying.
    proxy_.finalize();
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
    if (size != vector_->size()) {
      vector_->resize(size);
      init(*vector_);
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
