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
#include <array>
#include <cstring>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include "velox/expression/ComplexViewTypes.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/UdfTypeResolver.h"
#include "velox/expression/VariadicView.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

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

  const BaseVector* baseVector() const {
    return decoded_.base();
  }

  const DecodedVector& decoded_;
};

// ConstantVectorReader and FlatVectorReader are optimized for primitive types
// in constant or flat encoded vectors.  They operate directly on the vector's
// content avoiding the need to go through the expensive decoding process.
template <typename T>
struct ConstantVectorReader {
  using exec_in_t = typename VectorExec::template resolver<T>::in_type;

  std::optional<exec_in_t> value;

  explicit ConstantVectorReader<T>(ConstantVector<exec_in_t>& vector) {
    if (!vector.isNullAt(0)) {
      value = *vector.rawValues();
    }
  }

  exec_in_t operator[](vector_size_t) const {
    return *value;
  }

  exec_in_t readNullFree(vector_size_t) const {
    return *value;
  }

  bool isSet(vector_size_t) const {
    return value.has_value();
  }

  bool mayHaveNulls() const {
    return !value.has_value();
  }

  bool containsNull(vector_size_t) const {
    return !value.has_value();
  }

  bool containsNull(vector_size_t, vector_size_t) const {
    return !value.has_value();
  }

  inline bool mayHaveNullsRecursive() const {
    return !value.has_value();
  }

  // Scalars don't have children, so this is a no-op.
  void setChildrenMayHaveNulls() {}
};

template <typename T>
struct FlatVectorReader {
  using exec_in_t = typename VectorExec::template resolver<T>::in_type;

  const exec_in_t* values;
  FlatVector<exec_in_t>* vector;

  explicit FlatVectorReader<T>(FlatVector<exec_in_t>& baseVector)
      : values(baseVector.rawValues()), vector(&baseVector) {}

  exec_in_t operator[](vector_size_t offset) const {
    return values[offset];
  }

  exec_in_t readNullFree(vector_size_t offset) const {
    return operator[](offset);
  }

  bool isSet(vector_size_t offset) const {
    return !vector->isNullAt(offset);
  }

  bool mayHaveNulls() const {
    return vector->mayHaveNulls();
  }

  bool containsNull(vector_size_t index) const {
    return !isSet(index);
  }

  bool containsNull(vector_size_t startIndex, vector_size_t endIndex) const {
    for (auto index = startIndex; index < endIndex; ++index) {
      if (containsNull(index)) {
        return true;
      }
    }
    return false;
  }

  bool mayHaveNullsRecursive() const {
    return mayHaveNulls();
  }

  // Scalars don't have children, so this is a no-op.
  void setChildrenMayHaveNulls() {}
};

// This VectorReader is optimized for primitive types in constant or flat
// encoded vectors.  It operates directly on the vector's content avoiding
// the need to go through the expensive decoding process.
template <typename T>
struct ConstantFlatVectorReader {
  using exec_in_t = typename VectorExec::template resolver<T>::in_type;

  explicit ConstantFlatVectorReader(const FlatVector<exec_in_t>* vector)
      : rawValues_(vector->rawValues()),
        rawNulls_(vector->rawNulls()),
        indexMultiple_(1) {}

  explicit ConstantFlatVectorReader(const ConstantVector<exec_in_t>* vector)
      : rawValues_(vector->rawValues()),
        rawNulls_(vector->isNullAt(0) ? &bits::kNull64 : nullptr),
        indexMultiple_(0) {}

  explicit ConstantFlatVectorReader(const VectorReader<T>&) = delete;
  VectorReader<T>& operator=(const VectorReader<T>&) = delete;

  exec_in_t operator[](vector_size_t offset) const {
    return rawValues_[offset * indexMultiple_];
  }

  exec_in_t readNullFree(vector_size_t offset) const {
    return operator[](offset);
  }

  bool isSet(vector_size_t offset) const {
    return !rawNulls_ || !bits::isBitNull(rawNulls_, offset * indexMultiple_);
  }

  bool mayHaveNulls() const {
    return rawNulls_;
  }

  inline bool containsNull(vector_size_t index) const {
    return !isSet(index);
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
    return mayHaveNulls();
  }

  // Scalars don't have children, so this is a no-op.
  void setChildrenMayHaveNulls() {}

 private:
  const exec_in_t* rawValues_;
  const uint64_t* rawNulls_;
  // Flat Vectors use an identity mapping for indices, Constant Vectors map all
  // indices to 0.  This is the same as multiplying by 1 or 0 respectively.
  // We multiply the index by this value to get that mapping.
  vector_size_t indexMultiple_;
};

namespace detail {

template <typename TOut>
const TOut& getDecoded(const DecodedVector& decoded) {
  auto base = decoded.base();
  return *base->template as<TOut>();
}

inline DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  decoder.decode(vector);
  return &decoder;
}
} // namespace detail

template <typename K, typename V>
struct VectorReader<Map<K, V>> {
  using exec_in_t = typename VectorExec::template resolver<Map<K, V>>::in_type;
  using exec_null_free_in_t =
      typename VectorExec::template resolver<Map<K, V>>::null_free_in_type;

  explicit VectorReader(const DecodedVector* decoded)
      : decoded_{*decoded},
        vector_(detail::getDecoded<MapVector>(decoded_)),
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

  bool mayHaveNulls() const {
    return decoded_.mayHaveNulls();
  }

  void setChildrenMayHaveNulls() {
    keyReader_.setChildrenMayHaveNulls();
    valReader_.setChildrenMayHaveNulls();

    keysMayHaveNulls_ = keyReader_.mayHaveNullsRecursive();
    valuesMayHaveNulls_ = valReader_.mayHaveNullsRecursive();
  }

  const BaseVector* baseVector() const {
    return decoded_.base();
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
  using exec_in_t = typename VectorExec::template resolver<Array<V>>::in_type;
  using exec_null_free_in_t =
      typename VectorExec::template resolver<Array<V>>::null_free_in_type;
  using exec_in_child_t = typename VectorExec::template resolver<V>::in_type;

  explicit VectorReader(const DecodedVector* decoded)
      : decoded_(*decoded),
        vector_(detail::getDecoded<ArrayVector>(decoded_)),
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

  bool mayHaveNulls() const {
    return decoded_.mayHaveNulls();
  }

  void setChildrenMayHaveNulls() {
    childReader_.setChildrenMayHaveNulls();

    valuesMayHaveNulls_ = childReader_.mayHaveNullsRecursive();
  }

  const BaseVector* baseVector() const {
    return decoded_.base();
  }

  DecodedVector arrayValuesDecoder_;
  const DecodedVector& decoded_;
  const ArrayVector& vector_;
  const vector_size_t* offsets_;
  const vector_size_t* lengths_;
  VectorReader<V> childReader_;
  std::optional<bool> valuesMayHaveNulls_;
};

template <typename... T>
struct VectorReader<Row<T...>> {
  using in_vector_t = RowVector;
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

  bool mayHaveNulls() const {
    return decoded_.mayHaveNulls();
  }

  void setChildrenMayHaveNulls() {
    std::apply(
        [](auto&... reader) { (reader->setChildrenMayHaveNulls(), ...); },
        childReaders_);
  }

  const BaseVector* baseVector() const {
    return decoded_.base();
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

template <typename T>
struct VectorReader<Variadic<T>> {
  using exec_in_t = typename VectorExec::resolver<Variadic<T>>::in_type;
  using exec_null_free_in_t =
      typename VectorExec::template resolver<Variadic<T>>::null_free_in_type;

  explicit VectorReader(
      std::vector<std::optional<LocalDecodedVector>>& decodedArgs,
      int32_t startPosition)
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

  bool mayHaveNulls() const {
    for (const auto& childReader : childReaders_) {
      if (childReader->mayHaveNulls()) {
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
  auto prepareChildReaders(
      std::vector<std::optional<LocalDecodedVector>>& decodedArgs,
      int32_t startPosition) {
    std::vector<std::unique_ptr<VectorReader<T>>> childReaders;
    childReaders.reserve(decodedArgs.size() - startPosition);

    for (int i = startPosition; i < decodedArgs.size(); ++i) {
      childReaders.emplace_back(
          std::make_unique<VectorReader<T>>(decodedArgs.at(i).value().get()));
    }

    return childReaders;
  }

  std::vector<std::unique_ptr<VectorReader<T>>> childReaders_;
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

  bool mayHaveNulls() const {
    return decoded_.mayHaveNulls();
  }

  inline void setChildrenMayHaveNulls() {
    // This function is only called if callNullFree is defined.
    // TODO (kevinwilfong): Add support for Generics in callNullFree.
    VELOX_UNSUPPORTED(
        "Calling callNullFree with Generic arguments is not yet supported.");
  }

  const BaseVector* baseVector() const {
    return decoded_.base();
  }

  const DecodedVector& decoded_;

  // Those two variables are mutated by the GenericView during cast operations,
  // and are shared across GenericViews constructed by the reader.
  mutable std::array<std::shared_ptr<void>, 3> castReaders_;
  mutable TypePtr castType_ = nullptr;
};

} // namespace facebook::velox::exec
