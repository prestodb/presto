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

#include "velox/vector/BiasVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SequenceVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/tests/utils/VectorMakerStats.h"

namespace facebook::velox::test {

namespace detail {

template <typename T>
T jsonValue(const folly::dynamic& jsonValue) {
  if constexpr (
      std::is_same_v<int8_t, T> || std::is_same_v<int16_t, T> ||
      std::is_same_v<int32_t, T> || std::is_same_v<int64_t, T>) {
    return jsonValue.asInt();
  }

  if constexpr (std::is_same_v<float, T> || std::is_same_v<double, T>) {
    return jsonValue.asDouble();
  }

  if constexpr (std::is_same_v<std::string, T>) {
    return jsonValue.asString();
  }

  if constexpr (std::is_same_v<bool, T>) {
    return jsonValue.asBool();
  }

  VELOX_UNSUPPORTED();
}

template <typename T>
void appendVariant(std::vector<variant>& values, const T& x) {
  values.push_back(x);
};

template <typename TupleT, std::size_t... Is>
variant toVariantRow(const TupleT& tp, std::index_sequence<Is...>) {
  std::vector<variant> values;
  (appendVariant(values, std::get<Is>(tp)), ...);
  return variant::row(values);
}

template <typename TupleT, std::size_t TupleSize = std::tuple_size_v<TupleT>>
variant toVariantRow(const TupleT& tp) {
  return toVariantRow(tp, std::make_index_sequence<TupleSize>{});
}

} // namespace detail

class SimpleVectorLoader : public VectorLoader {
 public:
  explicit SimpleVectorLoader(std::function<VectorPtr(RowSet)> loader)
      : loader_(loader) {}

  void loadInternal(
      RowSet rows,
      ValueHook* hook,
      vector_size_t resultSize,
      VectorPtr* result) override {
    VELOX_CHECK(!hook, "SimpleVectorLoader doesn't support ValueHook");
    auto& resultRef = *result;
    resultRef = loader_(rows);
    if (resultRef->size() < resultSize) {
      resultRef->resize(resultSize);
    }
  }

 private:
  std::function<VectorPtr(RowSet)> loader_;
};

class VectorMaker {
 public:
  explicit VectorMaker(memory::MemoryPool* pool) : pool_(pool) {}

  static std::function<bool(vector_size_t /*row*/)> nullEvery(
      int n,
      int startingFrom = 0) {
    return [n, startingFrom](vector_size_t row) {
      return row >= startingFrom && ((row - startingFrom) % n == 0);
    };
  }

  static std::shared_ptr<const RowType> rowType(
      std::vector<std::shared_ptr<const Type>>&& types);

  RowVectorPtr rowVector(const std::vector<VectorPtr>& children);

  RowVectorPtr rowVector(
      std::vector<std::string> childNames,
      const std::vector<VectorPtr>& children);

  RowVectorPtr rowVector(
      const std::shared_ptr<const RowType>& rowType,
      vector_size_t size);

  template <typename T>
  FlatVectorPtr<EvalType<T>> flatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*row*/)> valueAt,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr,
      const TypePtr& type = CppToType<T>::create()) {
    auto flatVector =
        BaseVector::create<FlatVector<EvalType<T>>>(type, size, pool_);
    for (vector_size_t i = 0; i < size; i++) {
      if (isNullAt && isNullAt(i)) {
        flatVector->setNull(i, true);
      } else {
        auto v = valueAt(i);
        flatVector->set(i, EvalType<T>(v));
      }
    }
    return flatVector;
  }

  template <typename T>
  std::shared_ptr<LazyVector> lazyFlatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*row*/)> valueAt,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    return std::make_shared<LazyVector>(
        pool_,
        CppToType<T>::create(),
        size,
        std::make_unique<SimpleVectorLoader>([=](RowSet rowSet) {
          // Populate requested rows with correct data and fill in gaps with
          // "garbage".
          SelectivityVector rows(rowSet.back() + 1, false);
          for (auto row : rowSet) {
            rows.setValid(row, true);
          }
          rows.updateBounds();

          auto selectiveValueAt = [&](auto row) {
            return rows.isValid(row) ? valueAt(row) : T();
          };

          std::function<bool(vector_size_t)> selectiveIsNullAt = nullptr;
          if (isNullAt) {
            selectiveIsNullAt = [&](auto row) {
              return rows.isValid(row) ? isNullAt(row) : false;
            };
          }

          return flatVector<T>(
              rowSet.back() + 1, selectiveValueAt, selectiveIsNullAt);
        }));
  }

  template <typename T>
  FlatVectorPtr<T> flatVector(
      size_t size,
      const TypePtr& type = CppToType<T>::create()) {
    return BaseVector::create<FlatVector<T>>(type, size, pool_);
  }

  /// Create a FlatVector<T>
  /// creates a FlatVector based on elements from the input std::vector. String
  /// vectors can be created using std::vector of char*, StringView,
  /// std::string, or std::string_view as input. The string contents will be
  /// copied to the flatvector's internal string buffer.
  ///
  /// Elements are non-nullable.
  ///
  /// Examples:
  ///   auto flatVector = flatVector({1, 2, 3, 4});
  ///   auto flatVector2 = flatVector({"hello", "world"});
  template <typename T>
  FlatVectorPtr<EvalType<T>> flatVector(
      const std::vector<T>& data,
      const TypePtr& type = CppToType<T>::create());

  // Helper overload to allow users to use initializer list directly without
  // explicitly specifying the template type, e.g:
  //
  //   auto flatVector2 = flatVector({"hello", "world"});
  template <typename T>
  FlatVectorPtr<EvalType<T>> flatVector(
      const std::initializer_list<T>& data,
      const TypePtr& type = CppToType<T>::create()) {
    return flatVector(std::vector<T>(data), type);
  }

  /// Create a FlatVector<T>
  /// creates a FlatVector based on elements from the input std::vector.
  /// Works for primitive and string types, similarly to flatVector().
  ///
  /// Elements are nullable.
  ///
  /// Examples:
  ///   auto flatVector = flatVectorNullable({1, std::nullopt, 3});
  ///   auto flatVectorStr = flatVectorNullable({
  ///       "hello"_sv, std::nullopt, "world"_sv});
  template <typename T>
  FlatVectorPtr<EvalType<T>> flatVectorNullable(
      const std::vector<std::optional<T>>& data,
      const TypePtr& type = CppToType<T>::create());

  // Helper overload to allow users to use initializer list directly without
  // explicitly specifying the template type.
  template <typename T>
  FlatVectorPtr<EvalType<T>> flatVectorNullable(
      const std::initializer_list<std::optional<T>>& data,
      const TypePtr& type = CppToType<T>::create()) {
    return flatVectorNullable(std::vector<std::optional<T>>(data), type);
  }

  template <typename T, int TupleIndex, typename TupleType>
  FlatVectorPtr<T> flatVector(
      const std::vector<TupleType>& data,
      const TypePtr& type) {
    auto vector = BaseVector::create<FlatVector<T>>(type, data.size(), pool_);
    for (vector_size_t i = 0; i < data.size(); ++i) {
      vector->set(i, std::get<TupleIndex>(data[i]));
    }
    return vector;
  }

  template <typename T>
  FlatVectorPtr<T> allNullFlatVector(vector_size_t size) {
    auto flatVector =
        BaseVector::create<FlatVector<T>>(CppToType<T>::create(), size, pool_);
    for (vector_size_t i = 0; i < size; i++) {
      flatVector->setNull(i, true);
    }
    return flatVector;
  }

  /// Create a BiasVector<T>
  /// creates a BiasVector (vector encoded using bias encoding) based on a flat
  /// input from an std::vector.
  ///
  /// Elements are nullable.
  ///
  /// Example:
  ///   auto biasVector = maker.biasVector<int64_t>({10, 15, 13, 11, 12, 14});
  template <typename T>
  BiasVectorPtr<EvalType<T>> biasVector(
      const std::vector<std::optional<T>>& data);

  /// Create a SequenceVector<T>
  /// creates a SequenceVector (vector encoded using RLE) based on a flat
  /// input from an std::vector.
  ///
  /// Elements are nullable.
  ///
  /// Example:
  ///   auto sequenceVector = maker.sequenceVector<int64_t>({
  ///       10, 10, 10, std::nullopt, 15, 15, std::nullopt, std::nullopt});
  template <typename T>
  SequenceVectorPtr<EvalType<T>> sequenceVector(
      const std::vector<std::optional<T>>& data);

  /// Create a ConstantVector<T>
  /// creates a ConstantVector (vector that represents a single constant value)
  /// based on a flat input from an std::vector. The input vector may contain
  /// several elements, but if the input vector contains more than one distinct
  /// element, it fails.
  ///
  /// Elements are nullable.
  ///
  /// Examples:
  ///   auto constantVector = maker.constantVector<int64_t>({11, 11, 11});
  ///   auto constantVector = maker.constantVector<int64_t>(
  ///        {std::nullopt, std::nullopt});
  template <typename T>
  ConstantVectorPtr<EvalType<T>> constantVector(
      const std::vector<std::optional<T>>& data);

  /// Create a DictionaryVector<T>
  /// creates a dictionary encoded vector based on a flat input from an
  /// std::vector.
  ///
  /// Elements are nullable.
  ///
  /// Example:
  ///   auto dictionaryVector = maker.dictionaryVector<int64_t>({
  ///       10, 10, 10, std::nullopt, 15, 15, std::nullopt, std::nullopt});
  template <typename T>
  DictionaryVectorPtr<EvalType<T>> dictionaryVector(
      const std::vector<std::optional<T>>& data);

  /// Convenience function that creates an vector based on input std::vector
  /// data, encoded with given `vecType`.
  template <typename T>
  SimpleVectorPtr<EvalType<T>> encodedVector(
      VectorEncoding::Simple vecType,
      const std::vector<std::optional<T>>& data) {
    switch (vecType) {
      case VectorEncoding::Simple::FLAT:
        return flatVectorNullable(data);
      case VectorEncoding::Simple::CONSTANT:
        return constantVector(data);
      case VectorEncoding::Simple::DICTIONARY:
        return dictionaryVector(data);
      case VectorEncoding::Simple::SEQUENCE:
        return sequenceVector(data);
      case VectorEncoding::Simple::BIASED:
        return biasVector(data);
      default:
        VELOX_UNSUPPORTED("Unsupported encoding type for VectorMaker.");
    }
    return nullptr;
  }

  /// Create a ArrayVector<T>
  /// size and null for individual array is determined by sizeAt and isNullAt
  /// value for individual array is determined by valueAt.
  template <typename T>
  ArrayVectorPtr arrayVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<T(vector_size_t /* idx */)> valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr,
      std::function<bool(vector_size_t /* idx */)> valueIsNullAt = nullptr,
      const TypePtr& arrayType = ARRAY(CppToType<T>::create())) {
    BufferPtr nulls;
    BufferPtr offsets;
    BufferPtr sizes;
    auto numElements =
        createOffsetsAndSizes(size, sizeAt, isNullAt, &nulls, &offsets, &sizes);

    return std::make_shared<ArrayVector>(
        pool_,
        arrayType,
        nulls,
        size,
        offsets,
        sizes,
        flatVector<T>(numElements, valueAt, valueIsNullAt));
  }

  template <typename T>
  ArrayVectorPtr arrayVectorImpl(
      const TypePtr& type,
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<T(vector_size_t /* row */, vector_size_t /* idx */)>
          valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr) {
    BufferPtr nulls;
    BufferPtr offsets;
    BufferPtr sizes;
    auto numElements =
        createOffsetsAndSizes(size, sizeAt, isNullAt, &nulls, &offsets, &sizes);

    auto flatVector = BaseVector::create<FlatVector<EvalType<T>>>(
        type->childAt(0), numElements, pool_);
    vector_size_t currentIndex = 0;
    for (vector_size_t i = 0; i < size; ++i) {
      if (isNullAt && isNullAt(i)) {
        continue;
      }
      for (vector_size_t j = 0; j < sizeAt(i); ++j) {
        auto value = valueAt(i, j);
        flatVector->set(currentIndex, EvalType<T>(value));
        currentIndex++;
      }
    }

    return std::make_shared<ArrayVector>(
        pool_,
        type,
        nulls,
        size,
        offsets,
        sizes,
        flatVector,
        BaseVector::countNulls(nulls, 0, size));
  }

  /// Create a ArrayVector<T>
  /// size and null for individual array is determined by sizeAt and isNullAt
  /// value for elements of each array in a given row is determined by valueAt.
  template <typename T>
  ArrayVectorPtr arrayVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<T(vector_size_t /* row */, vector_size_t /* idx */)>
          valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr,
      const TypePtr& arrayType = ARRAY(CppToType<T>::create())) {
    return arrayVectorImpl(arrayType, size, sizeAt, valueAt, isNullAt);
  }

  template <typename T>
  ArrayVectorPtr arrayVectorImpl(
      const TypePtr& type,
      const std::vector<std::vector<T>>& data) {
    vector_size_t size = data.size();
    BufferPtr offsets = allocateOffsets(size, pool_);
    BufferPtr sizes = allocateSizes(size, pool_);

    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();

    // Count number of elements.
    vector_size_t numElements = 0;
    for (const auto& array : data) {
      numElements += array.size();
    }

    // Create the underlying flat vector.
    auto flatVector = BaseVector::create<FlatVector<EvalType<T>>>(
        type->childAt(0), numElements, pool_);

    vector_size_t currentIdx = 0;
    for (const auto& arrayValue : data) {
      *rawSizes++ = arrayValue.size();
      *rawOffsets++ = currentIdx;

      for (auto arrayElement : arrayValue) {
        flatVector->set(currentIdx++, EvalType<T>(arrayElement));
      }
    }

    return std::make_shared<ArrayVector>(
        pool_, type, nullptr, size, offsets, sizes, flatVector);
  }

  /// Create a ArrayVector<T>
  /// array elements are created based on input std::vectors and are
  /// non-nullable.
  template <typename T>
  ArrayVectorPtr arrayVector(
      const std::vector<std::vector<T>>& data,
      const TypePtr& elementType = CppToType<T>::create()) {
    return arrayVectorImpl(ARRAY(elementType), data);
  }

  /// Create an ArrayVector<ROW> from nested std::vectors of Variants.
  ArrayVectorPtr arrayOfRowVector(
      const RowTypePtr& rowType,
      const std::vector<std::vector<variant>>& data);

  /// Creates an ARRAY(ROW(...)) vector from a list of lists of optional tuples.
  ///
  /// Allows to create arrays with null elements, but arrays themselves cannot
  /// be null. Members of the structs cannot be null either.
  ///
  /// Example:
  ///
  ///  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
  ///      data = {
  ///          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
  ///          {},
  ///          {std::nullopt},
  ///          {{{4, "green"}}, {{5, "purple"}}},
  ///  };
  ///
  ///  auto arrayVector = maker_.arrayOfRowVector(data, ROW({INTEGER(),
  ///  VARCHAR()}));
  template <typename TupleT>
  ArrayVectorPtr arrayOfRowVector(
      const std::vector<std::vector<std::optional<TupleT>>>& data,
      const RowTypePtr& rowType) {
    std::vector<std::vector<variant>> arrays;
    for (const auto& tuples : data) {
      std::vector<variant> elements;
      for (const auto& t : tuples) {
        if (t.has_value()) {
          elements.push_back(detail::toVariantRow(t.value()));
        } else {
          elements.push_back(variant::null(TypeKind::ROW));
        }
      }
      arrays.push_back(elements);
    }

    return arrayOfRowVector(rowType, arrays);
  }

  template <typename T>
  ArrayVectorPtr arrayVectorNullableImpl(
      const TypePtr& type,
      const std::vector<std::optional<std::vector<std::optional<T>>>>& data) {
    VELOX_CHECK(type->isArray(), "Type must be an array: {}", type->toString());

    vector_size_t size = data.size();
    BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, pool_);
    BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool_);
    BufferPtr nulls = AlignedBuffer::allocate<uint64_t>(size, pool_);

    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();
    auto rawNulls = nulls->asMutable<uint64_t>();
    bits::fillBits(rawNulls, 0, size, pool_);

    // Count number of elements.
    vector_size_t numElements = 0;
    vector_size_t indexPtr = 0;
    for (const auto& array : data) {
      numElements += array.has_value() ? array.value().size() : 0;
      if (!array.has_value()) {
        bits::setNull(rawNulls, indexPtr, true);
      }
      indexPtr++;
    }

    using V = typename CppToType<T>::NativeType;

    // Create the underlying flat vector.
    auto flatVector =
        BaseVector::create<FlatVector<V>>(type->childAt(0), numElements, pool_);
    auto elementRawNulls = flatVector->mutableRawNulls();

    vector_size_t currentIdx = 0;

    for (const auto& arrayValue : data) {
      *rawSizes++ = arrayValue.has_value() ? arrayValue.value().size() : 0;
      *rawOffsets++ = currentIdx;

      if (arrayValue.has_value()) {
        for (auto arrayElement : arrayValue.value()) {
          if (arrayElement == std::nullopt) {
            bits::setNull(elementRawNulls, currentIdx, true);
          } else {
            flatVector->set(currentIdx, V(*arrayElement));
          }
          ++currentIdx;
        }
      }
    }

    return std::make_shared<ArrayVector>(
        pool_, type, nulls, size, offsets, sizes, flatVector);
  }

  /// Create a ArrayVector<T>
  /// array elements are created based on input std::vectors and are
  /// nullable.
  template <typename T>
  ArrayVectorPtr arrayVectorNullable(
      const std::vector<std::optional<std::vector<std::optional<T>>>>& data,
      const TypePtr& type = ARRAY(CppToType<T>::create())) {
    return arrayVectorNullableImpl(type, data);
  }

  /// Creates an ArrayVector from a list of JSON arrays.
  ///
  /// JSON arrays can represent a null array, an empty array or array with null
  /// elements.
  ///
  /// Examples:
  ///  [1, 2, 3]
  ///  [1, 2, null, 4]
  ///  [null, null]
  ///  [] - empty array
  ///  null - null array
  ///
  /// @tparam T Type of array elements.
  /// @param jsonArrays A list of JSON arrays. JSON array cannot be an empty
  /// string.
  template <typename T>
  ArrayVectorPtr arrayVectorFromJson(
      const std::vector<std::string>& jsonArrays,
      const TypePtr& arrayType = ARRAY(CppToType<T>::create())) {
    std::vector<std::optional<std::vector<std::optional<T>>>> arrays;
    for (const auto& jsonArray : jsonArrays) {
      VELOX_CHECK(!jsonArray.empty());

      const folly::dynamic arrayObject = folly::parseJson(jsonArray);
      if (arrayObject.isNull()) {
        // Null array.
        arrays.push_back(std::nullopt);
        continue;
      }
      std::vector<std::optional<T>> elements;
      appendElementsFromJsonArray(arrayObject, elements);
      arrays.push_back(elements);
    }
    return arrayVectorNullable<T>(arrays, arrayType);
  }

  /// Creates an ArrayVector from a list of JSON arrays of arrays.
  ///
  /// JSON array of arrays can represent a null array, an empty array or array
  /// with null elements.
  ///
  /// Examples:
  /// [[1, 2], [2, 3, 4], [null, 7]]
  /// [[1, 3, 7, 9], []]
  /// [] - empty array of arrays
  /// null - null array of arrays
  /// [null] - array of null array
  /// [[]]
  ///
  /// @tparam T Type of array elements.
  /// @param jsonArrays A list of JSON arrays. JSON array cannot be an empty
  /// string.
  template <typename T>
  ArrayVectorPtr nestedArrayVectorFromJson(
      const std::vector<std::string>& jsonArrays,
      const TypePtr& arrayType = ARRAY(CppToType<T>::create())) {
    std::vector<std::optional<std::vector<std::optional<T>>>> baseVector;
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> nulls;
    const std::vector<std::optional<T>> empty;
    int offset = 0;
    for (auto i = 0; i < jsonArrays.size(); i++) {
      const auto& jsonArray = jsonArrays.at(i);
      VELOX_CHECK(!jsonArray.empty());
      const folly::dynamic arraysObject = folly::parseJson(jsonArray);
      offsets.push_back(offset);
      if (arraysObject.isNull()) {
        // Null array.
        nulls.push_back(i);
        continue;
      }
      for (const auto& nestedArray : arraysObject) {
        if (nestedArray.isNull()) {
          // Null nested array
          baseVector.push_back(std::nullopt);
        } else {
          std::vector<std::optional<T>> elements;
          appendElementsFromJsonArray(nestedArray, elements);
          baseVector.push_back(elements);
        }
      }
      offset += arraysObject.size();
    }
    auto baseArrayVector = arrayVectorNullable<T>(baseVector, arrayType);
    return arrayVector(offsets, baseArrayVector, nulls);
  }

  ArrayVectorPtr allNullArrayVector(
      vector_size_t size,
      const TypePtr& elementType);

  /// Create a Map<TKey, TValue>
  /// size and null for individual map is determined by sizeAt and isNullAt
  /// key and value for individual map is determined by keyAt and valueAt
  template <typename TKey, typename TValue>
  MapVectorPtr mapVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<TKey(vector_size_t /* idx */)> keyAt,
      std::function<TValue(vector_size_t /* idx */)> valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr,
      std::function<bool(vector_size_t /*row */)> valueIsNullAt = nullptr,
      const TypePtr& type =
          MAP(CppToType<TKey>::create(), CppToType<TValue>::create())) {
    BufferPtr nulls;
    BufferPtr offsets;
    BufferPtr sizes;
    auto numElements =
        createOffsetsAndSizes(size, sizeAt, isNullAt, &nulls, &offsets, &sizes);

    return std::make_shared<MapVector>(
        pool_,
        type,
        nulls,
        size,
        offsets,
        sizes,
        flatVector<TKey>(numElements, keyAt, nullptr, type->childAt(0)),
        flatVector<TValue>(
            numElements, valueAt, valueIsNullAt, type->childAt(1)),
        BaseVector::countNulls(nulls, 0, size));
  }

  template <typename TKey, typename TValue>
  MapVectorPtr mapVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* mapRow */)> sizeAt,
      std::function<TKey(vector_size_t /* mapRow */, vector_size_t /*row*/)>
          keyAt,
      std::function<TValue(vector_size_t /* mapRow */, vector_size_t /*row*/)>
          valueAt,
      std::function<bool(vector_size_t /*mapRow */)> isNullAt = nullptr) {
    BufferPtr nulls;
    BufferPtr offsets;
    BufferPtr sizes;
    auto numElements =
        createOffsetsAndSizes(size, sizeAt, isNullAt, &nulls, &offsets, &sizes);

    auto rawNulls = nulls ? nulls->asMutable<uint64_t>() : nullptr;
    auto rawSizes = sizes->asMutable<vector_size_t>();

    std::vector<TKey> keys;
    keys.reserve(numElements);
    std::vector<TValue> values;
    values.reserve(numElements);
    for (vector_size_t mapRow = 0; mapRow < size; mapRow++) {
      if (rawNulls && bits::isBitNull(rawNulls, mapRow)) {
        continue;
      }

      auto mapSize = rawSizes[mapRow];
      for (vector_size_t row = 0; row < mapSize; row++) {
        keys.push_back(keyAt(mapRow, row));
        values.push_back(valueAt(mapRow, row));
      }
    }

    return std::make_shared<MapVector>(
        pool_,
        MAP(CppToType<TKey>::create(), CppToType<TValue>::create()),
        nulls,
        size,
        offsets,
        sizes,
        flatVector(keys),
        flatVector(values));
  }

  template <typename TKey, typename TValue>
  MapVectorPtr mapVector(
      const std::vector<std::vector<std::pair<TKey, std::optional<TValue>>>>&
          maps,
      const TypePtr& mapType =
          MAP(CppToType<TKey>::create(), CppToType<TValue>::create())) {
    std::vector<
        std::optional<std::vector<std::pair<TKey, std::optional<TValue>>>>>
        nullableMaps;
    nullableMaps.reserve(maps.size());
    for (const auto& m : maps) {
      nullableMaps.push_back(m);
    }

    return mapVector<TKey, TValue>(nullableMaps, mapType, false);
  }

  template <typename TKey, typename TValue>
  MapVectorPtr mapVector(
      const std::vector<std::optional<
          std::vector<std::pair<TKey, std::optional<TValue>>>>>& maps,
      const TypePtr& mapType =
          MAP(CppToType<TKey>::create(), CppToType<TValue>::create()),
      bool hasNulls = true) {
    std::vector<TKey> keys;
    std::vector<TValue> values;
    std::vector<bool> nullValues;

    for (const auto& map : maps) {
      if (map.has_value()) {
        for (const auto& [key, value] : map.value()) {
          keys.push_back(key);
          values.push_back(value.value_or(TValue()));
          nullValues.push_back(!value.has_value());
        }
      }
    }

    std::function<bool(vector_size_t)> isNullAt = nullptr;
    if (hasNulls) {
      isNullAt = [&](vector_size_t row) { return !maps[row].has_value(); };
    }

    return mapVector<TKey, TValue>(
        maps.size(),
        [&](vector_size_t row) {
          return maps[row].has_value() ? maps[row]->size() : 0;
        },
        [&](vector_size_t idx) { return keys[idx]; },
        [&](vector_size_t idx) { return values[idx]; },
        isNullAt,
        [&](vector_size_t idx) { return nullValues[idx]; },
        mapType);
  }

  /// Creates a MapVector from a list of JSON maps.
  ///
  /// JSON maps can represent a null map, an empty map or a map with null
  /// values. Null keys are not allowed.
  ///
  /// Note that order of map keys in the MapVector is not guaranteed to match
  /// the order of map keys in the JSON.
  ///
  /// Examples:
  ///  {1: 10, 2: 20, 3: 30}
  ///  {1: 10, 2: 20, 3: null, 4: 40}
  ///  {1: null, 2: null}
  ///  {} - empty map
  ///  null - null map
  ///
  /// @tparam K Type of map keys. Must be a std::string or an integer: int8_t,
  /// int16_t, int32_t, int64_t.
  /// @tparam V Type of map value. Can be an integer or a floating point
  /// number.
  /// @param jsonMaps A list of JSON maps. JSON map cannot be an empty
  /// string.
  template <typename K, typename V>
  MapVectorPtr mapVectorFromJson(
      const std::vector<std::string>& jsonMaps,
      const TypePtr& mapType =
          MAP(CppToType<K>::create(), CppToType<V>::create())) {
    static_assert(
        std::is_same_v<K, int8_t> || std::is_same_v<K, int16_t> ||
        std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t> ||
        std::is_same_v<K, float> || std::is_same_v<K, double> ||
        std::is_same_v<K, std::string>);

    std::vector<std::optional<std::vector<std::pair<K, std::optional<V>>>>>
        maps;
    for (const auto& jsonMap : jsonMaps) {
      VELOX_CHECK(!jsonMap.empty());

      folly::json::serialization_opts options;
      options.convert_int_keys = true;
      options.allow_non_string_keys = true;
      folly::dynamic mapObject = folly::parseJson(jsonMap, options);
      if (mapObject.isNull()) {
        // Null map.
        maps.push_back(std::nullopt);
        continue;
      }

      std::vector<std::pair<K, std::optional<V>>> pairs;
      for (const auto& item : mapObject.items()) {
        auto key = detail::jsonValue<K>(item.first);

        if (item.second.isNull()) {
          // Null value.
          pairs.push_back({key, std::nullopt});
        } else {
          pairs.push_back({key, detail::jsonValue<V>(item.second)});
        }
      }

      maps.push_back(pairs);
    }

    return mapVector<K, V>(maps, mapType);
  }

  MapVectorPtr allNullMapVector(
      vector_size_t size,
      const TypePtr& keyType,
      const TypePtr& valueType);

  /// Create a FlatVector from a variant containing a scalar value.
  template <TypeKind kind>
  VectorPtr toFlatVector(variant value) {
    using T = typename TypeTraits<kind>::NativeType;
    if constexpr (std::is_same_v<T, StringView>) {
      return flatVector<StringView>({StringView(value.value<const char*>())});
    } else {
      return flatVector(std::vector<T>(1, value.value<T>()));
    }
  }

  /// Create constant vector of type ROW from a variant.
  VectorPtr
  constantRow(const RowTypePtr& rowType, variant value, vector_size_t size) {
    VELOX_CHECK_EQ(value.kind(), TypeKind::ROW);

    std::vector<VectorPtr> fields(rowType->size());
    for (auto i = 0; i < rowType->size(); i++) {
      fields[i] = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          toFlatVector, rowType->childAt(i)->kind(), value.row()[i]);
    }

    return BaseVector::wrapInConstant(
        size,
        0,
        std::make_shared<RowVector>(
            pool_, rowType, nullptr, 1, std::move(fields)));
  }

  template <typename T = BaseVector>
  static std::shared_ptr<T> flatten(const VectorPtr& vector) {
    SelectivityVector allRows(vector->size());
    auto flatVector =
        BaseVector::create<T>(vector->type(), vector->size(), vector->pool());
    flatVector->copy(vector.get(), allRows, nullptr);
    return flatVector;
  }

  /// Create an ArrayVector from a vector of offsets and a base element
  /// vector. The size of the arrays is computed from the difference of
  /// offsets. An optional vector of nulls can be passed to specify null rows.
  /// The offset for a null value must match previous offset
  /// i.e size computed should be zero.
  /// E.g arrayVector({0, 2 ,2}, elements, {1}) creates an array vector
  /// with array at index 1 as null.
  ArrayVectorPtr arrayVector(
      const std::vector<vector_size_t>& offsets,
      const VectorPtr& elements,
      const std::vector<vector_size_t>& nulls = {});

  /// Create a MapVector from a vector of offsets and key and value vectors.
  /// The size of the maps is computed from the difference of offsets.
  /// The sizes of the key and value vectors must be equal.
  /// An optional vector of nulls can be passed to specify null rows.
  /// The offset for a null value must match previous offset
  /// i.e size computed should be zero.
  /// E.g map({0, 2 ,2}, keys, values, {1}) creates a map vector
  /// with map at index 1 as null.
  MapVectorPtr mapVector(
      const std::vector<vector_size_t>& offsets,
      const VectorPtr& keys,
      const VectorPtr& values,
      const std::vector<vector_size_t>& nulls = {});

 private:
  vector_size_t createOffsetsAndSizes(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<bool(vector_size_t /*row */)> isNullAt,
      BufferPtr* nulls,
      BufferPtr* offsets,
      BufferPtr* sizes);

  template <typename T>
  void appendElementsFromJsonArray(
      const folly::dynamic& arrayObject,
      std::vector<std::optional<T>>& elements) {
    for (const auto& element : arrayObject) {
      if (element.isNull()) {
        elements.push_back(std::nullopt);
      } else {
        elements.push_back(detail::jsonValue<T>(element));
      }
    }
  }

  memory::MemoryPool* pool_;
};

} // namespace facebook::velox::test

#include "velox/vector/tests/utils/VectorMaker-inl.h"
