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

#include "velox/exec/ContainerRowSerde.h"
#include "velox/type/FloatingPointUtil.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

namespace {

// Copy from vector to stream.
void serializeSwitch(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& out,
    const ContainerRowSerdeOptions& options);

template <TypeKind Kind>
void serializeOne(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& stream,
    const ContainerRowSerdeOptions& options) {
  using T = typename TypeTraits<Kind>::NativeType;
  stream.appendOne<T>(vector.asUnchecked<SimpleVector<T>>()->valueAt(index));
}

template <>
void serializeOne<TypeKind::VARCHAR>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& stream,
    const ContainerRowSerdeOptions& /*options*/) {
  auto string = vector.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  stream.appendOne<int32_t>(string.size());
  stream.appendStringView(string);
}

template <>
void serializeOne<TypeKind::VARBINARY>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& stream,
    const ContainerRowSerdeOptions& /*options*/) {
  auto string = vector.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  stream.appendOne<int32_t>(string.size());
  stream.appendStringView(string);
}

template <>
void serializeOne<TypeKind::ROW>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& out,
    const ContainerRowSerdeOptions& options) {
  auto row = vector.wrappedVector()->asUnchecked<RowVector>();
  auto wrappedIndex = vector.wrappedIndex(index);
  const auto& type = row->type()->as<TypeKind::ROW>();
  // The layout is given by the type, not the instance. This will work
  // in the case of missing elements which will come out as null in
  // deserialization.
  auto childrenSize = type.size();
  auto children = row->children();
  std::vector<uint64_t> nulls(bits::nwords(childrenSize));
  for (auto i = 0; i < childrenSize; ++i) {
    if (i >= children.size() || !children[i] ||
        children[i]->isNullAt(wrappedIndex)) {
      bits::setBit(nulls.data(), i);
    }
  }
  out.append<uint64_t>(nulls);
  for (auto i = 0; i < children.size(); ++i) {
    if (!bits ::isBitSet(nulls.data(), i)) {
      serializeSwitch(*children[i], wrappedIndex, out, options);
    }
  }
}

void writeNulls(
    const BaseVector& values,
    vector_size_t offset,
    vector_size_t size,
    ByteOutputStream& out) {
  for (auto i = 0; i < size; i += 64) {
    uint64_t flags = 0;
    auto end = i + 64 < size ? 64 : size - i;
    for (auto bit = 0; bit < end; ++bit) {
      if (values.isNullAt(offset + i + bit)) {
        bits::setBit(&flags, bit, true);
      }
    }
    out.appendOne<uint64_t>(flags);
  }
}

void writeNulls(
    const BaseVector& values,
    folly::Range<const vector_size_t*> indices,
    ByteOutputStream& out) {
  auto size = indices.size();
  for (auto i = 0; i < size; i += 64) {
    uint64_t flags = 0;
    auto end = i + 64 < size ? 64 : size - i;
    for (auto bit = 0; bit < end; ++bit) {
      if (values.isNullAt(indices[i + bit])) {
        bits::setBit(&flags, bit, true);
      }
    }
    out.appendOne<uint64_t>(flags);
  }
}

void serializeArray(
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t size,
    ByteOutputStream& out,
    const ContainerRowSerdeOptions& options) {
  out.appendOne<int32_t>(size);
  writeNulls(elements, offset, size, out);
  for (auto i = 0; i < size; ++i) {
    if (!elements.isNullAt(i + offset)) {
      serializeSwitch(elements, i + offset, out, options);
    }
  }
}

void serializeArray(
    const BaseVector& elements,
    folly::Range<const vector_size_t*> indices,
    ByteOutputStream& out,
    const ContainerRowSerdeOptions& options) {
  out.appendOne<int32_t>(indices.size());
  writeNulls(elements, indices, out);
  for (auto i : indices) {
    if (!elements.isNullAt(i)) {
      serializeSwitch(elements, i, out, options);
    }
  }
}

template <>
void serializeOne<TypeKind::ARRAY>(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& out,
    const ContainerRowSerdeOptions& options) {
  auto array = source.wrappedVector()->asUnchecked<ArrayVector>();
  auto wrappedIndex = source.wrappedIndex(index);
  serializeArray(
      *array->elements(),
      array->offsetAt(wrappedIndex),
      array->sizeAt(wrappedIndex),
      out,
      options);
}

template <>
void serializeOne<TypeKind::MAP>(
    const BaseVector& vector,
    vector_size_t index,
    ByteOutputStream& out,
    const ContainerRowSerdeOptions& options) {
  auto map = vector.wrappedVector()->asUnchecked<MapVector>();
  auto wrappedIndex = vector.wrappedIndex(index);
  if (options.isKey) {
    auto indices = map->sortedKeyIndices(wrappedIndex);
    serializeArray(*map->mapKeys(), indices, out, options);
    serializeArray(*map->mapValues(), indices, out, options);
  } else {
    auto size = map->sizeAt(wrappedIndex);
    auto offset = map->offsetAt(wrappedIndex);
    serializeArray(*map->mapKeys(), offset, size, out, options);
    serializeArray(*map->mapValues(), offset, size, out, options);
  }
}

void serializeSwitch(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& stream,
    const ContainerRowSerdeOptions& options) {
  VELOX_DYNAMIC_TYPE_DISPATCH(
      serializeOne, source.typeKind(), source, index, stream, options);
}

// Copy from serialization to vector.
void deserializeSwitch(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result);

template <TypeKind Kind>
void deserializeOne(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  using T = typename TypeTraits<Kind>::NativeType;
  // Check that the vector is writable. This is faster than dynamic_cast.
  VELOX_CHECK_EQ(result.encoding(), VectorEncoding::Simple::FLAT);
  auto values = result.asUnchecked<FlatVector<T>>();
  values->set(index, in.read<T>());
}

void deserializeString(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_CHECK_EQ(result.encoding(), VectorEncoding::Simple::FLAT);
  auto values = result.asUnchecked<FlatVector<StringView>>();
  auto size = in.read<int32_t>();
  auto buffer = values->getBufferWithSpace(size);
  auto start = buffer->asMutable<char>() + buffer->size();
  in.readBytes(start, size);
  // If the string is not inlined in string view, we need to advance the buffer.
  if (not StringView::isInline(size)) {
    buffer->setSize(buffer->size() + size);
  }
  values->setNoCopy(index, StringView(start, size));
}

template <>
void deserializeOne<TypeKind::VARCHAR>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  deserializeString(in, index, result);
}

template <>
void deserializeOne<TypeKind::VARBINARY>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  deserializeString(in, index, result);
}

std::vector<uint64_t> readNulls(ByteInputStream& in, int32_t size) {
  auto n = bits::nwords(size);
  std::vector<uint64_t> nulls(n);
  for (auto i = 0; i < n; ++i) {
    nulls[i] = in.read<uint64_t>();
  }
  return nulls;
}

template <>
void deserializeOne<TypeKind::ROW>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  const auto& type = result.type()->as<TypeKind::ROW>();
  VELOX_CHECK_EQ(result.encoding(), VectorEncoding::Simple::ROW);
  auto row = result.asUnchecked<RowVector>();
  auto childrenSize = type.size();
  VELOX_CHECK_EQ(childrenSize, row->childrenSize());
  auto nulls = readNulls(in, childrenSize);
  for (auto i = 0; i < childrenSize; ++i) {
    auto child = row->childAt(i);
    if (child->size() <= index) {
      child->resize(index + 1);
    }
    if (bits::isBitSet(nulls.data(), i)) {
      child->setNull(index, true);
    } else {
      deserializeSwitch(in, index, *child);
    }
  }
  result.setNull(index, false);
}

// Reads the size, null flags and deserializes from 'in', appending to
// the end of 'elements'. Returns the number of added elements and
// sets 'offset' to the index of the first added element.
vector_size_t deserializeArray(
    ByteInputStream& in,
    BaseVector& elements,
    vector_size_t& offset) {
  auto size = in.read<int32_t>();
  auto nulls = readNulls(in, size);
  offset = elements.size();
  elements.resize(offset + size);
  for (auto i = 0; i < size; ++i) {
    if (bits::isBitSet(nulls.data(), i)) {
      elements.setNull(i + offset, true);
    } else {
      deserializeSwitch(in, i + offset, elements);
    }
  }
  return size;
}

template <>
void deserializeOne<TypeKind::ARRAY>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_CHECK_EQ(result.encoding(), VectorEncoding::Simple::ARRAY);
  auto array = result.asUnchecked<ArrayVector>();
  if (array->size() <= index) {
    array->resize(index + 1);
  }
  vector_size_t offset;
  auto size = deserializeArray(in, *array->elements(), offset);
  array->setOffsetAndSize(index, offset, size);
  result.setNull(index, false);
}

template <>
void deserializeOne<TypeKind::MAP>(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_CHECK_EQ(result.encoding(), VectorEncoding::Simple::MAP);
  auto map = result.asUnchecked<MapVector>();
  if (map->size() <= index) {
    map->resize(index + 1);
  }
  vector_size_t keyOffset;
  auto keySize = deserializeArray(in, *map->mapKeys(), keyOffset);
  vector_size_t valueOffset;
  auto valueSize = deserializeArray(in, *map->mapValues(), valueOffset);
  VELOX_CHECK_EQ(keySize, valueSize);
  VELOX_CHECK_EQ(keyOffset, valueOffset);
  map->setOffsetAndSize(index, keyOffset, keySize);
  result.setNull(index, false);
}

void deserializeSwitch(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_DYNAMIC_TYPE_DISPATCH(
      deserializeOne, result.typeKind(), in, index, result);
}

// Comparison of serialization and vector.
template <bool typeProvidesCustomComparison>
std::optional<int32_t> compareSwitch(
    ByteInputStream& stream,
    const BaseVector& vector,
    vector_size_t index,
    CompareFlags flags);

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<
        Kind != TypeKind::VARCHAR && Kind != TypeKind::VARBINARY &&
            Kind != TypeKind::ARRAY && Kind != TypeKind::MAP &&
            Kind != TypeKind::ROW,
        int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  using T = typename TypeTraits<Kind>::NativeType;
  auto rightValue = right.asUnchecked<SimpleVector<T>>()->valueAt(index);
  auto leftValue = left.read<T>();

  int result;
  if constexpr (typeProvidesCustomComparison) {
    result =
        SimpleVector<T>::template comparePrimitiveAscWithCustomComparison<Kind>(
            right.type().get(), leftValue, rightValue);
  } else {
    result = SimpleVector<T>::comparePrimitiveAsc(leftValue, rightValue);
  }

  return flags.ascending ? result : result * -1;
}

int compareStringAsc(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    bool equalsOnly) {
  int32_t leftSize = left.read<int32_t>();
  auto rightView =
      right.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  if (rightView.size() != leftSize && equalsOnly) {
    return 1;
  }
  auto compareSize = std::min<int32_t>(leftSize, rightView.size());
  int32_t rightOffset = 0;
  while (compareSize > 0) {
    auto leftView = left.nextView(compareSize);
    auto result = memcmp(
        leftView.data(), rightView.data() + rightOffset, leftView.size());
    if (result != 0) {
      return result;
    }
    rightOffset += leftView.size();
    compareSize -= leftView.size();
  }
  return leftSize - rightView.size();
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::VARCHAR, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto result = compareStringAsc(left, right, index, flags.equalsOnly);
  return flags.ascending ? result : result * -1;
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::VARBINARY, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto result = compareStringAsc(left, right, index, flags.equalsOnly);
  return flags.ascending ? result : result * -1;
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::ROW, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto row = right.wrappedVector()->asUnchecked<RowVector>();
  auto wrappedIndex = right.wrappedIndex(index);
  VELOX_CHECK_EQ(row->encoding(), VectorEncoding::Simple::ROW);
  const auto& type = row->type()->as<TypeKind::ROW>();
  auto childrenSize = type.size();
  VELOX_CHECK_EQ(childrenSize, row->childrenSize());
  auto nulls = readNulls(left, childrenSize);
  for (auto i = 0; i < childrenSize; ++i) {
    auto child = row->childAt(i);
    auto leftNull = bits::isBitSet(nulls.data(), i);
    auto rightNull = child->isNullAt(wrappedIndex);

    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    std::optional<int32_t> result;
    if (child->typeUsesCustomComparison()) {
      result = compareSwitch<true>(left, *child, wrappedIndex, flags);
    } else {
      result = compareSwitch<false>(left, *child, wrappedIndex, flags);
    }

    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return 0;
}

template <bool elementTypeProvidesCustomComparison>
std::optional<int32_t> compareArrays(
    ByteInputStream& left,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t rightSize,
    CompareFlags flags) {
  int leftSize = left.read<int32_t>();
  if (leftSize != rightSize && flags.equalsOnly) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  auto wrappedElements = elements.wrappedVector();
  for (auto i = 0; i < compareSize; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = elements.isNullAt(offset + i);

    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto elementIndex = elements.wrappedIndex(offset + i);
    auto result = compareSwitch<elementTypeProvidesCustomComparison>(
        left, *wrappedElements, elementIndex, flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

template <bool elementTypeProvidesCustomComparison>
std::optional<int32_t> compareArrayIndices(
    ByteInputStream& left,
    const BaseVector& elements,
    folly::Range<const vector_size_t*> rightIndices,
    CompareFlags flags) {
  int32_t leftSize = left.read<int32_t>();
  int32_t rightSize = rightIndices.size();
  if (leftSize != rightSize && flags.equalsOnly) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  auto wrappedElements = elements.wrappedVector();
  for (auto i = 0; i < compareSize; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = elements.isNullAt(rightIndices[i]);

    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto elementIndex = elements.wrappedIndex(rightIndices[i]);
    auto result = compareSwitch<elementTypeProvidesCustomComparison>(
        left, *wrappedElements, elementIndex, flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::ARRAY, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto array = right.wrappedVector()->asUnchecked<ArrayVector>();
  VELOX_CHECK_EQ(array->encoding(), VectorEncoding::Simple::ARRAY);
  auto wrappedIndex = right.wrappedIndex(index);
  if (array->type()->childAt(0)->providesCustomComparison()) {
    return compareArrays<true>(
        left,
        *array->elements(),
        array->offsetAt(wrappedIndex),
        array->sizeAt(wrappedIndex),
        flags);
  } else {
    return compareArrays<false>(
        left,
        *array->elements(),
        array->offsetAt(wrappedIndex),
        array->sizeAt(wrappedIndex),
        flags);
  }
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::MAP, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  auto map = right.wrappedVector()->asUnchecked<MapVector>();
  VELOX_CHECK_EQ(map->encoding(), VectorEncoding::Simple::MAP);
  auto wrappedIndex = right.wrappedIndex(index);
  auto size = map->sizeAt(wrappedIndex);
  std::vector<vector_size_t> indices(size);
  auto rightIndices = map->sortedKeyIndices(wrappedIndex);
  std::optional<int32_t> result;

  if (map->type()->childAt(0)->providesCustomComparison()) {
    result =
        compareArrayIndices<true>(left, *map->mapKeys(), rightIndices, flags);
  } else {
    result =
        compareArrayIndices<false>(left, *map->mapKeys(), rightIndices, flags);
  }

  if (result.has_value() && result.value() == 0) {
    if (map->type()->childAt(1)->providesCustomComparison()) {
      return compareArrayIndices<true>(
          left, *map->mapValues(), rightIndices, flags);
    } else {
      return compareArrayIndices<false>(
          left, *map->mapValues(), rightIndices, flags);
    }
  }
  return result;
}

template <bool typeProvidesCustomComparison>
std::optional<int32_t> compareSwitch(
    ByteInputStream& stream,
    const BaseVector& vector,
    vector_size_t index,
    CompareFlags flags) {
  return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
      compare,
      typeProvidesCustomComparison,
      vector.typeKind(),
      stream,
      vector,
      index,
      flags);
}

// Returns a view over a serialized string with the string as a
// contiguous array of bytes. This may use 'storage' for a temporary
// copy.
StringView readStringView(ByteInputStream& stream, std::string& storage) {
  int32_t size = stream.read<int32_t>();
  auto view = stream.nextView(size);
  if (view.size() == size) {
    // The string is all in one piece, no copy.
    return StringView(view.data(), view.size());
  }
  storage.resize(size);
  memcpy(storage.data(), view.data(), view.size());
  stream.readBytes(storage.data() + view.size(), size - view.size());
  return StringView(storage);
}

// Comparison of two serializations.
template <bool typeProvidesCustomComparison>
std::optional<int32_t> compareSwitch(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags);

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<
        Kind != TypeKind::VARCHAR && Kind != TypeKind::VARBINARY &&
            Kind != TypeKind::ARRAY && Kind != TypeKind::MAP &&
            Kind != TypeKind::ROW,
        int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  using T = typename TypeTraits<Kind>::NativeType;
  T leftValue = left.read<T>();
  T rightValue = right.read<T>();

  int result;
  if constexpr (typeProvidesCustomComparison) {
    result =
        SimpleVector<T>::template comparePrimitiveAscWithCustomComparison<Kind>(
            type, leftValue, rightValue);
  } else {
    result = SimpleVector<T>::comparePrimitiveAsc(leftValue, rightValue);
  }

  return flags.ascending ? result : result * -1;
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::VARCHAR, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  std::string leftStorage;
  std::string rightStorage;
  StringView leftValue = readStringView(left, leftStorage);
  StringView rightValue = readStringView(right, rightStorage);
  return flags.ascending ? leftValue.compare(rightValue)
                         : rightValue.compare(leftValue);
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::VARBINARY, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  std::string leftStorage;
  std::string rightStorage;
  StringView leftValue = readStringView(left, leftStorage);
  StringView rightValue = readStringView(right, rightStorage);
  return flags.ascending ? leftValue.compare(rightValue)
                         : rightValue.compare(leftValue);
}

template <bool elementTypeProvidesCustomComparison>
std::optional<int32_t> compareArrays(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* elementType,
    CompareFlags flags) {
  auto leftSize = left.read<int32_t>();
  auto rightSize = right.read<int32_t>();
  if (flags.equalsOnly && leftSize != rightSize) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  auto rightNulls = readNulls(right, rightSize);
  for (auto i = 0; i < compareSize; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = bits::isBitSet(rightNulls.data(), i);
    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    auto result = compareSwitch<elementTypeProvidesCustomComparison>(
        left, right, elementType, flags);
    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::ROW, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  const auto& rowType = type->as<TypeKind::ROW>();
  int size = rowType.size();
  auto leftNulls = readNulls(left, size);
  auto rightNulls = readNulls(right, size);
  for (auto i = 0; i < size; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = bits::isBitSet(rightNulls.data(), i);
    if (leftNull || rightNull) {
      auto result = BaseVector::compareNulls(leftNull, rightNull, flags);
      if (result.has_value() && result.value() == 0) {
        continue;
      }
      return result;
    }

    std::optional<int32_t> result;
    const auto& childType = rowType.childAt(i);
    if (childType->providesCustomComparison()) {
      result = compareSwitch<true>(left, right, childType.get(), flags);
    } else {
      result = compareSwitch<false>(left, right, childType.get(), flags);
    }

    if (result.has_value() && result.value() == 0) {
      continue;
    }
    return result;
  }
  return 0;
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::ARRAY, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  const auto& elementType = type->childAt(0);

  if (elementType->providesCustomComparison()) {
    return compareArrays<true>(left, right, elementType.get(), flags);
  } else {
    return compareArrays<false>(left, right, elementType.get(), flags);
  }
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::MAP, int32_t> = 0>
std::optional<int32_t> compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  std::optional<int32_t> result;
  const auto& keyType = type->childAt(0);
  const auto& valueType = type->childAt(1);

  if (keyType->providesCustomComparison()) {
    result = compareArrays<true>(left, right, keyType.get(), flags);
  } else {
    result = compareArrays<false>(left, right, keyType.get(), flags);
  }

  if (result.has_value() && result.value() == 0) {
    if (valueType->providesCustomComparison()) {
      return compareArrays<true>(left, right, valueType.get(), flags);
    } else {
      return compareArrays<false>(left, right, valueType.get(), flags);
    }
  }
  return result;
}

template <bool typeProvidesCustomComparison>
std::optional<int32_t> compareSwitch(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
      compare,
      typeProvidesCustomComparison,
      type->kind(),
      left,
      right,
      type,
      flags);
}

// Hash functions.
template <bool typeProvidesCustomComparison>
uint64_t hashSwitch(ByteInputStream& stream, const Type* type);

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<
        Kind != TypeKind::VARBINARY && Kind != TypeKind::VARCHAR &&
            Kind != TypeKind::ARRAY && Kind != TypeKind::MAP &&
            Kind != TypeKind::ROW,
        int32_t> = 0>
uint64_t hashOne(ByteInputStream& stream, const Type* type) {
  using T = typename TypeTraits<Kind>::NativeType;

  T value = stream.read<T>();

  if constexpr (typeProvidesCustomComparison) {
    return static_cast<const CanProvideCustomComparisonType<Kind>*>(type)->hash(
        value);
  } else if constexpr (std::is_floating_point_v<T>) {
    return util::floating_point::NaNAwareHash<T>()(value);
  } else {
    return folly::hasher<T>()(value);
  }
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::VARCHAR, int32_t> = 0>
uint64_t hashOne(ByteInputStream& stream, const Type* /*type*/) {
  std::string storage;
  return folly::hasher<StringView>()(readStringView(stream, storage));
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::VARBINARY, int32_t> = 0>
uint64_t hashOne(ByteInputStream& stream, const Type* /*type*/) {
  std::string storage;
  return folly::hasher<StringView>()(readStringView(stream, storage));
}

template <bool elementTypeProvidesCustomComparison>
uint64_t
hashArray(ByteInputStream& in, uint64_t hash, const Type* elementType) {
  auto size = in.read<int32_t>();
  auto nulls = readNulls(in, size);
  for (auto i = 0; i < size; ++i) {
    uint64_t value;
    if (bits::isBitSet(nulls.data(), i)) {
      value = BaseVector::kNullHash;
    } else {
      value = hashSwitch<elementTypeProvidesCustomComparison>(in, elementType);
    }
    hash = bits::commutativeHashMix(hash, value);
  }
  return hash;
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::ROW, int32_t> = 0>
uint64_t hashOne(ByteInputStream& in, const Type* type) {
  auto size = type->size();
  auto nulls = readNulls(in, size);
  uint64_t hash = BaseVector::kNullHash;
  for (auto i = 0; i < size; ++i) {
    uint64_t value;
    if (bits::isBitSet(nulls.data(), i)) {
      value = BaseVector::kNullHash;
    } else {
      const auto& childType = type->childAt(i);
      if (childType->providesCustomComparison()) {
        value = hashSwitch<true>(in, childType.get());
      } else {
        value = hashSwitch<false>(in, childType.get());
      }
    }
    hash = i == 0 ? value : bits::hashMix(hash, value);
  }
  return hash;
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::ARRAY, int32_t> = 0>
uint64_t hashOne(ByteInputStream& in, const Type* type) {
  const auto& elementType = type->childAt(0);

  if (elementType->providesCustomComparison()) {
    return hashArray<true>(in, BaseVector::kNullHash, elementType.get());
  } else {
    return hashArray<false>(in, BaseVector::kNullHash, elementType.get());
  }
}

template <
    bool typeProvidesCustomComparison,
    TypeKind Kind,
    std::enable_if_t<Kind == TypeKind::MAP, int32_t> = 0>
uint64_t hashOne(ByteInputStream& in, const Type* type) {
  const auto& keyType = type->childAt(0);
  const auto& valueType = type->childAt(1);

  uint64_t hash;
  if (keyType->providesCustomComparison()) {
    hash = hashArray<true>(in, BaseVector::kNullHash, keyType.get());
  } else {
    hash = hashArray<false>(in, BaseVector::kNullHash, keyType.get());
  }

  if (valueType->providesCustomComparison()) {
    return hashArray<true>(in, hash, valueType.get());
  } else {
    return hashArray<false>(in, hash, valueType.get());
  }
}

template <bool typeProvidesCustomComparison>
uint64_t hashSwitch(ByteInputStream& in, const Type* type) {
  return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
      hashOne, typeProvidesCustomComparison, type->kind(), in, type);
}

} // namespace

// static
void ContainerRowSerde::serialize(
    const BaseVector& source,
    vector_size_t index,
    ByteOutputStream& out,
    const ContainerRowSerdeOptions& options) {
  VELOX_DCHECK(
      !source.isNullAt(index), "Null top-level values are not supported");
  serializeSwitch(source, index, out, options);
}

// static
void ContainerRowSerde::deserialize(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector* result) {
  deserializeSwitch(in, index, *result);
}

// static
int32_t ContainerRowSerde::compare(
    ByteInputStream& left,
    const DecodedVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(
      !right.isNullAt(index), "Null top-level values are not supported");
  VELOX_DCHECK(flags.nullAsValue(), "not supported null handling mode");
  if (right.base()->typeUsesCustomComparison()) {
    return compareSwitch<true>(left, *right.base(), right.index(index), flags)
        .value();
  } else {
    return compareSwitch<false>(left, *right.base(), right.index(index), flags)
        .value();
  }
}

// static
int32_t ContainerRowSerde::compare(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  VELOX_DCHECK(flags.nullAsValue(), "not supported null handling mode");

  if (type->providesCustomComparison()) {
    return compareSwitch<true>(left, right, type, flags).value();
  } else {
    return compareSwitch<false>(left, right, type, flags).value();
  }
}

std::optional<int32_t> ContainerRowSerde::compareWithNulls(
    ByteInputStream& left,
    const DecodedVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(
      !right.isNullAt(index), "Null top-level values are not supported");
  if (right.base()->typeUsesCustomComparison()) {
    return compareSwitch<true>(left, *right.base(), right.index(index), flags);
  } else {
    return compareSwitch<false>(left, *right.base(), right.index(index), flags);
  }
}

std::optional<int32_t> ContainerRowSerde::compareWithNulls(
    ByteInputStream& left,
    ByteInputStream& right,
    const Type* type,
    CompareFlags flags) {
  if (type->providesCustomComparison()) {
    return compareSwitch<true>(left, right, type, flags);
  } else {
    return compareSwitch<false>(left, right, type, flags);
  }
}

// static
uint64_t ContainerRowSerde::hash(ByteInputStream& in, const Type* type) {
  if (type->providesCustomComparison()) {
    return hashSwitch<true>(in, type);
  } else {
    return hashSwitch<false>(in, type);
  }
}

} // namespace facebook::velox::exec
