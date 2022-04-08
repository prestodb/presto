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
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

const ContainerRowSerde& ContainerRowSerde::instance() {
  static auto instance = std::make_unique<ContainerRowSerde>();
  return *instance;
}

namespace {

// Copy from vector to stream.
void serializeSwitch(
    const BaseVector& source,
    vector_size_t index,
    ByteStream& out);

template <TypeKind Kind>
void serializeOne(
    const BaseVector& vector,
    vector_size_t index,
    ByteStream& stream) {
  using T = typename TypeTraits<Kind>::NativeType;
  stream.appendOne<T>(vector.asUnchecked<SimpleVector<T>>()->valueAt(index));
}

template <>
void serializeOne<TypeKind::VARCHAR>(
    const BaseVector& vector,
    vector_size_t index,
    ByteStream& stream) {
  auto string = vector.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  stream.appendOne<int32_t>(string.size());
  stream.appendStringPiece(folly::StringPiece(string.data(), string.size()));
}

template <>
void serializeOne<TypeKind::VARBINARY>(
    const BaseVector& vector,
    vector_size_t index,
    ByteStream& stream) {
  auto string = vector.asUnchecked<SimpleVector<StringView>>()->valueAt(index);
  stream.appendOne<int32_t>(string.size());
  stream.appendStringPiece(folly::StringPiece(string.data(), string.size()));
}

template <>
void serializeOne<TypeKind::ROW>(
    const BaseVector& vector,
    vector_size_t index,
    ByteStream& out) {
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
      serializeSwitch(*children[i], wrappedIndex, out);
    }
  }
}

void writeNulls(
    const BaseVector& values,
    vector_size_t offset,
    vector_size_t size,
    ByteStream& out) {
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
    ByteStream& out) {
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
    BaseVector& elements,
    vector_size_t offset,
    vector_size_t size,
    ByteStream& out) {
  out.appendOne<int32_t>(size);
  writeNulls(elements, offset, size, out);
  for (auto i = 0; i < size; ++i) {
    if (!elements.isNullAt(i + offset)) {
      serializeSwitch(elements, i + offset, out);
    }
  }
}

void serializeArray(
    BaseVector& elements,
    folly::Range<const vector_size_t*> indices,
    ByteStream& out) {
  out.appendOne<int32_t>(indices.size());
  writeNulls(elements, indices, out);
  for (auto i : indices) {
    if (!elements.isNullAt(i)) {
      serializeSwitch(elements, i, out);
    }
  }
}

template <>
void serializeOne<TypeKind::ARRAY>(
    const BaseVector& source,
    vector_size_t index,
    ByteStream& out) {
  auto array = source.wrappedVector()->asUnchecked<ArrayVector>();
  auto wrappedIndex = source.wrappedIndex(index);
  serializeArray(
      *array->elements(),
      array->offsetAt(wrappedIndex),
      array->sizeAt(wrappedIndex),
      out);
}

template <>
void serializeOne<TypeKind::MAP>(
    const BaseVector& vector,
    vector_size_t index,
    ByteStream& out) {
  auto map = vector.wrappedVector()->asUnchecked<MapVector>();
  auto wrappedIndex = vector.wrappedIndex(index);
  auto size = map->sizeAt(wrappedIndex);
  auto offset = map->offsetAt(wrappedIndex);
  auto indices = map->sortedKeyIndices(wrappedIndex);
  serializeArray(*map->mapKeys(), indices, out);
  serializeArray(*map->mapValues(), indices, out);
}

void serializeSwitch(
    const BaseVector& source,
    vector_size_t index,
    ByteStream& stream) {
  VELOX_DYNAMIC_TYPE_DISPATCH(
      serializeOne, source.typeKind(), source, index, stream);
}

// Copy from serialization to vector.
void deserializeSwitch(ByteStream& in, vector_size_t index, BaseVector& result);

template <TypeKind Kind>
void deserializeOne(ByteStream& in, vector_size_t index, BaseVector& result) {
  using T = typename TypeTraits<Kind>::NativeType;
  // Check that the vector is writable. This is faster than dynamic_cast.
  VELOX_CHECK(result.encoding() == VectorEncoding::Simple::FLAT);
  auto values = result.asUnchecked<FlatVector<T>>();
  values->set(index, in.read<T>());
}

void deserializeString(
    ByteStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_CHECK(result.encoding() == VectorEncoding::Simple::FLAT);
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
    ByteStream& in,
    vector_size_t index,
    BaseVector& result) {
  deserializeString(in, index, result);
}

template <>
void deserializeOne<TypeKind::VARBINARY>(
    ByteStream& in,
    vector_size_t index,
    BaseVector& result) {
  deserializeString(in, index, result);
}

std::vector<uint64_t> readNulls(ByteStream& in, int32_t size) {
  auto n = bits::nwords(size);
  std::vector<uint64_t> nulls(n);
  for (auto i = 0; i < n; ++i) {
    nulls[i] = in.read<uint64_t>();
  }
  return nulls;
}

template <>
void deserializeOne<TypeKind::ROW>(
    ByteStream& in,
    vector_size_t index,
    BaseVector& result) {
  const auto& type = result.type()->as<TypeKind::ROW>();
  VELOX_CHECK(result.encoding() == VectorEncoding::Simple::ROW);
  auto row = result.asUnchecked<RowVector>();
  auto childrenSize = type.size();
  VELOX_CHECK(childrenSize == row->childrenSize());
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
}

// Reads the size, null flags and deserializes from 'in', appending to
// the end of 'elements'. Returns the number of added elements and
// sets 'offset' to the index of the first added element.
vector_size_t
deserializeArray(ByteStream& in, BaseVector& elements, vector_size_t& offset) {
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
    ByteStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_CHECK(result.encoding() == VectorEncoding::Simple::ARRAY);
  auto array = result.asUnchecked<ArrayVector>();
  if (array->size() <= index) {
    array->resize(index + 1);
  }
  vector_size_t offset;
  auto size = deserializeArray(in, *array->elements(), offset);
  array->setOffsetAndSize(index, offset, size);
}

template <>
void deserializeOne<TypeKind::MAP>(
    ByteStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_CHECK(result.encoding() == VectorEncoding::Simple::MAP);
  auto map = result.asUnchecked<MapVector>();
  if (map->size() <= index) {
    map->resize(index + 1);
  }
  vector_size_t keyOffset;
  auto keySize = deserializeArray(in, *map->mapKeys(), keyOffset);
  vector_size_t valueOffset;
  auto valueSize = deserializeArray(in, *map->mapValues(), valueOffset);
  VELOX_CHECK(keySize == valueSize);
  VELOX_CHECK(keyOffset == valueOffset);
  map->setOffsetAndSize(index, keyOffset, keySize);
}

void deserializeSwitch(
    ByteStream& in,
    vector_size_t index,
    BaseVector& result) {
  VELOX_DYNAMIC_TYPE_DISPATCH(
      deserializeOne, result.typeKind(), in, index, result);
}

// Comparison of serialization and vector.
int compareSwitch(
    ByteStream& stream,
    const BaseVector& vector,
    vector_size_t index,
    CompareFlags flags);

template <TypeKind Kind>
int32_t compare(
    ByteStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");
  using T = typename TypeTraits<Kind>::NativeType;
  auto rightValue = right.asUnchecked<SimpleVector<T>>()->valueAt(index);
  auto leftValue = left.read<T>();
  auto result = leftValue < rightValue ? -1 : leftValue == rightValue ? 0 : 1;
  return flags.ascending ? result : result * -1;
}

int compareStringAsc(
    ByteStream& left,
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

template <>
int compare<TypeKind::VARCHAR>(
    ByteStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  auto result = compareStringAsc(left, right, index, flags.equalsOnly);
  return flags.ascending ? result : result * -1;
}

template <>
int compare<TypeKind::VARBINARY>(
    ByteStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  auto result = compareStringAsc(left, right, index, flags.equalsOnly);
  return flags.ascending ? result : result * -1;
}

template <>
int compare<TypeKind::ROW>(
    ByteStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  auto row = right.wrappedVector()->asUnchecked<RowVector>();
  auto wrappedIndex = right.wrappedIndex(index);
  VELOX_CHECK(row->encoding() == VectorEncoding::Simple::ROW);
  const auto& type = row->type()->as<TypeKind::ROW>();
  auto childrenSize = type.size();
  VELOX_CHECK(childrenSize == row->childrenSize());
  auto nulls = readNulls(left, childrenSize);
  for (auto i = 0; i < childrenSize; ++i) {
    auto child = row->childAt(i);
    auto leftNull = bits::isBitSet(nulls.data(), i);
    auto rightNull = child->isNullAt(wrappedIndex);
    if (leftNull && rightNull) {
      continue;
    }
    if (leftNull) {
      return flags.nullsFirst ? -1 : 1;
    }
    if (rightNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    auto result = compareSwitch(left, *child, wrappedIndex, flags);
    if (result) {
      return result;
    }
  }
  return 0;
}

int32_t compareArrays(
    ByteStream& left,
    BaseVector& elements,
    vector_size_t offset,
    vector_size_t rightSize,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  int leftSize = left.read<int32_t>();
  if (leftSize != rightSize && flags.equalsOnly) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  auto wrappedElements = elements.wrappedVector();
  for (auto i = 0; i < compareSize; ++i) {
    auto elementIndex = elements.wrappedIndex(offset + i);
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = wrappedElements->isNullAt(elementIndex);
    if (leftNull) {
      if (rightNull) {
        continue;
      }
      return flags.nullsFirst ? -1 : 1;
    } else if (rightNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    int result = compareSwitch(left, *wrappedElements, elementIndex, flags);
    if (result) {
      return result;
    }
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

int32_t compareArrayIndices(
    ByteStream& left,
    BaseVector& elements,
    folly::Range<const vector_size_t*> rightIndices,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  int32_t leftSize = left.read<int32_t>();
  int32_t rightSize = rightIndices.size();
  if (leftSize != rightSize && flags.equalsOnly) {
    return flags.ascending ? 1 : -1;
  }
  auto compareSize = std::min(leftSize, rightSize);
  auto leftNulls = readNulls(left, leftSize);
  auto wrappedElements = elements.wrappedVector();
  for (auto i = 0; i < compareSize; ++i) {
    auto elementIndex = elements.wrappedIndex(rightIndices[i]);
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = wrappedElements->isNullAt(elementIndex);
    if (leftNull) {
      if (rightNull) {
        continue;
      }
      return flags.nullsFirst ? -1 : 1;
    } else if (rightNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    int result = compareSwitch(left, *wrappedElements, elementIndex, flags);
    if (result) {
      return result;
    }
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

template <>
int compare<TypeKind::ARRAY>(
    ByteStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  auto array = right.wrappedVector()->asUnchecked<ArrayVector>();
  VELOX_CHECK(array->encoding() == VectorEncoding::Simple::ARRAY);
  auto wrappedIndex = right.wrappedIndex(index);
  return compareArrays(
      left,
      *array->elements(),
      array->offsetAt(wrappedIndex),
      array->sizeAt(wrappedIndex),
      flags);
}

template <>
int compare<TypeKind::MAP>(
    ByteStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  auto map = right.wrappedVector()->asUnchecked<MapVector>();
  VELOX_CHECK(map->encoding() == VectorEncoding::Simple::MAP);
  auto wrappedIndex = right.wrappedIndex(index);
  auto size = map->sizeAt(wrappedIndex);
  std::vector<vector_size_t> indices(size);
  auto rightIndices = map->sortedKeyIndices(wrappedIndex);
  auto result = compareArrayIndices(left, *map->mapKeys(), rightIndices, flags);
  if (result) {
    return result;
  }
  return compareArrayIndices(left, *map->mapValues(), rightIndices, flags);
}

int32_t compareSwitch(
    ByteStream& stream,
    const BaseVector& vector,
    vector_size_t index,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  return VELOX_DYNAMIC_TYPE_DISPATCH(
      compare, vector.typeKind(), stream, vector, index, flags);
}

// Returns a view over a serialized string with the string as a
// contiguous array of bytes. This may use 'storage' for a temporary
// copy.
StringView readStringView(ByteStream& stream, std::string& storage) {
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
int32_t compareSwitch(
    ByteStream& left,
    ByteStream& right,
    const Type* type,
    CompareFlags flags);

template <TypeKind Kind>
int32_t compare(
    ByteStream& left,
    ByteStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  using T = typename TypeTraits<Kind>::NativeType;
  T leftValue = left.read<T>();
  T rightValue = right.read<T>();
  auto result = leftValue == rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  return flags.ascending ? result : result * -1;
}

template <>
int32_t compare<TypeKind::VARCHAR>(
    ByteStream& left,
    ByteStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  std::string leftStorage;
  std::string rightStorage;
  StringView leftValue = readStringView(left, leftStorage);
  StringView rightValue = readStringView(right, rightStorage);
  return flags.ascending ? leftValue.compare(rightValue)
                         : rightValue.compare(leftValue);
}

template <>
int32_t compare<TypeKind::VARBINARY>(
    ByteStream& left,
    ByteStream& right,
    const Type* /*type*/,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  std::string leftStorage;
  std::string rightStorage;
  StringView leftValue = readStringView(left, leftStorage);
  StringView rightValue = readStringView(right, rightStorage);
  return flags.ascending ? leftValue.compare(rightValue)
                         : rightValue.compare(leftValue);
}

int32_t compareArrays(
    ByteStream& left,
    ByteStream& right,
    const Type* elementType,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

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
    if (leftNull && rightNull) {
      continue;
    }
    if (leftNull) {
      return flags.nullsFirst ? -1 : 1;
    }
    if (rightNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    auto result = compareSwitch(left, right, elementType, flags);
    if (result) {
      return result;
    }
  }
  return flags.ascending ? (leftSize - rightSize) : (rightSize - leftSize);
}

template <>
int32_t compare<TypeKind::ROW>(
    ByteStream& left,
    ByteStream& right,
    const Type* type,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  const auto& rowType = type->as<TypeKind::ROW>();
  int size = rowType.size();
  auto leftNulls = readNulls(left, size);
  auto rightNulls = readNulls(right, size);
  for (auto i = 0; i < size; ++i) {
    bool leftNull = bits::isBitSet(leftNulls.data(), i);
    bool rightNull = bits::isBitSet(rightNulls.data(), i);
    if (leftNull && rightNull) {
      continue;
    }
    if (leftNull) {
      return flags.nullsFirst ? -1 : 1;
    }
    if (rightNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    auto result = compareSwitch(left, right, rowType.childAt(i).get(), flags);
    if (result) {
      return result;
    }
  }
  return 0;
}

template <>
int32_t compare<TypeKind::ARRAY>(
    ByteStream& left,
    ByteStream& right,
    const Type* type,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  return compareArrays(left, right, type->childAt(0).get(), flags);
}

template <>
int32_t compare<TypeKind::MAP>(
    ByteStream& left,
    ByteStream& right,
    const Type* type,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  auto result = compareArrays(left, right, type->childAt(0).get(), flags);
  if (result) {
    return result;
  }
  return compareArrays(left, right, type->childAt(1).get(), flags);
}

int32_t compareSwitch(
    ByteStream& left,
    ByteStream& right,
    const Type* type,
    CompareFlags flags) {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  return VELOX_DYNAMIC_TYPE_DISPATCH(
      compare, type->kind(), left, right, type, flags);
}

// Hash functions.
uint64_t hashSwitch(ByteStream& stream, const Type* type);

template <TypeKind Kind>
uint64_t hashOne(ByteStream& stream, const Type* /*type*/) {
  using T = typename TypeTraits<Kind>::NativeType;
  return folly::hasher<T>()(stream.read<T>());
}

template <>
uint64_t hashOne<TypeKind::VARCHAR>(ByteStream& stream, const Type* /*type*/) {
  std::string storage;
  return folly::hasher<StringView>()(readStringView(stream, storage));
}

template <>
uint64_t hashOne<TypeKind::VARBINARY>(
    ByteStream& stream,
    const Type* /*type*/) {
  std::string storage;
  return folly::hasher<StringView>()(readStringView(stream, storage));
}

uint64_t hashArray(ByteStream& in, uint64_t hash, const Type* elementType) {
  auto size = in.read<int32_t>();
  auto nulls = readNulls(in, size);
  for (auto i = 0; i < size; ++i) {
    uint64_t value;
    if (bits::isBitSet(nulls.data(), i)) {
      value = BaseVector::kNullHash;
    } else {
      value = hashSwitch(in, elementType);
    }
    hash = bits::commutativeHashMix(hash, value);
  }
  return hash;
}

template <>
uint64_t hashOne<TypeKind::ROW>(ByteStream& in, const Type* type) {
  auto size = type->size();
  auto nulls = readNulls(in, size);
  uint64_t hash = BaseVector::kNullHash;
  for (auto i = 0; i < size; ++i) {
    uint64_t value;
    if (bits::isBitSet(nulls.data(), i)) {
      value = BaseVector::kNullHash;
    } else {
      value = hashSwitch(in, type->childAt(i).get());
    }
    hash = i == 0 ? value : bits::hashMix(hash, value);
  }
  return hash;
}

template <>
uint64_t hashOne<TypeKind::ARRAY>(ByteStream& in, const Type* type) {
  return hashArray(in, BaseVector::kNullHash, type->childAt(0).get());
}

template <>
uint64_t hashOne<TypeKind::MAP>(ByteStream& in, const Type* type) {
  return hashArray(
      in,
      hashArray(in, BaseVector::kNullHash, type->childAt(0).get()),
      type->childAt(1).get());
}

uint64_t hashSwitch(ByteStream& in, const Type* type) {
  return VELOX_DYNAMIC_TYPE_DISPATCH(hashOne, type->kind(), in, type);
}

} // namespace

void ContainerRowSerde::serialize(
    const BaseVector& source,
    vector_size_t index,
    ByteStream& out) const {
  serializeSwitch(source, index, out);
}

void ContainerRowSerde::deserialize(
    ByteStream& in,
    vector_size_t index,
    BaseVector* result) const {
  deserializeSwitch(in, index, *result);
}

int32_t ContainerRowSerde::compare(
    ByteStream& left,
    const DecodedVector& right,
    vector_size_t index,
    CompareFlags flags) const {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  return compareSwitch(left, *right.base(), right.index(index), flags);
}

int32_t ContainerRowSerde::compare(
    ByteStream& left,
    ByteStream& right,
    const Type* type,
    CompareFlags flags) const {
  VELOX_DCHECK(!flags.stopAtNull, "not supported compare flag");

  return compareSwitch(left, right, type, flags);
}

uint64_t ContainerRowSerde::hash(ByteStream& in, const Type* type) const {
  return hashSwitch(in, type);
}

} // namespace facebook::velox::exec
