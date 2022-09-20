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
#include "velox/vector/VectorSaver.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox {

namespace {

enum class Encoding : int8_t {
  kFlat = 0,
  kConstant = 1,
  kDictionary = 2,
};

template <typename T>
void write(const T& value, std::ostream& out) {
  out.write((char*)&value, sizeof(T));
}

template <>
void write<bool>(const bool& value, std::ostream& out) {
  write<int8_t>(value ? 1 : 0, out);
}

template <>
void write<std::string>(const std::string& value, std::ostream& out) {
  write<int32_t>(value.size(), out);
  out.write(value.data(), value.size());
}

template <typename T>
T read(std::istream& in) {
  T value;
  in.read((char*)&value, sizeof(T));
  return value;
}

template <>
bool read<bool>(std::istream& in) {
  int8_t value = read<int8_t>(in);
  return value != 0;
}

template <>
std::string read<std::string>(std::istream& in) {
  auto size = read<int32_t>(in);
  std::string data;
  data.resize(size);
  in.read(data.data(), size);
  return data;
}

void writeEncoding(VectorEncoding::Simple encoding, std::ostream& out) {
  switch (encoding) {
    case VectorEncoding::Simple::FLAT:
    case VectorEncoding::Simple::ROW:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
      write<int32_t>((int8_t)Encoding::kFlat, out);
      return;
    case VectorEncoding::Simple::CONSTANT:
      write<int32_t>((int8_t)Encoding::kConstant, out);
      return;
    case VectorEncoding::Simple::DICTIONARY:
      write<int32_t>((int8_t)Encoding::kDictionary, out);
      return;
    default:
      VELOX_UNSUPPORTED("Unsupported encoding: {}", mapSimpleToName(encoding));
  }
}

Encoding readEncoding(std::istream& in) {
  auto encoding = Encoding(read<int32_t>(in));
  switch (encoding) {
    case Encoding::kFlat:
    case Encoding::kConstant:
    case Encoding::kDictionary:
      return encoding;
    default:
      VELOX_UNSUPPORTED("Unsupported encoding: {}", encoding);
  }
}

void writeBuffer(const BufferPtr& buffer, std::ostream& out) {
  write<int32_t>(buffer->size(), out);
  out.write(buffer->as<char>(), buffer->size());
}

void writeOptionalBuffer(const BufferPtr& buffer, std::ostream& out) {
  if (buffer) {
    write<bool>(true, out);
    writeBuffer(buffer, out);
  } else {
    write<bool>(false, out);
  }
}

BufferPtr readBuffer(std::istream& in, memory::MemoryPool* pool) {
  auto numBytes = read<int32_t>(in);
  auto buffer = AlignedBuffer::allocate<char>(numBytes, pool);
  auto rawBuffer = buffer->asMutable<char>();
  in.read(rawBuffer, numBytes);
  return buffer;
}

BufferPtr readOptionalBuffer(std::istream& in, memory::MemoryPool* pool) {
  bool hasBuffer = read<bool>(in);
  if (hasBuffer) {
    return readBuffer(in, pool);
  }

  return nullptr;
}

template <TypeKind kind>
VectorPtr createFlat(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    BufferPtr nulls,
    BufferPtr values,
    std::vector<BufferPtr> stringBuffers) {
  using T = typename TypeTraits<kind>::NativeType;

  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      std::move(nulls),
      size,
      std::move(values),
      std::move(stringBuffers));
}

int32_t computeStringOffset(
    StringView value,
    const std::vector<BufferPtr>& stringBuffers) {
  int32_t offset = 0;
  for (const auto& buffer : stringBuffers) {
    auto start = buffer->as<char>();

    if (value.data() >= start && value.data() < start + buffer->size()) {
      return (value.data() - start) + offset;
    }

    offset += buffer->size();
  }

  VELOX_FAIL("String view points outside of the string buffers");
}

const char* computeStringPointer(
    int32_t offset,
    const std::vector<BufferPtr>& stringBuffers) {
  int32_t totalOffset = 0;
  for (const auto& buffer : stringBuffers) {
    auto size = buffer->size();
    if (offset >= totalOffset && offset < totalOffset + size) {
      return buffer->as<char>() + offset - totalOffset;
    }

    totalOffset += buffer->size();
  }

  VELOX_FAIL("String offset is outside of the string buffers: {}", offset);
}

void writeStringViews(
    vector_size_t size,
    const BufferPtr& strings,
    const std::vector<BufferPtr>& stringBuffers,
    std::ostream& out) {
  write<int32_t>(strings->size(), out);

  auto rawBytes = strings->as<char>();
  auto rawValues = strings->as<StringView>();
  for (auto i = 0; i < size; ++i) {
    auto stringView = rawValues[i];
    if (stringView.isInline()) {
      out.write(rawBytes + i * sizeof(StringView), sizeof(StringView));
    } else {
      // Size.
      write<int32_t>(stringView.size(), out);

      // 4 bytes of zeros for prefix. We'll fill this in appropriately when
      // deserializing.
      write<int32_t>(0, out);

      // Offset.
      auto offset = computeStringOffset(stringView, stringBuffers);
      write<int64_t>(offset, out);
    }
  }
}

void restoreVectorStringViews(
    vector_size_t size,
    const BufferPtr& strings,
    const std::vector<BufferPtr>& stringBuffers) {
  auto rawBytes = strings->as<char>();
  auto rawValues = strings->asMutable<StringView>();
  for (auto i = 0; i < size; ++i) {
    auto value = rawValues[i];
    if (!value.isInline()) {
      auto offset = *reinterpret_cast<const int64_t*>(
          rawBytes + i * sizeof(StringView) + 8);
      rawValues[i] =
          StringView(computeStringPointer(offset, stringBuffers), value.size());
    }
  }
}

bool isVarcharOrVarbinary(const BaseVector& vector) {
  return vector.typeKind() == TypeKind::VARCHAR ||
      vector.typeKind() == TypeKind::VARBINARY;
}

void writeFlatVector(const BaseVector& vector, std::ostream& out) {
  // Nulls buffer.
  writeOptionalBuffer(vector.nulls(), out);

  // Values buffer.
  if (isVarcharOrVarbinary(vector)) {
    const auto& values = vector.values();
    if (values) {
      write<bool>(true, out);

      const auto& stringBuffers =
          vector.asFlatVector<StringView>()->stringBuffers();
      writeStringViews(vector.size(), values, stringBuffers, out);
    } else {
      write<bool>(false, out);
    }
  } else {
    writeOptionalBuffer(vector.values(), out);
  }

  // String buffers.
  if (isVarcharOrVarbinary(vector)) {
    const auto& stringBuffers =
        vector.asFlatVector<StringView>()->stringBuffers();
    write<int32_t>(stringBuffers.size(), out);

    for (const auto& buffer : stringBuffers) {
      writeBuffer(buffer, out);
    }
  }
}

VectorPtr readFlatVector(
    const TypePtr& type,
    vector_size_t size,
    std::istream& in,
    memory::MemoryPool* pool) {
  // Nulls buffer.
  BufferPtr nulls = readOptionalBuffer(in, pool);

  // Values buffer.
  BufferPtr values = readOptionalBuffer(in, pool);

  // String buffers.
  std::vector<BufferPtr> stringBuffers;
  if (type->isVarchar() || type->isVarbinary()) {
    int32_t numStringBuffers = read<int32_t>(in);
    for (auto i = 0; i < numStringBuffers; ++i) {
      stringBuffers.push_back(readBuffer(in, pool));
    }

    // Update the pointers in the StringViews.
    if (values) {
      restoreVectorStringViews(size, values, stringBuffers);
    }
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      createFlat, type->kind(), type, size, pool, nulls, values, stringBuffers);
}

template <TypeKind kind>
void writeScalarConstant(const BaseVector& vector, std::ostream& out) {
  using T = typename TypeTraits<kind>::NativeType;

  auto value = vector.as<ConstantVector<T>>()->valueAt(0);
  out.write((const char*)&value, sizeof(T));

  if constexpr (std::is_same_v<T, StringView>) {
    if (!value.isInline()) {
      write<int32_t>(value.size(), out);
      out.write(value.data(), value.size());
    }
  }
}

void writeConstantVector(const BaseVector& vector, std::ostream& out) {
  bool isNull = vector.isNullAt(0);
  write<bool>(isNull, out);

  if (isNull) {
    return;
  }

  auto baseVector = vector.valueVector();
  write<bool>(baseVector == nullptr, out);

  if (baseVector) {
    saveVector(*baseVector, out);
    write<int32_t>(vector.as<ConstantVector<ComplexType>>()->index(), out);
  } else {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        writeScalarConstant, vector.typeKind(), vector, out);
  }
}

template <TypeKind kind>
VectorPtr readConstant(
    const TypePtr& /* type */,
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    std::istream& in) {
  using T = typename TypeTraits<kind>::NativeType;

  T value = read<T>(in);

  if constexpr (std::is_same_v<T, StringView>) {
    if (!value.isInline()) {
      auto stringSize = read<int32_t>(in);
      BufferPtr stringBuffer = AlignedBuffer::allocate<char>(stringSize, pool);
      in.read(stringBuffer->template asMutable<char>(), stringSize);

      return std::make_shared<ConstantVector<T>>(
          pool, size, false, StringView(stringBuffer->as<char>(), stringSize));
    }
  }

  return std::make_shared<ConstantVector<T>>(
      pool, size, false, std::move(value));
}

VectorPtr readConstantVector(
    const TypePtr& type,
    vector_size_t size,
    std::istream& in,
    memory::MemoryPool* pool) {
  // Is-null flag.
  auto isNull = read<bool>(in);

  if (isNull) {
    return BaseVector::createNullConstant(type, size, pool);
  }

  bool scalar = read<bool>(in);
  if (scalar) {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        readConstant, type->kind(), type, size, pool, in);
  }

  auto baseVector = restoreVector(in, pool);
  auto baseIndex = read<int32_t>(in);

  return BaseVector::wrapInConstant(size, baseIndex, baseVector);
}

void writeDictionaryVector(const BaseVector& vector, std::ostream& out) {
  // Nulls buffer.
  writeOptionalBuffer(vector.nulls(), out);

  // Indices buffer.
  writeBuffer(vector.wrapInfo(), out);

  // Base vector.
  saveVector(*vector.valueVector(), out);
}

VectorPtr readDictionaryVector(
    const TypePtr& /*type*/,
    vector_size_t size,
    std::istream& in,
    memory::MemoryPool* pool) {
  // Nulls buffer.
  BufferPtr nulls = readOptionalBuffer(in, pool);

  // Indices buffer.
  BufferPtr indices = readBuffer(in, pool);

  // Base vector.
  auto baseVector = restoreVector(in, pool);

  return BaseVector::wrapInDictionary(nulls, indices, size, baseVector);
}

void writeRowVector(const BaseVector& vector, std::ostream& out) {
  // Nulls buffer.
  writeOptionalBuffer(vector.nulls(), out);

  auto rowVector = vector.as<RowVector>();

  // Child vectors.
  auto numChildren = rowVector->childrenSize();
  write<int32_t>(numChildren, out);

  for (auto i = 0; i < numChildren; ++i) {
    const auto& child = rowVector->childAt(i);

    write<bool>(child != nullptr, out);
    if (child) {
      saveVector(*child, out);
    }
  }
}

VectorPtr readRowVector(
    const TypePtr& type,
    vector_size_t size,
    std::istream& in,
    memory::MemoryPool* pool) {
  // Nulls buffer.
  BufferPtr nulls = readOptionalBuffer(in, pool);

  // Child vectors.
  auto numChildren = read<int32_t>(in);
  std::vector<VectorPtr> children;
  children.reserve(numChildren);
  for (auto i = 0; i < numChildren; ++i) {
    bool present = read<bool>(in);
    if (present) {
      children.push_back(restoreVector(in, pool));
    } else {
      children.push_back(nullptr);
    }
  }

  return std::make_shared<RowVector>(pool, type, nulls, size, children);
}

void writeArrayVector(const BaseVector& vector, std::ostream& out) {
  // Nulls buffer.
  writeOptionalBuffer(vector.nulls(), out);

  // Offsets and sizes.
  auto arrayVector = vector.as<ArrayVector>();
  writeBuffer(arrayVector->offsets(), out);
  writeBuffer(arrayVector->sizes(), out);

  // Elements vector.
  saveVector(*arrayVector->elements(), out);
}

VectorPtr readArrayVector(
    const TypePtr& type,
    vector_size_t size,
    std::istream& in,
    memory::MemoryPool* pool) {
  // Nulls buffer.
  BufferPtr nulls = readOptionalBuffer(in, pool);

  BufferPtr offsets = readBuffer(in, pool);
  BufferPtr sizes = readBuffer(in, pool);

  auto elements = restoreVector(in, pool);

  return std::make_shared<ArrayVector>(
      pool, type, nulls, size, offsets, sizes, elements);
}

void writeMapVector(const BaseVector& vector, std::ostream& out) {
  // Nulls buffer.
  writeOptionalBuffer(vector.nulls(), out);

  // Offsets and sizes.
  auto mapVector = vector.as<MapVector>();
  writeBuffer(mapVector->offsets(), out);
  writeBuffer(mapVector->sizes(), out);

  // Keys and values vectors.
  saveVector(*mapVector->mapKeys(), out);
  saveVector(*mapVector->mapValues(), out);
}

VectorPtr readMapVector(
    const TypePtr& type,
    vector_size_t size,
    std::istream& in,
    memory::MemoryPool* pool) {
  // Nulls buffer.
  BufferPtr nulls = readOptionalBuffer(in, pool);

  BufferPtr offsets = readBuffer(in, pool);
  BufferPtr sizes = readBuffer(in, pool);

  auto keys = restoreVector(in, pool);
  auto values = restoreVector(in, pool);

  return std::make_shared<MapVector>(
      pool, type, nulls, size, offsets, sizes, keys, values);
}
} // namespace

void saveType(const TypePtr& type, std::ostream& out) {
  // Type kind.
  write(static_cast<int>(type->kind()), out);

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
    case TypeKind::DATE:
    case TypeKind::UNKNOWN:
      break;
    case TypeKind::ROW: {
      // Number of children.
      write<int32_t>(type->size(), out);

      // Child types.
      const auto& names = type->asRow().names();
      for (auto i = 0; i < type->size(); ++i) {
        write<int32_t>(names[i].size(), out);
        out.write(names[i].data(), names[i].size());
        saveType(type->childAt(i), out);
      }
      break;
    }
    case TypeKind::ARRAY:
      saveType(type->childAt(0), out);
      break;
    case TypeKind::MAP:
      saveType(type->childAt(0), out);
      saveType(type->childAt(1), out);
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", type->toString());
  }
}

TypePtr restoreType(std::istream& in) {
  // Type kind.
  auto typeKind = TypeKind(read<int32_t>(in));

  if (typeKind == TypeKind::UNKNOWN) {
    return UNKNOWN();
  }

  if (typeKind == TypeKind::ROW) {
    // Number of children.
    auto numChildren = read<int32_t>(in);

    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (auto i = 0; i < numChildren; ++i) {
      names.push_back(read<std::string>(in));
      types.push_back(restoreType(in));
    }

    return ROW(std::move(names), std::move(types));
  }

  if (typeKind == TypeKind::ARRAY) {
    return ARRAY(restoreType(in));
  }

  if (typeKind == TypeKind::MAP) {
    auto keyType = restoreType(in);
    auto valueType = restoreType(in);
    return MAP(keyType, valueType);
  }

  return createScalarType(typeKind);
}

void saveVector(const BaseVector& vector, std::ostream& out) {
  // Encoding.
  writeEncoding(vector.encoding(), out);

  // Type kind.
  saveType(vector.type(), out);

  // Vector size.
  write(vector.size(), out);

  switch (vector.encoding()) {
    case VectorEncoding::Simple::FLAT:
      writeFlatVector(vector, out);
      return;
    case VectorEncoding::Simple::CONSTANT:
      writeConstantVector(vector, out);
      return;
    case VectorEncoding::Simple::DICTIONARY:
      writeDictionaryVector(vector, out);
      return;
    case VectorEncoding::Simple::ROW:
      writeRowVector(vector, out);
      return;
    case VectorEncoding::Simple::ARRAY:
      writeArrayVector(vector, out);
      return;
    case VectorEncoding::Simple::MAP:
      writeMapVector(vector, out);
      return;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported encoding: {}", mapSimpleToName(vector.encoding()));
  }
}

VectorPtr restoreVector(std::istream& in, memory::MemoryPool* pool) {
  // Encoding.
  auto encoding = readEncoding(in);

  // Type kind.
  auto type = restoreType(in);

  // Vector size.
  auto size = read<int32_t>(in);

  switch (encoding) {
    case Encoding::kFlat:
      if (type->isRow()) {
        return readRowVector(type, size, in, pool);
      } else if (type->isArray()) {
        return readArrayVector(type, size, in, pool);
      } else if (type->isMap()) {
        return readMapVector(type, size, in, pool);
      }
      return readFlatVector(type, size, in, pool);
    case Encoding::kConstant:
      return readConstantVector(type, size, in, pool);
    case Encoding::kDictionary:
      return readDictionaryVector(type, size, in, pool);
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace facebook::velox
