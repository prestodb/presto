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

#include "velox/vector/BaseVector.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SequenceVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/VectorPool.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

BaseVector::BaseVector(
    velox::memory::MemoryPool* pool,
    std::shared_ptr<const Type> type,
    VectorEncoding::Simple encoding,
    BufferPtr nulls,
    size_t length,
    std::optional<vector_size_t> distinctValueCount,
    std::optional<vector_size_t> nullCount,
    std::optional<ByteCount> representedByteCount,
    std::optional<ByteCount> storageByteCount)
    : type_(std::move(type)),
      typeKind_(type_ ? type_->kind() : TypeKind::INVALID),
      encoding_(encoding),
      nulls_(std::move(nulls)),
      rawNulls_(nulls_.get() ? nulls_->as<uint64_t>() : nullptr),
      pool_(pool),
      length_(length),
      nullCount_(nullCount),
      distinctValueCount_(distinctValueCount),
      representedByteCount_(representedByteCount),
      storageByteCount_(storageByteCount) {
  VELOX_CHECK_NOT_NULL(type_, "Vector creation requires a non-null type.");

  if (nulls_) {
    int32_t bytes = byteSize<bool>(length_);
    VELOX_CHECK(nulls_->capacity() >= bytes);
    if (nulls_->size() < bytes) {
      // Set the size so that values get preserved by resize. Do not
      // set if already large enough, so that it is safe to take a
      // second reference to an immutable 'nulls_'.
      nulls_->setSize(bytes);
    }
    inMemoryBytes_ += nulls_->size();
  }
}

void BaseVector::ensureNullsCapacity(
    vector_size_t minimumSize,
    bool setNotNull) {
  auto fill = setNotNull ? bits::kNotNull : bits::kNull;
  // Ensure the size of nulls_ is always at least as large as length_.
  auto size = std::max(minimumSize, length_);
  if (nulls_ && nulls_->isMutable()) {
    if (nulls_->capacity() < bits::nbytes(size)) {
      AlignedBuffer::reallocate<bool>(&nulls_, size, fill);
    }
    // ensure that the newly added positions have the right initial value for
    // the case where changes in size don't result in change in the size of
    // the underlying buffer.
    // TODO: move this inside reallocate.
    rawNulls_ = nulls_->as<uint64_t>();
    if (setNotNull && length_ < size) {
      bits::fillBits(
          const_cast<uint64_t*>(rawNulls_), length_, size, bits::kNotNull);
    }
  } else {
    auto newNulls = AlignedBuffer::allocate<bool>(size, pool_, fill);
    if (nulls_) {
      memcpy(
          newNulls->asMutable<char>(),
          nulls_->as<char>(),
          byteSize<bool>(std::min(length_, size)));
    }
    nulls_ = std::move(newNulls);
    rawNulls_ = nulls_->as<uint64_t>();
  }
}

template <>
uint64_t BaseVector::byteSize<bool>(vector_size_t count) {
  return bits::nbytes(count);
}

void BaseVector::resize(vector_size_t size, bool setNotNull) {
  if (nulls_) {
    auto bytes = byteSize<bool>(size);
    if (length_ < size || !nulls_->isMutable()) {
      ensureNullsCapacity(size, setNotNull);
    }
    nulls_->setSize(bytes);
  }
  length_ = size;
}

template <TypeKind kind>
static VectorPtr addDictionary(
    BufferPtr nulls,
    BufferPtr indices,
    size_t size,
    VectorPtr vector) {
  auto pool = vector->pool();
  return std::make_shared<
      DictionaryVector<typename KindToFlatVector<kind>::WrapperType>>(
      pool, nulls, size, std::move(vector), std::move(indices));
}

// static
VectorPtr BaseVector::wrapInDictionary(
    BufferPtr nulls,
    BufferPtr indices,
    vector_size_t size,
    VectorPtr vector) {
  // Dictionary that doesn't add nulls over constant is same as constant. Just
  // make sure to adjust the size.
  if (vector->encoding() == VectorEncoding::Simple::CONSTANT && !nulls) {
    if (size == vector->size()) {
      return vector;
    }
    return BaseVector::wrapInConstant(size, 0, std::move(vector));
  }

  auto kind = vector->typeKind();
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      addDictionary,
      kind,
      std::move(nulls),
      std::move(indices),
      size,
      std::move(vector));
}

template <TypeKind kind>
static VectorPtr
addSequence(BufferPtr lengths, vector_size_t size, VectorPtr vector) {
  auto base = vector.get();
  auto pool = base->pool();
  auto lsize = lengths->size();
  return std::make_shared<
      SequenceVector<typename KindToFlatVector<kind>::WrapperType>>(
      pool,
      size,
      std::move(vector),
      std::move(lengths),
      SimpleVectorStats<typename KindToFlatVector<kind>::WrapperType>{},
      std::nullopt /*distinctCount*/,
      std::nullopt,
      false /*sorted*/,
      base->representedBytes().has_value()
          ? std::optional<ByteCount>(
                base->representedBytes().value() * size /
                (1 + (lsize / sizeof(vector_size_t))))
          : std::nullopt);
}

// static
VectorPtr BaseVector::wrapInSequence(
    BufferPtr lengths,
    vector_size_t size,
    VectorPtr vector) {
  auto kind = vector->typeKind();
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      addSequence, kind, std::move(lengths), size, std::move(vector));
}

template <TypeKind kind>
static VectorPtr
addConstant(vector_size_t size, vector_size_t index, VectorPtr vector) {
  using T = typename KindToFlatVector<kind>::WrapperType;

  auto pool = vector->pool();

  if (vector->isNullAt(index)) {
    if constexpr (std::is_same_v<T, ComplexType>) {
      auto singleNull = BaseVector::create(vector->type(), 1, pool);
      singleNull->setNull(0, true);
      return std::make_shared<ConstantVector<T>>(
          pool, size, 0, singleNull, SimpleVectorStats<T>{});
    } else {
      return std::make_shared<ConstantVector<T>>(
          pool, size, true, vector->type(), T());
    }
  }

  for (;;) {
    if (vector->isConstantEncoding()) {
      auto constVector = vector->as<ConstantVector<T>>();
      if constexpr (!std::is_same_v<T, ComplexType>) {
        if (!vector->valueVector()) {
          T value = constVector->valueAt(0);
          return std::make_shared<ConstantVector<T>>(
              pool, size, false, vector->type(), std::move(value));
        }
      }

      index = constVector->index();
      vector = vector->valueVector();
    } else if (vector->encoding() == VectorEncoding::Simple::DICTIONARY) {
      BufferPtr indices = vector->as<DictionaryVector<T>>()->indices();
      index = indices->as<vector_size_t>()[index];
      vector = vector->valueVector();
    } else {
      break;
    }
  }

  return std::make_shared<ConstantVector<T>>(
      pool, size, index, std::move(vector), SimpleVectorStats<T>{});
}

// static
VectorPtr BaseVector::wrapInConstant(
    vector_size_t length,
    vector_size_t index,
    VectorPtr vector) {
  auto kind = vector->typeKind();
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      addConstant, kind, length, index, std::move(vector));
}

template <TypeKind kind>
static VectorPtr createEmpty(
    vector_size_t size,
    velox::memory::MemoryPool* pool,
    const TypePtr& type) {
  using T = typename TypeTraits<kind>::NativeType;

  BufferPtr values;
  if constexpr (std::is_same_v<T, StringView>) {
    // Make sure to initialize StringView values so they can be safely accessed.
    values = AlignedBuffer::allocate<T>(size, pool, T());
  } else {
    values = AlignedBuffer::allocate<T>(size, pool);
  }

  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      BufferPtr(nullptr),
      size,
      std::move(values),
      std::vector<BufferPtr>());
}

// static
VectorPtr BaseVector::createInternal(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(type, "Vector creation requires a non-null type.");
  auto kind = type->kind();
  switch (kind) {
    case TypeKind::ROW: {
      std::vector<VectorPtr> children;
      auto rowType = type->as<TypeKind::ROW>();
      // Children are reserved the parent size but are set to 0 elements.
      for (int32_t i = 0; i < rowType.size(); ++i) {
        children.push_back(create(rowType.childAt(i), size, pool));
        children.back()->resize(0);
      }
      return std::make_shared<RowVector>(
          pool, type, nullptr, size, std::move(children));
    }
    case TypeKind::ARRAY: {
      BufferPtr sizes = allocateSizes(size, pool);
      BufferPtr offsets = allocateOffsets(size, pool);
      auto elementType = type->as<TypeKind::ARRAY>().elementType();
      auto elements = create(elementType, 0, pool);
      return std::make_shared<ArrayVector>(
          pool,
          type,
          nullptr,
          size,
          std::move(offsets),
          std::move(sizes),
          std::move(elements));
    }
    case TypeKind::MAP: {
      BufferPtr sizes = allocateSizes(size, pool);
      BufferPtr offsets = allocateOffsets(size, pool);
      auto keyType = type->as<TypeKind::MAP>().keyType();
      auto valueType = type->as<TypeKind::MAP>().valueType();
      auto keys = create(keyType, 0, pool);
      auto values = create(valueType, 0, pool);
      return std::make_shared<MapVector>(
          pool,
          type,
          nullptr,
          size,
          std::move(offsets),
          std::move(sizes),
          std::move(keys),
          std::move(values));
    }
    case TypeKind::UNKNOWN: {
      BufferPtr nulls = allocateNulls(size, pool, bits::kNull);
      return std::make_shared<FlatVector<UnknownValue>>(
          pool, UNKNOWN(), nulls, size, nullptr, std::vector<BufferPtr>());
    }
    default:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          createEmpty, kind, size, pool, type);
  }
}

void BaseVector::addNulls(const uint64_t* bits, const SelectivityVector& rows) {
  VELOX_CHECK(isNullsWritable());
  VELOX_CHECK(length_ >= rows.end());
  ensureNulls();
  auto target = nulls_->asMutable<uint64_t>();
  const uint64_t* selected = rows.asRange().bits();
  if (!bits) {
    // A 1 in rows makes a 0 in nulls.
    bits::andWithNegatedBits(target, selected, rows.begin(), rows.end());
    return;
  }
  // A 0 in bits with a 1 in rows makes a 0 in nulls.
  bits::forEachWord(
      rows.begin(),
      rows.end(),
      [target, bits, selected](int32_t idx, uint64_t mask) {
        target[idx] &= ~mask | (bits[idx] | ~selected[idx]);
      },
      [target, bits, selected](int32_t idx) {
        target[idx] &= bits[idx] | ~selected[idx];
      });
}

void BaseVector::clearNulls(const SelectivityVector& rows) {
  VELOX_CHECK(isNullsWritable());
  if (!nulls_) {
    return;
  }

  if (rows.isAllSelected() && rows.end() == length_) {
    nulls_ = nullptr;
    rawNulls_ = nullptr;
    nullCount_ = 0;
    return;
  }

  auto rawNulls = nulls_->asMutable<uint64_t>();
  bits::orBits(
      rawNulls,
      rows.asRange().bits(),
      std::min(length_, rows.begin()),
      std::min(length_, rows.end()));
  nullCount_ = std::nullopt;
}

void BaseVector::clearNulls(vector_size_t begin, vector_size_t end) {
  VELOX_CHECK(isNullsWritable());
  if (!nulls_) {
    return;
  }

  if (begin == 0 && end == length_) {
    nulls_ = nullptr;
    rawNulls_ = nullptr;
    nullCount_ = 0;
    return;
  }

  auto rawNulls = nulls_->asMutable<uint64_t>();
  bits::fillBits(rawNulls, begin, end, true);
  nullCount_ = std::nullopt;
}

void BaseVector::setNulls(const BufferPtr& nulls) {
  if (nulls) {
    VELOX_DCHECK_GE(nulls->size(), bits::nbytes(length_));
    nulls_ = nulls;
    rawNulls_ = nulls->as<uint64_t>();
    nullCount_ = std::nullopt;
  } else {
    nulls_ = nullptr;
    rawNulls_ = nullptr;
    nullCount_ = 0;
  }
}

// static
void BaseVector::resizeIndices(
    vector_size_t size,
    vector_size_t initialValue,
    velox::memory::MemoryPool* pool,
    BufferPtr* indices,
    const vector_size_t** raw) {
  if (indices->get() && indices->get()->isMutable()) {
    auto newByteSize = byteSize<vector_size_t>(size);
    if (indices->get()->size() < newByteSize) {
      AlignedBuffer::reallocate<vector_size_t>(indices, size, initialValue);
    }
  } else {
    auto newIndices =
        AlignedBuffer::allocate<vector_size_t>(size, pool, initialValue);
    if (indices->get()) {
      auto dst = newIndices->asMutable<vector_size_t>();
      auto src = indices->get()->as<vector_size_t>();
      auto len = std::min(indices->get()->size(), size * sizeof(vector_size_t));
      memcpy(dst, src, len);
    }
    *indices = newIndices;
  }
  *raw = indices->get()->asMutable<vector_size_t>();
}

std::string BaseVector::toSummaryString() const {
  std::stringstream out;
  out << "[" << encoding() << " " << type_->toString() << ": " << length_
      << " elements, ";
  if (!nulls_) {
    out << "no nulls";
  } else {
    out << countNulls(nulls_, 0, length_) << " nulls";
  }
  out << "]";
  return out.str();
}

std::string BaseVector::toString(bool recursive) const {
  std::stringstream out;
  out << toSummaryString();

  if (recursive) {
    switch (encoding()) {
      case VectorEncoding::Simple::DICTIONARY:
      case VectorEncoding::Simple::SEQUENCE:
      case VectorEncoding::Simple::CONSTANT:
        if (valueVector() != nullptr) {
          out << ", " << valueVector()->toString(true);
        }
        break;
      default:
        break;
    }
  }

  return out.str();
}

std::string BaseVector::toString(vector_size_t index) const {
  VELOX_CHECK_LT(index, length_, "Vector index should be less than length.");
  std::stringstream out;
  if (!nulls_) {
    out << "no nulls";
  } else if (isNullAt(index)) {
    out << "null";
  } else {
    out << "not null";
  }
  return out.str();
}

std::string BaseVector::toString(
    vector_size_t from,
    vector_size_t to,
    const char* delimiter,
    bool includeRowNumbers) const {
  const auto start = std::max(0, std::min<int32_t>(from, length_));
  const auto end = std::max(0, std::min<int32_t>(to, length_));

  std::stringstream out;
  for (auto i = start; i < end; ++i) {
    if (i > start) {
      out << delimiter;
    }
    if (includeRowNumbers) {
      out << i << ": ";
    }
    out << toString(i);
  }
  return out.str();
}

void BaseVector::ensureWritable(const SelectivityVector& rows) {
  auto newSize = std::max<vector_size_t>(rows.end(), length_);
  if (nulls_ && !(nulls_->unique() && nulls_->isMutable())) {
    BufferPtr newNulls = AlignedBuffer::allocate<bool>(newSize, pool_);
    auto rawNewNulls = newNulls->asMutable<uint64_t>();
    memcpy(rawNewNulls, rawNulls_, bits::nbytes(length_));

    nulls_ = std::move(newNulls);
    rawNulls_ = nulls_->as<uint64_t>();
  }

  this->resize(newSize);
  this->resetDataDependentFlags(&rows);
}

void BaseVector::ensureWritable(
    const SelectivityVector& rows,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    VectorPool* vectorPool) {
  if (!result) {
    if (vectorPool) {
      result = vectorPool->get(type, rows.end());
    } else {
      result = BaseVector::create(type, rows.end(), pool);
    }
    return;
  }
  auto resultType = result->type();
  bool isUnknownType = resultType->containsUnknown();
  if (result->encoding() == VectorEncoding::Simple::LAZY) {
    // TODO Figure out how to allow memory reuse for a newly loaded vector.
    // LazyVector holds a reference to loaded vector, hence, unique() check
    // below will never pass.
    VELOX_NYI();
  }
  if (result.unique() && !isUnknownType) {
    switch (result->encoding()) {
      case VectorEncoding::Simple::FLAT:
      case VectorEncoding::Simple::ROW:
      case VectorEncoding::Simple::ARRAY:
      case VectorEncoding::Simple::MAP:
      case VectorEncoding::Simple::FUNCTION: {
        result->ensureWritable(rows);
        return;
      }
      default:
        break; /** NOOP **/
    }
  }

  // The copy-on-write size is the max of the writable row set and the
  // vector.
  auto targetSize = std::max<vector_size_t>(rows.end(), result->size());

  VectorPtr copy;
  if (vectorPool) {
    copy = vectorPool->get(isUnknownType ? type : resultType, targetSize);
  } else {
    copy =
        BaseVector::create(isUnknownType ? type : resultType, targetSize, pool);
  }
  SelectivityVector copyRows(
      std::min<vector_size_t>(targetSize, result->size()));
  copyRows.deselect(rows);
  if (copyRows.hasSelections()) {
    copy->copy(result.get(), copyRows, nullptr);
  }
  result = std::move(copy);
}

template <TypeKind kind>
VectorPtr newConstant(
    const TypePtr& type,
    variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  using T = typename KindToFlatVector<kind>::WrapperType;

  if (value.isNull()) {
    return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
  }

  T copy;
  if constexpr (std::is_same_v<T, StringView>) {
    copy = StringView(value.value<kind>());
  } else {
    copy = value.value<T>();
  }

  return std::make_shared<ConstantVector<T>>(
      pool, size, false, type, std::move(copy));
}

template <>
VectorPtr newConstant<TypeKind::OPAQUE>(
    const TypePtr& type,
    variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  const auto& capsule = value.value<TypeKind::OPAQUE>();

  return std::make_shared<ConstantVector<std::shared_ptr<void>>>(
      pool, size, value.isNull(), type, std::shared_ptr<void>(capsule.obj));
}

// static
VectorPtr BaseVector::createConstant(
    const TypePtr& type,
    variant value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_EQ(type->kind(), value.kind());
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      newConstant, value.kind(), type, value, size, pool);
}

namespace {

template <TypeKind kind>
VectorPtr newNullConstant(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  return std::make_shared<ConstantVector<T>>(pool, size, true, type, T());
}
} // namespace

VectorPtr BaseVector::createNullConstant(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(type, "Vector creation requires a non-null type.");
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      newNullConstant, type->kind(), type, size, pool);
}

// static
VectorPtr BaseVector::loadedVectorShared(VectorPtr vector) {
  if (vector->encoding() != VectorEncoding::Simple::LAZY) {
    // If 'vector' is a wrapper, we load any wrapped LazyVector.
    vector->loadedVector();
    return vector;
  }
  return vector->asUnchecked<LazyVector>()->loadedVectorShared();
}

// static
VectorPtr BaseVector::transpose(BufferPtr indices, VectorPtr&& source) {
  // TODO: Reuse the indices if 'source' is already a dictionary and
  // there are no other users of its indices.
  return wrapInDictionary(
      BufferPtr(nullptr),
      indices,
      indices->size() / sizeof(vector_size_t),
      std::move(source));
}

bool isLazyNotLoaded(const BaseVector& vector) {
  switch (vector.encoding()) {
    case VectorEncoding::Simple::LAZY:
      return !vector.as<LazyVector>()->isLoaded();
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      return isLazyNotLoaded(*vector.valueVector());
    case VectorEncoding::Simple::CONSTANT:
      return vector.valueVector() ? isLazyNotLoaded(*vector.valueVector())
                                  : false;
    default:
      return false;
  }
}

uint64_t BaseVector::estimateFlatSize() const {
  if (length_ == 0) {
    return 0;
  }

  if (isLazyNotLoaded(*this)) {
    return 0;
  }

  auto leaf = wrappedVector();
  // If underlying vector is empty we should return the leaf's single element
  // size times this vector's size plus any nulls of this vector.
  if (UNLIKELY(leaf->size() == 0)) {
    const auto& leafType = leaf->type();
    return length_ *
        (leafType->isFixedWidth() ? leafType->cppSizeInBytes() : 0) +
        BaseVector::retainedSize();
  }

  auto avgRowSize = 1.0 * leaf->retainedSize() / leaf->size();
  return length_ * avgRowSize;
}

namespace {
bool isReusableEncoding(VectorEncoding::Simple encoding) {
  return encoding == VectorEncoding::Simple::FLAT ||
      encoding == VectorEncoding::Simple::ARRAY ||
      encoding == VectorEncoding::Simple::MAP ||
      encoding == VectorEncoding::Simple::ROW;
}
} // namespace

// static
void BaseVector::flattenVector(VectorPtr& vector) {
  if (!vector) {
    return;
  }
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      return;
    case VectorEncoding::Simple::ROW: {
      auto* rowVector = vector->asUnchecked<RowVector>();
      for (auto& child : rowVector->children()) {
        BaseVector::flattenVector(child);
      }
      return;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* arrayVector = vector->asUnchecked<ArrayVector>();
      BaseVector::flattenVector(arrayVector->elements());
      return;
    }
    case VectorEncoding::Simple::MAP: {
      auto* mapVector = vector->asUnchecked<MapVector>();
      BaseVector::flattenVector(mapVector->mapKeys());
      BaseVector::flattenVector(mapVector->mapValues());
      return;
    }
    default:
      BaseVector::ensureWritable(
          SelectivityVector::empty(), vector->type(), vector->pool(), vector);
  }
}

void BaseVector::prepareForReuse(VectorPtr& vector, vector_size_t size) {
  if (!vector.unique() || !isReusableEncoding(vector->encoding())) {
    vector = BaseVector::create(vector->type(), size, vector->pool());
    return;
  }

  vector->prepareForReuse();
  vector->resize(size);
}

void BaseVector::prepareForReuse() {
  // Check nulls buffer. Keep the buffer if singly-referenced and mutable and
  // there is at least one null bit set. Reset otherwise.
  if (nulls_) {
    if (nulls_->unique() && nulls_->isMutable()) {
      if (0 == BaseVector::countNulls(nulls_, length_)) {
        nulls_ = nullptr;
        rawNulls_ = nullptr;
      }
    } else {
      nulls_ = nullptr;
      rawNulls_ = nullptr;
    }
  }
  this->resetDataDependentFlags(nullptr);
}

namespace {

size_t typeSize(const Type& type) {
  switch (type.kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(StringView);
    case TypeKind::OPAQUE:
      return sizeof(std::shared_ptr<void>);
    default:
      VELOX_DCHECK(type.isPrimitiveType(), type.toString());
      return type.cppSizeInBytes();
  }
}

struct BufferReleaser {
  explicit BufferReleaser(const BufferPtr& parent) : parent_(parent) {}
  void addRef() const {}
  void release() const {}

 private:
  BufferPtr parent_;
};

BufferPtr sliceBufferZeroCopy(
    size_t typeSize,
    bool podType,
    const BufferPtr& buf,
    vector_size_t offset,
    vector_size_t length) {
  // Cannot use `Buffer::as<uint8_t>()` here because Buffer::podType_ is false
  // when type is OPAQUE.
  auto data =
      reinterpret_cast<const uint8_t*>(buf->as<void>()) + offset * typeSize;
  return BufferView<BufferReleaser>::create(
      data, length * typeSize, BufferReleaser(buf), podType);
}

} // namespace

// static
BufferPtr BaseVector::sliceBuffer(
    const Type& type,
    const BufferPtr& buf,
    vector_size_t offset,
    vector_size_t length,
    memory::MemoryPool* pool) {
  if (!buf) {
    return nullptr;
  }
  if (type.kind() != TypeKind::BOOLEAN) {
    return sliceBufferZeroCopy(
        typeSize(type), type.isPrimitiveType(), buf, offset, length);
  }
  if (offset % 8 == 0) {
    return sliceBufferZeroCopy(1, true, buf, offset / 8, (length + 7) / 8);
  }
  VELOX_DCHECK_NOT_NULL(pool);
  auto ans = AlignedBuffer::allocate<bool>(length, pool);
  bits::copyBits(
      buf->as<uint64_t>(), offset, ans->asMutable<uint64_t>(), 0, length);
  return ans;
}

std::optional<vector_size_t> BaseVector::findDuplicateValue(
    vector_size_t start,
    vector_size_t size,
    CompareFlags flags) {
  if (length_ == 0 || size == 0) {
    return std::nullopt;
  }

  VELOX_DCHECK_GE(start, 0, "Start index must not be negative");
  VELOX_DCHECK_LT(start, length_, "Start index is too large");
  VELOX_DCHECK_GT(size, 0, "Size must not be negative");
  VELOX_DCHECK_LE(start + size, length_, "Size is too large");

  std::vector<vector_size_t> indices(size);
  std::iota(indices.begin(), indices.end(), start);
  sortIndices(indices, flags);

  for (auto i = 1; i < size; ++i) {
    if (equalValueAt(this, indices[i], indices[i - 1])) {
      return indices[i];
    }
  }

  return std::nullopt;
}

std::string printNulls(const BufferPtr& nulls, vector_size_t maxBitsToPrint) {
  VELOX_CHECK_GE(maxBitsToPrint, 0);

  vector_size_t totalCount = nulls->size() * 8;
  auto* rawNulls = nulls->as<uint64_t>();
  auto nullCount = bits::countNulls(rawNulls, 0, totalCount);

  std::stringstream out;
  out << nullCount << " out of " << totalCount << " rows are null";

  if (nullCount) {
    out << ": ";
    for (auto i = 0; i < maxBitsToPrint && i < totalCount; ++i) {
      out << (bits::isBitNull(rawNulls, i) ? "n" : ".");
    }
  }

  return out.str();
}

std::string printIndices(
    const BufferPtr& indices,
    vector_size_t maxIndicesToPrint) {
  VELOX_CHECK_GE(maxIndicesToPrint, 0);

  auto* rawIndices = indices->as<vector_size_t>();

  vector_size_t size = indices->size() / sizeof(vector_size_t);

  std::unordered_set<vector_size_t> uniqueIndices;
  for (auto i = 0; i < size; ++i) {
    uniqueIndices.insert(rawIndices[i]);
  }

  std::stringstream out;
  out << uniqueIndices.size() << " unique indices out of " << size << ": ";
  for (auto i = 0; i < maxIndicesToPrint && i < size; ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << rawIndices[i];
  }

  return out.str();
}

} // namespace facebook::velox
