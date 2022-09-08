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

namespace facebook {
namespace velox {

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
      typeKind_(type_->kind()),
      encoding_(encoding),
      nulls_(std::move(nulls)),
      rawNulls_(nulls_.get() ? nulls_->as<uint64_t>() : nullptr),
      pool_(pool),
      length_(length),
      nullCount_(nullCount),
      distinctValueCount_(distinctValueCount),
      representedByteCount_(representedByteCount),
      storageByteCount_(storageByteCount) {
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

void BaseVector::ensureNullsCapacity(vector_size_t size, bool setNotNull) {
  auto fill = setNotNull ? bits::kNotNull : bits::kNull;
  if (nulls_ && nulls_->isMutable()) {
    if (nulls_->capacity() >= bits::nbytes(size)) {
      return;
    }
    AlignedBuffer::reallocate<bool>(&nulls_, size, fill);
  } else {
    auto newNulls = AlignedBuffer::allocate<bool>(size, pool_, fill);
    if (nulls_) {
      memcpy(
          newNulls->asMutable<char>(),
          nulls_->as<char>(),
          byteSize<bool>(std::min(length_, size)));
    }
    nulls_ = std::move(newNulls);
  }
  rawNulls_ = nulls_->as<uint64_t>();
}

template <>
uint64_t BaseVector::byteSize<bool>(vector_size_t count) {
  return bits::nbytes(count);
}

void BaseVector::resize(vector_size_t size, bool setNotNull) {
  if (nulls_) {
    auto bytes = byteSize<bool>(size);
    if (length_ < size) {
      if (nulls_->size() < bytes) {
        AlignedBuffer::reallocate<char>(&nulls_, bytes);
        rawNulls_ = nulls_->as<uint64_t>();
      }
      if (setNotNull && size > length_) {
        bits::fillBits(
            const_cast<uint64_t*>(rawNulls_), length_, size, bits::kNotNull);
      }
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
    return BaseVector::wrapInConstant(size, 0, vector);
  }

  auto kind = vector->typeKind();
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      addDictionary, kind, nulls, indices, size, std::move(vector));
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
      addSequence, kind, lengths, size, std::move(vector));
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
          pool,
          type,
          BufferPtr(nullptr),
          size,
          std::move(children),
          0 /*nullCount*/);
    }
    case TypeKind::ARRAY: {
      BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
      BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
      // When constructing FixedSizeArray types we validate the
      // provided lengths are of the expected size. But
      // BaseVector::create constructs the array with the
      // sizes/offsets all set to zero -- presumably because the
      // caller will fill them in later by directly manipulating
      // rawSizes/rawOffsets. This makes the sanity check in the
      // FixedSizeArray constructor less powerful than it would be if
      // we knew the sizes / offsets were going to populate them with
      // in advance and they were immutable after constructing the
      // array.
      //
      // For now to support the current code structure of "create then
      // populate" for BaseVector::create(), in the case of
      // FixedSizeArrays we pre-initialize the sizes / offsets here
      // with what we expect them to be so the constructor validation
      // passes. The code that subsequently manipulates the
      // sizes/offsets directly should also validate they are
      // continuing to upload the fixedSize constraint.
      if (type->isFixedWidth()) {
        auto rawOffsets = offsets->asMutable<vector_size_t>();
        auto rawSizes = sizes->asMutable<vector_size_t>();
        const auto width = type->fixedElementsWidth();
        for (vector_size_t i = 0; i < size; ++i) {
          *rawSizes++ = width;
          *rawOffsets++ = width * i;
        }
      }
      auto elementType = type->as<TypeKind::ARRAY>().elementType();
      auto elements = create(elementType, 0, pool);
      return std::make_shared<ArrayVector>(
          pool,
          type,
          BufferPtr(nullptr),
          size,
          std::move(offsets),
          std::move(sizes),
          std::move(elements),
          0 /*nullCount*/);
    }
    case TypeKind::MAP: {
      BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
      BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
      auto keyType = type->as<TypeKind::MAP>().keyType();
      auto valueType = type->as<TypeKind::MAP>().valueType();
      auto keys = create(keyType, 0, pool);
      auto values = create(valueType, 0, pool);
      return std::make_shared<MapVector>(
          pool,
          type,
          BufferPtr(nullptr),
          size,
          std::move(offsets),
          std::move(sizes),
          std::move(keys),
          std::move(values),
          0 /*nullCount*/);
    }
    case TypeKind::UNKNOWN: {
      BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool, bits::kNull);
      return std::make_shared<FlatVector<UnknownValue>>(
          pool,
          nulls,
          size,
          BufferPtr(nullptr),
          std::vector<BufferPtr>(),
          SimpleVectorStats<UnknownValue>{},
          1 /*distinctValueCount*/,
          size /*nullCount*/,
          true /*isSorted*/,
          0 /*representedBytes*/);
    }
    case TypeKind::SHORT_DECIMAL: {
      return createEmpty<TypeKind::SHORT_DECIMAL>(size, pool, type);
    }
    case TypeKind::LONG_DECIMAL: {
      return createEmpty<TypeKind::LONG_DECIMAL>(size, pool, type);
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
    if (indices->get()->size() < size * sizeof(vector_size_t)) {
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
    const std::string& delimiter,
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
  auto newSize = std::max<vector_size_t>(rows.size(), length_);
  if (nulls_ && !(nulls_->unique() && nulls_->isMutable())) {
    BufferPtr newNulls = AlignedBuffer::allocate<bool>(newSize, pool_);
    auto rawNewNulls = newNulls->asMutable<uint64_t>();
    memcpy(rawNewNulls, rawNulls_, bits::nbytes(length_));

    nulls_ = std::move(newNulls);
    rawNulls_ = nulls_->as<uint64_t>();
  }

  this->resize(newSize);
}

void BaseVector::ensureWritable(
    const SelectivityVector& rows,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    VectorPool* vectorPool) {
  if (!result) {
    if (vectorPool) {
      result = vectorPool->get(type, rows.size());
    } else {
      result = BaseVector::create(type, rows.size(), pool);
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
  auto targetSize = std::max<vector_size_t>(rows.size(), result->size());

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
    variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  T copy = T();
  TypePtr type;
  if constexpr (std::is_same_v<T, StringView>) {
    type = Type::create<kind>();
    if (!value.isNull()) {
      copy = StringView(value.value<kind>());
    }
  } else if constexpr (
      std::is_same_v<T, UnscaledShortDecimal> ||
      std::is_same_v<T, UnscaledLongDecimal>) {
    const auto& decimal = value.value<kind>();
    type = DECIMAL(decimal.precision, decimal.scale);
    if (!value.isNull()) {
      copy = decimal.value();
    }
  } else {
    type = Type::create<kind>();
    if (!value.isNull()) {
      copy = value.value<T>();
    }
  }

  return std::make_shared<ConstantVector<T>>(
      pool,
      size,
      value.isNull(),
      type,
      std::move(copy),
      SimpleVectorStats<T>{},
      sizeof(T) /*representedByteCount*/);
}

template <>
VectorPtr newConstant<TypeKind::OPAQUE>(
    variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK(!value.isNull(), "Can't infer type from null opaque object");
  const auto& capsule = value.value<TypeKind::OPAQUE>();

  return std::make_shared<ConstantVector<std::shared_ptr<void>>>(
      pool,
      size,
      value.isNull(),
      capsule.type,
      std::shared_ptr<void>(capsule.obj),
      SimpleVectorStats<std::shared_ptr<void>>{},
      sizeof(std::shared_ptr<void>) /*representedByteCount*/);
}

// static
VectorPtr BaseVector::createConstant(
    variant value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      newConstant, value.kind(), value, size, pool);
}

std::shared_ptr<BaseVector> BaseVector::createNullConstant(
    const TypePtr& type,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  if (!type->isPrimitiveType()) {
    return std::make_shared<ConstantVector<ComplexType>>(
        pool, size, true, type, ComplexType());
  }

  if (type->kind() == TypeKind::SHORT_DECIMAL) {
    return std::make_shared<ConstantVector<UnscaledShortDecimal>>(
        pool, size, true, type, UnscaledShortDecimal());
  }

  if (type->kind() == TypeKind::LONG_DECIMAL) {
    return std::make_shared<ConstantVector<UnscaledLongDecimal>>(
        pool, size, true, type, UnscaledLongDecimal());
  }

  return BaseVector::createConstant(variant(type->kind()), size, pool);
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
      source);
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
void BaseVector::prepareForReuse(
    std::shared_ptr<BaseVector>& vector,
    vector_size_t size) {
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

} // namespace velox
} // namespace facebook
