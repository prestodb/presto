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

void BaseVector::allocateNulls() {
  VELOX_CHECK(!nulls_);
  int32_t bytes = byteSize<bool>(length_);
  nulls_ = AlignedBuffer::allocate<char>(bytes, pool());
  nulls_->setSize(bytes);
  memset(nulls_->asMutable<uint8_t>(), bits::kNotNullByte, bytes);
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
  auto base = vector.get();
  auto pool = base->pool();
  auto indicesBuffer = indices.get();
  auto vsize = vector->size();
  return std::make_shared<
      DictionaryVector<typename KindToFlatVector<kind>::WrapperType>>(
      pool,
      nulls,
      size,
      std::move(vector),
      TypeKind::INTEGER,
      std::move(indices),
      SimpleVectorStats<typename KindToFlatVector<kind>::WrapperType>{},
      indicesBuffer->size() / sizeof(vector_size_t) /*distinctValueCount*/,
      base->getNullCount(),
      false /*isSorted*/,
      base->representedBytes().has_value()
          ? std::optional<ByteCount>(
                base->representedBytes().value() * size / (1 + vsize))
          : std::nullopt);
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
  BufferPtr values = AlignedBuffer::allocate<T>(size, pool);
  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      BufferPtr(nullptr),
      size,
      std::move(values),
      std::vector<BufferPtr>(),
      SimpleVectorStats<T>{},
      0 /*distinctValueCount*/,
      0 /*nullCount*/,
      false /*isSorted*/,
      0 /*representedBytes*/);
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
      BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool, true);
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

// static
void BaseVector::resizeIndices(
    vector_size_t size,
    vector_size_t initialValue,
    velox::memory::MemoryPool* pool,
    BufferPtr* indices,
    const vector_size_t** raw) {
  if (!indices->get()) {
    *indices = AlignedBuffer::allocate<vector_size_t>(size, pool, initialValue);
  } else if ((*indices)->size() < size * sizeof(vector_size_t)) {
    AlignedBuffer::reallocate<vector_size_t>(indices, size, initialValue);
  }
  *raw = indices->get()->asMutable<vector_size_t>();
}

std::string BaseVector::toString() const {
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
  if (nulls_ && !nulls_->unique()) {
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
    VectorPtr* result) {
  if (!*result) {
    *result = BaseVector::create(type, rows.size(), pool);
    return;
  }
  auto resultType = (*result)->type();
  bool isUnknownType = resultType->containsUnknown();
  if ((*result)->encoding() == VectorEncoding::Simple::LAZY) {
    // TODO Figure out how to allow memory reuse for a newly loaded vector.
    // LazyVector holds a reference to loaded vector, hence, unique() check
    // below will never pass.
    VELOX_NYI();
  }
  if (result->unique() && !isUnknownType) {
    switch ((*result)->encoding()) {
      case VectorEncoding::Simple::FLAT:
      case VectorEncoding::Simple::ROW:
      case VectorEncoding::Simple::ARRAY:
      case VectorEncoding::Simple::MAP:
      case VectorEncoding::Simple::FUNCTION: {
        (*result)->ensureWritable(rows);
        return;
      }
      default:
        break; /** NOOP **/
    }
  }

  // The copy-on-write size is the max of the writable row set and the
  // vector.
  auto targetSize = std::max<vector_size_t>(rows.size(), (*result)->size());

  auto copy =
      BaseVector::create(isUnknownType ? type : resultType, targetSize, pool);
  SelectivityVector copyRows(
      std::min<vector_size_t>(targetSize, (*result)->size()));
  copyRows.deselect(rows);
  if (copyRows.hasSelections()) {
    copy->copy(result->get(), copyRows, nullptr);
  }
  *result = std::move(copy);
}

template <TypeKind kind>
VectorPtr newConstant(
    variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  T copy = T();
  if (!value.isNull()) {
    if constexpr (std::is_same_v<T, StringView>) {
      copy = StringView(value.value<kind>());
    } else {
      copy = value.value<T>();
    }
  }

  return std::make_shared<ConstantVector<T>>(
      pool,
      size,
      value.isNull(),
      Type::create<kind>(),
      std::move(copy),
      SimpleVectorStats<T>{},
      sizeof(T) /*representedByteCount*/);
}

template <>
VectorPtr newConstant<TypeKind::SHORT_DECIMAL>(
    variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  // ShortDecimal variant is not supported to create
  // constant vector.
  VELOX_UNSUPPORTED();
}

template <>
VectorPtr newConstant<TypeKind::LONG_DECIMAL>(
    variant& value,
    vector_size_t size,
    velox::memory::MemoryPool* pool) {
  // LongDecimal variant is not supported to create
  // constant vector.
  VELOX_UNSUPPORTED();
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

// static
bool BaseVector::isReusableFlatVector(const VectorPtr& vector) {
  // If the main shared_ptr has more than one references, or if it's not a flat
  // vector, can't reuse.
  if (!vector.unique() || !isFlat(vector->encoding())) {
    return false;
  }

  // Now check if nulls and values buffers also have a single reference and
  // are mutable.
  auto checkNullsAndValueBuffers = [&]() {
    const auto& nulls = vector->nulls();
    if (!nulls || (nulls->unique() && nulls->isMutable())) {
      const auto& values = vector->values();
      if (!values || (values->unique() && values->isMutable())) {
        return true;
      }
    }
    return false;
  };

  // Check that all string buffers are single referenced.
  auto checkStringBuffers = [&]() {
    if (vector->typeKind_ == TypeKind::VARBINARY ||
        vector->typeKind_ == TypeKind::VARCHAR) {
      for (auto& buffer : vector->asFlatVector<StringView>()->stringBuffers()) {
        if (buffer->refCount() > 1) {
          return false;
        }
      }
    }
    return true;
  };
  return checkNullsAndValueBuffers() && checkStringBuffers();
}

uint64_t BaseVector::estimateFlatSize() const {
  if (length_ == 0) {
    return 0;
  }

  if (isLazyNotLoaded(*this)) {
    return 0;
  }

  auto leaf = wrappedVector();
  VELOX_DCHECK_GT(leaf->size(), 0);
  auto avgRowSize = 1.0 * leaf->retainedSize() / leaf->size();
  return length_ * avgRowSize;
}

namespace {
bool isFlatEncoding(VectorEncoding::Simple encoding) {
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
  if (!vector.unique() || !isFlatEncoding(vector->encoding())) {
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

} // namespace velox
} // namespace facebook
