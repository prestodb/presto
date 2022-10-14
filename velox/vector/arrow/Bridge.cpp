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

#include "velox/vector/arrow/Bridge.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Abi.h"

namespace facebook::velox {

namespace {

// The supported conversions use one buffer for nulls (0), one for values (1),
// and one for offsets (2).
static constexpr size_t kMaxBuffers{3};

// Structure that will hold the buffers needed by ArrowArray. This is opaquely
// carried by ArrowArray.private_data
class VeloxToArrowBridgeHolder {
 public:
  VeloxToArrowBridgeHolder() {
    for (size_t i = 0; i < kMaxBuffers; ++i) {
      buffers_[i] = nullptr;
    }
  }

  // Acquires a buffer at index `idx`.
  void setBuffer(size_t idx, const BufferPtr& buffer) {
    bufferPtrs_[idx] = buffer;
    if (buffer) {
      buffers_[idx] = buffer->as<void>();
    }
  }

  template <typename T>
  T* getBufferAs(size_t idx) {
    return bufferPtrs_[idx]->asMutable<T>();
  }

  const void** getArrowBuffers() {
    return buffers_;
  }

  // Allocates space for `numChildren` ArrowArray pointers.
  void resizeChildren(size_t numChildren) {
    childrenPtrs_.resize(numChildren);
    children_ = (numChildren > 0)
        ? std::make_unique<ArrowArray*[]>(sizeof(ArrowArray*) * numChildren)
        : nullptr;
  }

  // Allocates and properly acquires buffers for a child ArrowArray structure.
  ArrowArray* allocateChild(size_t i) {
    VELOX_CHECK_LT(i, childrenPtrs_.size());
    childrenPtrs_[i] = std::make_unique<ArrowArray>();
    children_[i] = childrenPtrs_[i].get();
    return children_[i];
  }

  // Returns the pointer to be used in the parent ArrowArray structure.
  ArrowArray** getChildrenArrays() {
    return children_.get();
  }

  ArrowArray* allocateDictionary() {
    dictionary_ = std::make_unique<ArrowArray>();
    return dictionary_.get();
  }

 private:
  // Holds the pointers to the arrow buffers.
  const void* buffers_[kMaxBuffers];

  // Holds ownership over the Buffers being referenced by the buffers vector
  // above.
  BufferPtr bufferPtrs_[kMaxBuffers];

  // Auxiliary buffers to hold ownership over ArrowArray children structures.
  std::vector<std::unique_ptr<ArrowArray>> childrenPtrs_;

  // Array that will hold pointers to the structures above - to be used by
  // ArrowArray.children
  std::unique_ptr<ArrowArray*[]> children_;

  std::unique_ptr<ArrowArray> dictionary_;
};

// Structure that will hold buffers needed by ArrowSchema. This is opaquely
// carried by ArrowSchema.private_data
struct VeloxToArrowSchemaBridgeHolder {
  // Unfortunately, we need two vectors here since ArrowSchema takes a
  // ArrowSchema** pointer for children (so we can't just cast the
  // vector<unique_ptr<>>), but we also need a member to control the
  // lifetime of the children objects. The following invariable should always
  // hold:
  //   childrenRaw[i] == childrenOwned[i].get()
  std::vector<ArrowSchema*> childrenRaw;
  std::vector<std::unique_ptr<ArrowSchema>> childrenOwned;

  // If the input type is a RowType, we keep the shared_ptr alive so we can set
  // ArrowSchema.name pointer to the internal string that contains the column
  // name.
  RowTypePtr rowType;

  std::unique_ptr<ArrowSchema> dictionary;

  // Buffer required to generate a decimal format.
  std::string formatBuffer;
};

void setUniqueChild(
    std::unique_ptr<ArrowSchema>&& child,
    VeloxToArrowSchemaBridgeHolder& holder,
    ArrowSchema& schema) {
  holder.childrenOwned.resize(1);
  holder.childrenRaw.resize(1);
  holder.childrenOwned[0] = std::move(child);
  schema.children = holder.childrenRaw.data();
  schema.n_children = 1;
  schema.children[0] = holder.childrenOwned[0].get();
}

// Release function for ArrowArray. Arrow standard requires it to recurse down
// to children and dictionary arrays, and set release and private_data to null
// to signal it has been released.
static void bridgeRelease(ArrowArray* arrowArray) {
  if (!arrowArray || !arrowArray->release) {
    return;
  }

  // Recurse down to release children arrays.
  for (int64_t i = 0; i < arrowArray->n_children; ++i) {
    ArrowArray* child = arrowArray->children[i];
    if (child != nullptr && child->release != nullptr) {
      child->release(child);
      VELOX_CHECK_NULL(child->release);
    }
  }

  // Release dictionary.
  ArrowArray* dict = arrowArray->dictionary;
  if (dict != nullptr && dict->release != nullptr) {
    dict->release(dict);
    VELOX_CHECK_NULL(dict->release);
  }

  // Destroy the current holder.
  auto* bridgeHolder =
      static_cast<VeloxToArrowBridgeHolder*>(arrowArray->private_data);
  delete bridgeHolder;

  // Finally, mark the array as released.
  arrowArray->release = nullptr;
  arrowArray->private_data = nullptr;
}

// Release function for ArrowSchema. Arrow standard requires it to recurse down
// to all children, and set release and private_data to null to signal it has
// been released.
static void bridgeSchemaRelease(ArrowSchema* arrowSchema) {
  if (!arrowSchema || !arrowSchema->release) {
    return;
  }

  // Recurse down to release children arrays.
  for (int64_t i = 0; i < arrowSchema->n_children; ++i) {
    ArrowSchema* child = arrowSchema->children[i];
    if (child != nullptr && child->release != nullptr) {
      child->release(child);
      VELOX_CHECK_NULL(child->release);
    }
  }

  // Release dictionary.
  ArrowSchema* dict = arrowSchema->dictionary;
  if (dict != nullptr && dict->release != nullptr) {
    dict->release(dict);
    VELOX_CHECK_NULL(dict->release);
  }

  // Destroy the current holder.
  auto* bridgeHolder =
      static_cast<VeloxToArrowSchemaBridgeHolder*>(arrowSchema->private_data);
  delete bridgeHolder;

  // Finally, mark the array as released.
  arrowSchema->release = nullptr;
  arrowSchema->private_data = nullptr;
}

// Returns the Arrow C data interface format type for a given Velox type.
const char* exportArrowFormatStr(
    const TypePtr& type,
    std::string& formatBuffer) {
  switch (type->kind()) {
    // Scalar types.
    case TypeKind::BOOLEAN:
      return "b"; // boolean
    case TypeKind::TINYINT:
      return "c"; // int8
    case TypeKind::SMALLINT:
      return "s"; // int16
    case TypeKind::INTEGER:
      return "i"; // int32
    case TypeKind::BIGINT:
      return "l"; // int64
    case TypeKind::REAL:
      return "f"; // float32
    case TypeKind::DOUBLE:
      return "g"; // float64
    // Decimal types encode the precision, scale values.
    case TypeKind::SHORT_DECIMAL:
    case TypeKind::LONG_DECIMAL: {
      const auto& [precision, scale] = getDecimalPrecisionScale(*type);
      formatBuffer = fmt::format("d:{},{}", precision, scale);
      return formatBuffer.c_str();
    }
    // We always map VARCHAR and VARBINARY to the "small" version (lower case
    // format string), which uses 32 bit offsets.
    case TypeKind::VARCHAR:
      return "u"; // utf-8 string
    case TypeKind::VARBINARY:
      return "z"; // binary

    case TypeKind::TIMESTAMP:
      // TODO: need to figure out how we'll map this since in Velox we currently
      // store timestamps as two int64s (epoch in sec and nanos).
      return "ttn"; // time64 [nanoseconds]
    case TypeKind::DATE:
      return "tdD"; // date32[days]
    // Complex/nested types.
    case TypeKind::ARRAY:
      static_assert(sizeof(vector_size_t) == 4);
      return "+l"; // list
    case TypeKind::MAP:
      return "+m"; // map
    case TypeKind::ROW:
      return "+s"; // struct

    default:
      VELOX_NYI("Unable to map type '{}' to ArrowSchema.", type->kind());
  }
}

// A filter representation that can also keep the order.
struct Selection {
  explicit Selection(vector_size_t total) : total_(total) {}

  // Whether filtering or reorder should be applied to the original elements.
  bool changed() const {
    return static_cast<bool>(ranges_);
  }

  template <typename F>
  void apply(F&& f) const {
    if (changed()) {
      for (auto [offset, size] : *ranges_) {
        for (vector_size_t i = 0; i < size; ++i) {
          f(offset + i);
        }
      }
    } else {
      for (vector_size_t i = 0; i < total_; ++i) {
        f(i);
      }
    }
  }

  vector_size_t count() const {
    if (!changed()) {
      return total_;
    }
    vector_size_t ans = 0;
    for (auto [_, size] : *ranges_) {
      ans += size;
    }
    return ans;
  }

  void clearAll() {
    ranges_ = std::vector<std::pair<vector_size_t, vector_size_t>>();
  }

  void addRange(vector_size_t offset, vector_size_t size) {
    VELOX_DCHECK(ranges_);
    ranges_->emplace_back(offset, size);
  }

 private:
  std::optional<std::vector<std::pair<vector_size_t, vector_size_t>>> ranges_;
  vector_size_t total_;
};

void gatherFromBuffer(
    const Type& type,
    const Buffer& buf,
    const Selection& rows,
    Buffer& out) {
  auto src = buf.as<uint8_t>();
  auto dst = out.asMutable<uint8_t>();
  vector_size_t j = 0; // index into dst
  if (type.kind() == TypeKind::BOOLEAN) {
    rows.apply([&](vector_size_t i) {
      bits::setBit(dst, j++, bits::isBitSet(src, i));
    });
  } else if (type.kind() == TypeKind::SHORT_DECIMAL) {
    rows.apply([&](vector_size_t i) {
      auto decimalSrc = buf.as<UnscaledShortDecimal>();
      int128_t value = decimalSrc[i].unscaledValue();
      memcpy(dst + (j++) * sizeof(int128_t), &value, sizeof(int128_t));
    });
  } else {
    auto typeSize = type.cppSizeInBytes();
    rows.apply([&](vector_size_t i) {
      memcpy(dst + (j++) * typeSize, src + i * typeSize, typeSize);
    });
  }
}

void exportNulls(
    const BaseVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  if (!vec.nulls()) {
    out.null_count = 0;
    return;
  }
  if (!rows.changed()) {
    holder.setBuffer(0, vec.nulls());
    out.null_count = vec.getNullCount().value_or(-1);
    return;
  }
  auto nulls = AlignedBuffer::allocate<bool>(out.length, pool);
  gatherFromBuffer(*BOOLEAN(), *vec.nulls(), rows, *nulls);
  holder.setBuffer(0, nulls);
  out.null_count = -1;
}

void exportValues(
    const BaseVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  out.n_buffers = 2;
  // Short decimals need to be converted to 128 bit values as they are mapped
  // to Arrow Decimal128.
  if (!rows.changed() && !vec.type()->isShortDecimal()) {
    holder.setBuffer(1, vec.values());
    return;
  }
  auto size = vec.type()->isShortDecimal() ? sizeof(int128_t)
                                           : vec.type()->cppSizeInBytes();
  auto values = vec.type()->isBoolean()
      ? AlignedBuffer::allocate<bool>(out.length, pool)
      : AlignedBuffer::allocate<uint8_t>(
            checkedMultiply<size_t>(out.length, size), pool);
  gatherFromBuffer(*vec.type(), *vec.values(), rows, *values);
  holder.setBuffer(1, values);
}

void exportStrings(
    const FlatVector<StringView>& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  out.n_buffers = 3;
  size_t bufSize = 0;
  rows.apply([&](vector_size_t i) {
    if (!vec.isNullAt(i)) {
      bufSize += vec.valueAtFast(i).size();
    }
  });
  holder.setBuffer(2, AlignedBuffer::allocate<char>(bufSize, pool));
  char* rawBuffer = holder.getBufferAs<char>(2);
  VELOX_CHECK_LT(bufSize, std::numeric_limits<int32_t>::max());
  auto offsetLen = checkedPlus<size_t>(out.length, 1);
  holder.setBuffer(1, AlignedBuffer::allocate<int32_t>(offsetLen, pool));
  auto* rawOffsets = holder.getBufferAs<int32_t>(1);
  *rawOffsets = 0;
  rows.apply([&](vector_size_t i) {
    auto newOffset = *rawOffsets;
    if (!vec.isNullAt(i)) {
      auto& sv = vec.valueAtFast(i);
      memcpy(rawBuffer, sv.data(), sv.size());
      rawBuffer += sv.size();
      newOffset += sv.size();
    }
    *++rawOffsets = newOffset;
  });
  VELOX_DCHECK_EQ(bufSize, *rawOffsets);
}

void exportFlat(
    const BaseVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  out.n_children = 0;
  out.children = nullptr;
  switch (vec.typeKind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::SHORT_DECIMAL:
    case TypeKind::LONG_DECIMAL:
      exportValues(vec, rows, out, pool, holder);
      break;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      exportStrings(
          *vec.asUnchecked<FlatVector<StringView>>(), rows, out, pool, holder);
      break;
    default:
      VELOX_NYI(
          "Conversion of FlatVector of {} is not supported yet.",
          vec.typeKind());
  }
}

void exportBase(
    const BaseVector&,
    const Selection&,
    ArrowArray&,
    memory::MemoryPool*);

void exportRows(
    const RowVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  out.n_buffers = 1;
  holder.resizeChildren(vec.childrenSize());
  out.n_children = vec.childrenSize();
  out.children = holder.getChildrenArrays();
  for (column_index_t i = 0; i < vec.childrenSize(); ++i) {
    try {
      exportBase(
          *vec.childAt(i)->loadedVector(),
          rows,
          *holder.allocateChild(i),
          pool);
    } catch (const VeloxException&) {
      for (column_index_t j = 0; j < i; ++j) {
        // When exception is thrown, i th child is guaranteed unset.
        out.children[j]->release(out.children[j]);
      }
      throw;
    }
  }
}

template <typename Vector>
bool isCompact(const Vector& vec) {
  for (vector_size_t i = 1; i < vec.size(); ++i) {
    if (vec.offsetAt(i - 1) + vec.sizeAt(i - 1) != vec.offsetAt(i)) {
      return false;
    }
  }
  return true;
}

template <typename Vector>
void exportOffsets(
    const Vector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder,
    Selection& childRows) {
  auto offsets = AlignedBuffer::allocate<vector_size_t>(
      checkedPlus<size_t>(out.length, 1), pool);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  if (!rows.changed() && isCompact(vec)) {
    memcpy(rawOffsets, vec.rawOffsets(), sizeof(vector_size_t) * vec.size());
    rawOffsets[vec.size()] = vec.size() == 0
        ? 0
        : vec.offsetAt(vec.size() - 1) + vec.sizeAt(vec.size() - 1);
  } else {
    childRows.clearAll();
    // j: Index of element we are writing.
    // k: Total size so far.
    vector_size_t j = 0, k = 0;
    rows.apply([&](vector_size_t i) {
      rawOffsets[j++] = k;
      if (!vec.isNullAt(i)) {
        childRows.addRange(vec.offsetAt(i), vec.sizeAt(i));
        k += vec.sizeAt(i);
      }
    });
    VELOX_DCHECK_EQ(j, out.length);
    rawOffsets[j] = k;
  }
  holder.setBuffer(1, offsets);
  out.n_buffers = 2;
}

void exportArrays(
    const ArrayVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  Selection childRows(vec.elements()->size());
  exportOffsets(vec, rows, out, pool, holder, childRows);
  holder.resizeChildren(1);
  exportBase(
      *vec.elements()->loadedVector(),
      childRows,
      *holder.allocateChild(0),
      pool);
  out.n_children = 1;
  out.children = holder.getChildrenArrays();
}

void exportMaps(
    const MapVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  RowVector child(
      pool,
      ROW({"key", "value"}, {vec.mapKeys()->type(), vec.mapValues()->type()}),
      nullptr,
      vec.mapKeys()->size(),
      {vec.mapKeys(), vec.mapValues()});
  Selection childRows(child.size());
  exportOffsets(vec, rows, out, pool, holder, childRows);
  holder.resizeChildren(1);
  exportBase(child, childRows, *holder.allocateChild(0), pool);
  out.n_children = 1;
  out.children = holder.getChildrenArrays();
}

void exportDictionary(
    const BaseVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool,
    VeloxToArrowBridgeHolder& holder) {
  out.n_buffers = 2;
  out.n_children = 0;
  if (rows.changed()) {
    auto indices = AlignedBuffer::allocate<vector_size_t>(out.length, pool);
    gatherFromBuffer(*INTEGER(), *vec.wrapInfo(), rows, *indices);
    holder.setBuffer(1, indices);
  } else {
    holder.setBuffer(1, vec.wrapInfo());
  }
  auto& values = *vec.valueVector()->loadedVector();
  out.dictionary = holder.allocateDictionary();
  exportBase(values, Selection(values.size()), *out.dictionary, pool);
}

void exportBase(
    const BaseVector& vec,
    const Selection& rows,
    ArrowArray& out,
    memory::MemoryPool* pool) {
  auto holder = std::make_unique<VeloxToArrowBridgeHolder>();
  out.buffers = holder->getArrowBuffers();
  out.length = rows.count();
  out.offset = 0;
  out.dictionary = nullptr;
  exportNulls(vec, rows, out, pool, *holder);
  switch (vec.encoding()) {
    case VectorEncoding::Simple::FLAT:
      exportFlat(vec, rows, out, pool, *holder);
      break;
    case VectorEncoding::Simple::ROW:
      exportRows(*vec.asUnchecked<RowVector>(), rows, out, pool, *holder);
      break;
    case VectorEncoding::Simple::ARRAY:
      exportArrays(*vec.asUnchecked<ArrayVector>(), rows, out, pool, *holder);
      break;
    case VectorEncoding::Simple::MAP:
      exportMaps(*vec.asUnchecked<MapVector>(), rows, out, pool, *holder);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      exportDictionary(vec, rows, out, pool, *holder);
      break;
    default:
      VELOX_NYI("{} cannot be exported to Arrow yet.", vec.encoding());
  }
  out.private_data = holder.release();
  out.release = bridgeRelease;
}

} // namespace

void exportToArrow(
    const VectorPtr& vector,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
  exportBase(*vector, Selection(vector->size()), arrowArray, pool);
}

void exportToArrow(const VectorPtr& vec, ArrowSchema& arrowSchema) {
  auto& type = vec->type();

  arrowSchema.name = nullptr;

  // No additional metadata for now.
  arrowSchema.metadata = nullptr;

  // All supported types are semantically nullable.
  arrowSchema.flags = ARROW_FLAG_NULLABLE;

  // Allocate private data buffer holder and recurse down to children types.
  auto bridgeHolder = std::make_unique<VeloxToArrowSchemaBridgeHolder>();

  if (vec->encoding() == VectorEncoding::Simple::DICTIONARY) {
    arrowSchema.n_children = 0;
    arrowSchema.children = nullptr;
    arrowSchema.format = "i";
    bridgeHolder->dictionary = std::make_unique<ArrowSchema>();
    arrowSchema.dictionary = bridgeHolder->dictionary.get();
    exportToArrow(vec->valueVector(), *arrowSchema.dictionary);

  } else {
    arrowSchema.format = exportArrowFormatStr(type, bridgeHolder->formatBuffer);
    arrowSchema.dictionary = nullptr;

    if (type->kind() == TypeKind::MAP) {
      // Need to wrap the key and value types in a struct type.
      VELOX_DCHECK_EQ(type->size(), 2);
      auto child = std::make_unique<ArrowSchema>();
      auto& maps = *vec->asUnchecked<MapVector>();
      auto rows = std::make_shared<RowVector>(
          nullptr,
          ROW({"key", "value"}, {type->childAt(0), type->childAt(1)}),
          nullptr,
          0,
          std::vector<VectorPtr>{maps.mapKeys(), maps.mapValues()},
          maps.getNullCount());
      exportToArrow(rows, *child);
      child->name = "entries";
      setUniqueChild(std::move(child), *bridgeHolder, arrowSchema);

    } else if (type->kind() == TypeKind::ARRAY) {
      auto child = std::make_unique<ArrowSchema>();
      auto& arrays = *vec->asUnchecked<ArrayVector>();
      exportToArrow(arrays.elements(), *child);
      // Name is required, and "item" is the default name used in arrow itself.
      child->name = "item";
      setUniqueChild(std::move(child), *bridgeHolder, arrowSchema);

    } else if (type->kind() == TypeKind::ROW) {
      auto& rows = *vec->asUnchecked<RowVector>();
      auto numChildren = rows.childrenSize();
      bridgeHolder->childrenRaw.resize(numChildren);
      bridgeHolder->childrenOwned.resize(numChildren);

      // Hold the shared_ptr so we can set the ArrowSchema.name pointer to its
      // internal `name` string.
      bridgeHolder->rowType = std::static_pointer_cast<const RowType>(type);

      arrowSchema.children = bridgeHolder->childrenRaw.data();
      arrowSchema.n_children = numChildren;

      for (size_t i = 0; i < numChildren; ++i) {
        // Recurse down the children. We use the same trick of temporarily
        // holding the buffer in a unique_ptr so it doesn't leak if the
        // recursion throws.
        //
        // But this is more nuanced: for types with a list of children (like
        // row/structs), if one of the children throws, we need to make sure we
        // call release() on the children that have already been created before
        // we re-throw the exception back to the client, or memory will leak.
        // This is needed because Arrow doesn't define what the client needs to
        // do if the conversion fails, so we can't expect the client to call the
        // release() method.
        try {
          auto& currentSchema = bridgeHolder->childrenOwned[i];
          currentSchema = std::make_unique<ArrowSchema>();
          exportToArrow(rows.childAt(i), *currentSchema);
          currentSchema->name = bridgeHolder->rowType->nameOf(i).data();
          arrowSchema.children[i] = currentSchema.get();
        } catch (const VeloxException& e) {
          // Release any children that have already been built before
          // re-throwing the exception back to the client.
          for (size_t j = 0; j < i; ++j) {
            arrowSchema.children[j]->release(arrowSchema.children[j]);
          }
          throw;
        }
      }

    } else {
      VELOX_DCHECK_EQ(type->size(), 0);
      arrowSchema.n_children = 0;
      arrowSchema.children = nullptr;
    }
  }

  // Set release callback.
  arrowSchema.release = bridgeSchemaRelease;
  arrowSchema.private_data = bridgeHolder.release();
}

TypePtr importFromArrow(const ArrowSchema& arrowSchema) {
  const char* format = arrowSchema.format;
  VELOX_CHECK_NOT_NULL(format);

  switch (format[0]) {
    case 'b':
      return BOOLEAN();
    case 'c':
      return TINYINT();
    case 's':
      return SMALLINT();
    case 'i':
      return INTEGER();
    case 'l':
      return BIGINT();
    case 'f':
      return REAL();
    case 'g':
      return DOUBLE();

    // Map both utf-8 and large utf-8 string to varchar.
    case 'u':
    case 'U':
      return VARCHAR();

    // Same for binary.
    case 'z':
    case 'Z':
      return VARBINARY();

    case 't': // temporal types.
      // Mapping it to ttn for now.
      if (format[1] == 't' && format[2] == 'n') {
        return TIMESTAMP();
      }
      if (format[1] == 'd' && format[2] == 'D') {
        return DATE();
      }
      break;

    case 'd': { // decimal types.
      try {
        std::string::size_type sz;
        // Parse "d:".
        int precision = std::stoi(&format[2], &sz);
        // Parse ",".
        int scale = std::stoi(&format[2 + sz + 1], &sz);
        return DECIMAL(precision, scale);
      } catch (std::invalid_argument& err) {
        VELOX_USER_FAIL(
            "Unable to convert '{}' ArrowSchema decimal format to Velox decimal",
            format);
      }
    }

    // Complex types.
    case '+': {
      switch (format[1]) {
        // Array/list.
        case 'l':
          VELOX_CHECK_EQ(arrowSchema.n_children, 1);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[0]);
          return ARRAY(importFromArrow(*arrowSchema.children[0]));

        // Map.
        case 'm': {
          VELOX_CHECK_EQ(arrowSchema.n_children, 1);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[0]);
          auto& child = *arrowSchema.children[0];
          VELOX_CHECK_EQ(strcmp(child.format, "+s"), 0);
          VELOX_CHECK_EQ(child.n_children, 2);
          VELOX_CHECK_NOT_NULL(child.children[0]);
          VELOX_CHECK_NOT_NULL(child.children[1]);
          return MAP(
              importFromArrow(*child.children[0]),
              importFromArrow(*child.children[1]));
        }

        // Struct/rows.
        case 's': {
          // Loop collecting the child types and names.
          std::vector<TypePtr> childTypes;
          std::vector<std::string> childNames;
          childTypes.reserve(arrowSchema.n_children);
          childNames.reserve(arrowSchema.n_children);

          for (size_t i = 0; i < arrowSchema.n_children; ++i) {
            VELOX_CHECK_NOT_NULL(arrowSchema.children[i]);
            childTypes.emplace_back(importFromArrow(*arrowSchema.children[i]));
            childNames.emplace_back(
                arrowSchema.children[i]->name != nullptr
                    ? arrowSchema.children[i]->name
                    : "");
          }
          return ROW(std::move(childNames), std::move(childTypes));
        }

        default:
          break;
      }
    } break;

    default:
      break;
  }
  VELOX_USER_FAIL(
      "Unable to convert '{}' ArrowSchema format type to Velox.", format);
}

namespace {
// Optionally, holds shared_ptrs pointing to the ArrowArray object that
// holds the buffer and the ArrowSchema object that describes the ArrowArray,
// which will be released to signal that we will no longer hold on to the data
// and the shared_ptr deleters should run the release procedures if no one
// else is referencing the objects.
struct BufferViewReleaser {
  BufferViewReleaser() : BufferViewReleaser(nullptr, nullptr) {}
  BufferViewReleaser(
      std::shared_ptr<ArrowSchema> arrowSchema,
      std::shared_ptr<ArrowArray> arrowArray)
      : schemaReleaser_(std::move(arrowSchema)),
        arrayReleaser_(std::move(arrowArray)) {}

  void addRef() const {}
  void release() const {}

 private:
  const std::shared_ptr<ArrowSchema> schemaReleaser_;
  const std::shared_ptr<ArrowArray> arrayReleaser_;
};

// Wraps a naked pointer using a Velox buffer view, without copying it. Adding a
// dummy releaser as the buffer lifetime is fully controled by the client of the
// API.
BufferPtr wrapInBufferViewAsViewer(const void* buffer, size_t length) {
  static const BufferViewReleaser kViewerReleaser;
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer), length, kViewerReleaser);
}

// Wraps a naked pointer using a Velox buffer view, without copying it. This
// buffer view uses shared_ptr to manage reference counting and releasing for
// the ArrowSchema object and the ArrowArray object
BufferPtr wrapInBufferViewAsOwner(
    const void* buffer,
    size_t length,
    std::shared_ptr<ArrowSchema> schemaReleaser,
    std::shared_ptr<ArrowArray> arrayReleaser) {
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer),
      length,
      {std::move(schemaReleaser), std::move(arrayReleaser)});
}

std::optional<int64_t> optionalNullCount(int64_t value) {
  return value == -1 ? std::nullopt : std::optional<int64_t>(value);
}

// Dispatch based on the type.
template <TypeKind kind>
VectorPtr createFlatVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    BufferPtr nulls,
    size_t length,
    BufferPtr values,
    int64_t nullCount) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      nulls,
      length,
      values,
      std::vector<BufferPtr>(),
      SimpleVectorStats<T>{},
      std::nullopt,
      optionalNullCount(nullCount));
}

using WrapInBufferViewFunc =
    std::function<BufferPtr(const void* buffer, size_t length)>;

template <typename TOffset>
VectorPtr createStringFlatVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    BufferPtr nulls,
    size_t length,
    const TOffset* offsets,
    const char* values,
    int64_t nullCount,
    WrapInBufferViewFunc wrapInBufferView) {
  BufferPtr stringViews = AlignedBuffer::allocate<StringView>(length, pool);
  auto rawStringViews = stringViews->asMutable<StringView>();
  bool shouldAcquireStringBuffer = false;

  for (size_t i = 0; i < length; ++i) {
    rawStringViews[i] =
        StringView(values + offsets[i], offsets[i + 1] - offsets[i]);
    shouldAcquireStringBuffer |= !rawStringViews[i].isInline();
  }

  std::vector<BufferPtr> stringViewBuffers;
  if (shouldAcquireStringBuffer) {
    stringViewBuffers.emplace_back(
        wrapInBufferView(values, offsets[length + 1]));
  }

  return std::make_shared<FlatVector<StringView>>(
      pool,
      type,
      nulls,
      length,
      stringViews,
      std::move(stringViewBuffers),
      SimpleVectorStats<StringView>{},
      std::nullopt,
      optionalNullCount(nullCount));
}

VectorPtr importFromArrowImpl(
    ArrowSchema& arrowSchema,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool,
    bool isViewer);

RowVectorPtr createRowVector(
    memory::MemoryPool* pool,
    const RowTypePtr& rowType,
    BufferPtr nulls,
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    bool isViewer) {
  VELOX_CHECK_EQ(arrowArray.n_children, rowType->size());

  // Recursively create the children vectors.
  std::vector<VectorPtr> childrenVector;
  childrenVector.reserve(arrowArray.n_children);

  for (size_t i = 0; i < arrowArray.n_children; ++i) {
    childrenVector.push_back(importFromArrowImpl(
        *arrowSchema.children[i], *arrowArray.children[i], pool, isViewer));
  }
  return std::make_shared<RowVector>(
      pool,
      rowType,
      nulls,
      arrowArray.length,
      std::move(childrenVector),
      optionalNullCount(arrowArray.null_count));
}

BufferPtr computeSizes(
    const vector_size_t* offsets,
    int64_t length,
    memory::MemoryPool* pool) {
  auto sizesBuf = AlignedBuffer::allocate<vector_size_t>(length, pool);
  auto sizes = sizesBuf->asMutable<vector_size_t>();
  for (int64_t i = 0; i < length; ++i) {
    // `offsets` here has size length + 1 so i + 1 is valid.
    sizes[i] = offsets[i + 1] - offsets[i];
  }
  return sizesBuf;
}

ArrayVectorPtr createArrayVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    BufferPtr nulls,
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    bool isViewer,
    WrapInBufferViewFunc wrapInBufferView) {
  static_assert(sizeof(vector_size_t) == sizeof(int32_t));
  VELOX_CHECK_EQ(arrowArray.n_buffers, 2);
  VELOX_CHECK_EQ(arrowArray.n_children, 1);
  auto offsets = wrapInBufferView(
      arrowArray.buffers[1], (arrowArray.length + 1) * sizeof(vector_size_t));
  auto sizes =
      computeSizes(offsets->as<vector_size_t>(), arrowArray.length, pool);
  auto elements = importFromArrowImpl(
      *arrowSchema.children[0], *arrowArray.children[0], pool, isViewer);
  return std::make_shared<ArrayVector>(
      pool,
      type,
      std::move(nulls),
      arrowArray.length,
      std::move(offsets),
      std::move(sizes),
      std::move(elements),
      optionalNullCount(arrowArray.null_count));
}

MapVectorPtr createMapVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    BufferPtr nulls,
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    bool isViewer,
    WrapInBufferViewFunc wrapInBufferView) {
  VELOX_CHECK_EQ(arrowArray.n_buffers, 2);
  VELOX_CHECK_EQ(arrowArray.n_children, 1);
  auto offsets = wrapInBufferView(
      arrowArray.buffers[1], (arrowArray.length + 1) * sizeof(vector_size_t));
  auto sizes =
      computeSizes(offsets->as<vector_size_t>(), arrowArray.length, pool);
  // Arrow wraps keys and values into a struct.
  auto entries = importFromArrowImpl(
      *arrowSchema.children[0], *arrowArray.children[0], pool, isViewer);
  VELOX_CHECK(entries->type()->isRow());
  const auto& rows = *entries->asUnchecked<RowVector>();
  VELOX_CHECK_EQ(rows.childrenSize(), 2);
  return std::make_shared<MapVector>(
      pool,
      type,
      std::move(nulls),
      arrowArray.length,
      std::move(offsets),
      std::move(sizes),
      rows.childAt(0),
      rows.childAt(1),
      optionalNullCount(arrowArray.null_count));
}

VectorPtr createDictionaryVector(
    memory::MemoryPool* pool,
    const TypePtr& indexType,
    BufferPtr nulls,
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    bool isViewer,
    WrapInBufferViewFunc wrapInBufferView) {
  VELOX_CHECK_EQ(arrowArray.n_buffers, 2);
  VELOX_CHECK_NOT_NULL(arrowArray.dictionary);
  static_assert(sizeof(vector_size_t) == sizeof(int32_t));
  VELOX_CHECK_EQ(
      indexType->kind(),
      TypeKind::INTEGER,
      "Only int32 indices are supported for arrow conversion");
  auto indices = wrapInBufferView(
      arrowArray.buffers[1], arrowArray.length * sizeof(vector_size_t));
  auto type = importFromArrow(*arrowSchema.dictionary);
  auto wrapped = importFromArrowImpl(
      *arrowSchema.dictionary, *arrowArray.dictionary, pool, isViewer);
  return BaseVector::wrapInDictionary(
      std::move(nulls),
      std::move(indices),
      arrowArray.length,
      std::move(wrapped));
}

VectorPtr importFromArrowImpl(
    ArrowSchema& arrowSchema,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool,
    bool isViewer,
    WrapInBufferViewFunc wrapInBufferView) {
  VELOX_USER_CHECK_NOT_NULL(arrowSchema.release, "arrowSchema was released.");
  VELOX_USER_CHECK_NOT_NULL(arrowArray.release, "arrowArray was released.");
  VELOX_USER_CHECK_EQ(
      arrowArray.offset,
      0,
      "Offsets are not supported during arrow conversion yet.");
  VELOX_CHECK_GE(
      arrowArray.length, 0, "Array length needs to be non-negative.");

  // First parse and generate a Velox type.
  auto type = importFromArrow(arrowSchema);

  // Wrap the nulls buffer into a Velox BufferView (zero-copy). Null buffer size
  // needs to be at least one bit per element.
  BufferPtr nulls = nullptr;

  // If null_count is greater than zero or -1 (unknown), nulls buffer has to
  // present.
  // Otherwise, when null_count is zero, it's legit for the arrow array to have
  // non-null nulls buffer, in that case the converted Velox vector will not
  // have null buffer.
  if (arrowArray.null_count != 0) {
    VELOX_USER_CHECK_NOT_NULL(
        arrowArray.buffers[0],
        "Nulls buffer can't be null unless null_count is zero.");
    nulls = wrapInBufferView(
        arrowArray.buffers[0], bits::nbytes(arrowArray.length));
  }

  if (arrowSchema.dictionary) {
    return createDictionaryVector(
        pool, type, nulls, arrowSchema, arrowArray, isViewer, wrapInBufferView);
  }

  // String data types (VARCHAR and VARBINARY).
  if (type->isVarchar() || type->isVarbinary()) {
    VELOX_USER_CHECK_EQ(
        arrowArray.n_buffers,
        3,
        "Expecting three buffers as input for string types.");
    return createStringFlatVector(
        pool,
        type,
        nulls,
        arrowArray.length,
        static_cast<const int32_t*>(arrowArray.buffers[1]), // offsets
        static_cast<const char*>(arrowArray.buffers[2]), // values
        arrowArray.null_count,
        wrapInBufferView);
  }
  // Row/structs.
  if (type->isRow()) {
    return createRowVector(
        pool,
        std::dynamic_pointer_cast<const RowType>(type),
        nulls,
        arrowSchema,
        arrowArray,
        isViewer);
  }
  if (type->isArray()) {
    return createArrayVector(
        pool, type, nulls, arrowSchema, arrowArray, isViewer, wrapInBufferView);
  }
  if (type->isMap()) {
    return createMapVector(
        pool, type, nulls, arrowSchema, arrowArray, isViewer, wrapInBufferView);
  }
  // Other primitive types.
  VELOX_CHECK(
      type->isPrimitiveType(),
      "Conversion of '{}' from Arrow not supported yet.",
      type->toString());

  // Wrap the values buffer into a Velox BufferView - zero-copy.
  VELOX_USER_CHECK_EQ(
      arrowArray.n_buffers, 2, "Primitive types expect two buffers as input.");
  auto values = wrapInBufferView(
      arrowArray.buffers[1], arrowArray.length * type->cppSizeInBytes());

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      createFlatVector,
      type->kind(),
      pool,
      type,
      nulls,
      arrowArray.length,
      values,
      arrowArray.null_count);
}

VectorPtr importFromArrowImpl(
    ArrowSchema& arrowSchema,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool,
    bool isViewer) {
  if (isViewer) {
    return importFromArrowImpl(
        arrowSchema, arrowArray, pool, isViewer, wrapInBufferViewAsViewer);
  }

  // This Vector will take over the ownership of `arrowSchema` and `arrowArray`
  // by marking them as released and becoming responsible for calling the
  // release callbacks when use count reaches zero. These ArrowSchema object and
  // ArrowArray object will be co-owned by both the BufferVieweReleaser of the
  // nulls buffer and values buffer.
  std::shared_ptr<ArrowSchema> schemaReleaser(
      new ArrowSchema(arrowSchema), [](ArrowSchema* toDelete) {
        if (toDelete != nullptr) {
          if (toDelete->release != nullptr) {
            toDelete->release(toDelete);
          }
          delete toDelete;
        }
      });
  std::shared_ptr<ArrowArray> arrayReleaser(
      new ArrowArray(arrowArray), [](ArrowArray* toDelete) {
        if (toDelete != nullptr) {
          if (toDelete->release != nullptr) {
            toDelete->release(toDelete);
          }
          delete toDelete;
        }
      });
  VectorPtr imported = importFromArrowImpl(
      arrowSchema,
      arrowArray,
      pool,
      /*isViewer=*/false,
      [&schemaReleaser, &arrayReleaser](const void* buffer, size_t length) {
        return wrapInBufferViewAsOwner(
            buffer, length, schemaReleaser, arrayReleaser);
      });

  arrowSchema.release = nullptr;
  arrowArray.release = nullptr;

  return imported;
}

} // namespace

VectorPtr importFromArrowAsViewer(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
  return importFromArrowImpl(
      const_cast<ArrowSchema&>(arrowSchema),
      const_cast<ArrowArray&>(arrowArray),
      pool,
      true);
}

VectorPtr importFromArrowAsOwner(
    ArrowSchema& arrowSchema,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
  return importFromArrowImpl(arrowSchema, arrowArray, pool, false);
}

} // namespace facebook::velox
