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
};

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

template <typename TOffset>
void exportFlatStringVector(
    FlatVector<StringView>* vector,
    ArrowArray& arrowArray,
    VeloxToArrowBridgeHolder& bridgeHolder,
    memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(vector);

  // Quick first pass to calculate the buffer size for a single allocation.
  size_t bufferSize = 0;
  for (size_t i = 0; i < vector->size(); i++) {
    bufferSize += vector->valueAtFast(i).size();
  }

  // Allocate raw string buffer.
  bridgeHolder.setBuffer(2, AlignedBuffer::allocate<char>(bufferSize, pool));
  char* rawBuffer = bridgeHolder.getBufferAs<char>(2);

  // Allocate offset buffer.
  bridgeHolder.setBuffer(
      1, AlignedBuffer::allocate<TOffset>(vector->size() + 1, pool));
  TOffset* rawOffsets = bridgeHolder.getBufferAs<TOffset>(1);
  *rawOffsets = 0;

  // Second pass to actually copy the string data and set offsets.
  for (size_t i = 0; i < vector->size(); i++) {
    // Copy string content.
    const StringView& sv = vector->valueAtFast(i);
    std::memcpy(rawBuffer, sv.data(), sv.size());
    rawBuffer += sv.size();

    // Set offset.
    *(rawOffsets + 1) = *rawOffsets + sv.size();
    ++rawOffsets;
  }
  VELOX_CHECK_EQ(bufferSize, *rawOffsets);
}

void exportFlatVector(
    const VectorPtr& vector,
    ArrowArray& arrowArray,
    VeloxToArrowBridgeHolder& bridgeHolder,
    memory::MemoryPool* pool) {
  switch (vector->typeKind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      bridgeHolder.setBuffer(1, vector->values());
      arrowArray.n_buffers = 2;
      break;

    // Returned string types always assume int32_t offsets.
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      exportFlatStringVector<int32_t>(
          vector->asFlatVector<StringView>(), arrowArray, bridgeHolder, pool);
      arrowArray.n_buffers = 3;
      break;

    default:
      VELOX_NYI(
          "Conversion of FlatVector of {} is not supported yet.",
          vector->typeKind());
  }
}

void exportRowVector(
    const RowVectorPtr& rowVector,
    ArrowArray& arrowArray,
    VeloxToArrowBridgeHolder& bridgeHolder,
    memory::MemoryPool* pool) {
  const size_t numChildren = rowVector->childrenSize();
  bridgeHolder.resizeChildren(numChildren);

  // Convert each child.
  for (size_t i = 0; i < numChildren; ++i) {
    auto childVector = BaseVector::loadedVectorShared(rowVector->childAt(i));
    exportToArrow(childVector, *bridgeHolder.allocateChild(i), pool);
  }

  // Acquire children ArrowArray pointers.
  arrowArray.n_children = numChildren;
  arrowArray.children = bridgeHolder.getChildrenArrays();
}

// Returns the Arrow C data interface format type for a given Velox type.
const char* exportArrowFormatStr(const TypePtr& type) {
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
      return "+l"; // list
    case TypeKind::MAP:
      return "+m"; // map
    case TypeKind::ROW:
      return "+s"; // struct

    default:
      VELOX_NYI("Unable to map type '{}' to ArrowSchema.", type->kind());
  }
}

} // namespace

void exportToArrow(
    const VectorPtr& vector,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
  // Bridge holder is stored in private_data, which is a C-compatible naked
  // pointer. However, since this function can throw (unsupported conversion
  // type, for instance), we temporarily use a unique_ptr to ensure the bridge
  // holder is released in case this function fails.
  //
  // Since this unique_ptr dies with this function and we'll need this bridge
  // alive, the last step in this function is to release this unique_ptr.
  auto bridgeHolder = std::make_unique<VeloxToArrowBridgeHolder>();
  arrowArray.buffers = bridgeHolder->getArrowBuffers();
  arrowArray.release = bridgeRelease;
  arrowArray.length = vector->size();
  // Following the fix described in the below Jira:
  // https://issues.apache.org/jira/browse/ARROW-15846
  arrowArray.n_buffers = 1;
  arrowArray.n_children = 0;
  arrowArray.children = nullptr;

  // Velox does not support offset'ed vectors yet.
  arrowArray.offset = 0;

  // Setting up buffer pointers. First one is always nulls.
  if (vector->nulls()) {
    bridgeHolder->setBuffer(0, vector->nulls());

    // getNullCount() returns a std::optional. -1 means we don't have the count
    // available yet (and we don't want to count it here).
    arrowArray.null_count = vector->getNullCount().value_or(-1);
  } else {
    // If no nulls buffer, it means we have exactly zero nulls.
    arrowArray.null_count = 0;
  }

  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      exportFlatVector(vector, arrowArray, *bridgeHolder, pool);
      break;

    case VectorEncoding::Simple::ROW:
      exportRowVector(
          std::dynamic_pointer_cast<RowVector>(vector),
          arrowArray,
          *bridgeHolder,
          pool);
      break;

    default:
      VELOX_NYI("{} cannot be exported to Arrow yet.", vector->encoding());
      break;
  }

  // TODO: No dictionaries for now.
  arrowArray.dictionary = nullptr;

  // We release the unique_ptr since bridgeHolder will now be carried inside
  // ArrowArray.
  arrowArray.private_data = bridgeHolder.release();
}

void exportToArrow(const TypePtr& type, ArrowSchema& arrowSchema) {
  arrowSchema.format = exportArrowFormatStr(type);
  arrowSchema.name = nullptr;

  // No additional metadata or support for dictionaries for now.
  arrowSchema.metadata = nullptr;
  arrowSchema.dictionary = nullptr;

  // All supported types are semantically nullable.
  arrowSchema.flags = ARROW_FLAG_NULLABLE;

  // Allocate private data buffer holder and recurse down to children types.
  auto bridgeHolder = std::make_unique<VeloxToArrowSchemaBridgeHolder>();

  if (type->kind() == TypeKind::MAP) {
    // Need to wrap the key and value types in a struct type.
    VELOX_DCHECK_EQ(type->size(), 2);
    auto child = std::make_unique<ArrowSchema>();
    exportToArrow(
        ROW({"key", "value"}, {type->childAt(0), type->childAt(1)}), *child);
    child->name = "entries";
    bridgeHolder->childrenOwned.resize(1);
    bridgeHolder->childrenRaw.resize(1);
    bridgeHolder->childrenOwned[0] = std::move(child);
    arrowSchema.children = bridgeHolder->childrenRaw.data();
    arrowSchema.n_children = 1;
    arrowSchema.children[0] = bridgeHolder->childrenOwned[0].get();

  } else if (const size_t numChildren = type->size(); numChildren > 0) {
    bridgeHolder->childrenRaw.resize(numChildren);
    bridgeHolder->childrenOwned.resize(numChildren);

    // If this is a RowType, hold the shared_ptr so we can set the
    // ArrowSchema.name pointer to its internal `name` string.
    if (type->kind() == TypeKind::ROW) {
      bridgeHolder->rowType = std::dynamic_pointer_cast<const RowType>(type);
    }

    arrowSchema.children = bridgeHolder->childrenRaw.data();
    arrowSchema.n_children = numChildren;

    for (size_t i = 0; i < numChildren; ++i) {
      // Recurse down the children. We use the same trick of temporarily holding
      // the buffer in a unique_ptr so it doesn't leak if the recursion throws.
      //
      // But this is more nuanced: for types with a list of children (like
      // row/structs), if one of the children throws, we need to make sure we
      // call release() on the children that have already been created before we
      // re-throw the exception back to the client, or memory will leak. This is
      // needed because Arrow doesn't define what the client needs to do if the
      // conversion fails, so we can't expect the client to call the release()
      // method.
      try {
        auto& currentSchema = bridgeHolder->childrenOwned[i];
        currentSchema = std::make_unique<ArrowSchema>();
        exportToArrow(type->childAt(i), *currentSchema);

        if (bridgeHolder->rowType) {
          currentSchema->name = bridgeHolder->rowType->nameOf(i).data();
        } else if (type->kind() == TypeKind::ARRAY) {
          // Name is required, and "item" is the default name used in arrow
          // itself.
          currentSchema->name = "item";
        }
        arrowSchema.children[i] = currentSchema.get();
      } catch (const VeloxException& e) {
        // Release any children that have already been built before re-throwing
        // the exception back to the client.
        for (size_t j = 0; j < i; ++j) {
          arrowSchema.children[j]->release(arrowSchema.children[j]);
        }
        throw;
      }
    }
  } else {
    arrowSchema.n_children = 0;
    arrowSchema.children = nullptr;
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
      nullCount == -1 ? std::nullopt : std::optional<int64_t>(nullCount));
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
      nullCount == -1 ? std::nullopt : std::optional<int64_t>(nullCount));
}

VectorPtr importFromArrowImpl(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    memory::MemoryPool* pool,
    bool isViewer,
    WrapInBufferViewFunc wrapInBufferView);

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
    childrenVector.emplace_back(
        isViewer
            ? importFromArrowAsViewer(
                  *arrowSchema.children[i], *arrowArray.children[i], pool)
            : importFromArrowAsOwner(
                  *arrowSchema.children[i], *arrowArray.children[i], pool));
  }
  return std::make_shared<RowVector>(
      pool,
      rowType,
      nulls,
      arrowArray.length,
      std::move(childrenVector),
      arrowArray.null_count == -1
          ? std::nullopt
          : std::optional<int64_t>(arrowArray.null_count));
}

VectorPtr importFromArrowImpl(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    memory::MemoryPool* pool,
    bool isViewer,
    WrapInBufferViewFunc wrapInBufferView) {
  VELOX_USER_CHECK_NOT_NULL(arrowSchema.release, "arrowSchema was released.");
  VELOX_USER_CHECK_NOT_NULL(arrowArray.release, "arrowArray was released.");
  VELOX_USER_CHECK_NULL(
      arrowArray.dictionary,
      "Dictionary encoded arrowArrays not supported yet.");
  VELOX_USER_CHECK_EQ(
      arrowArray.offset,
      0,
      "Offsets are not supported during arrow conversion yet.");
  VELOX_CHECK_GE(arrowArray.length, 0, "Array length needs to be positive.");

  // First parse and generate a Velox type.
  auto type = importFromArrow(arrowSchema);

  // Only primitive types and row/struct supported for now.
  VELOX_CHECK(
      type->isPrimitiveType() || type->isRow(),
      "Conversion of '{}' from arrow not supported yet.",
      type->toString());

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
  else if (type->isRow()) {
    return createRowVector(
        pool,
        std::dynamic_pointer_cast<const RowType>(type),
        nulls,
        arrowSchema,
        arrowArray,
        isViewer);
  }
  // Other primitive types.
  else {
    // Wrap the values buffer into a Velox BufferView - zero-copy.
    VELOX_USER_CHECK_EQ(
        arrowArray.n_buffers,
        2,
        "Primitive types expect two buffers as input.");
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
}

} // namespace

VectorPtr importFromArrowAsViewer(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
  return importFromArrowImpl(
      arrowSchema,
      arrowArray,
      pool,
      /*isViewer=*/true,
      wrapInBufferViewAsViewer);
}

VectorPtr importFromArrowAsOwner(
    ArrowSchema& arrowSchema,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
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

} // namespace facebook::velox
