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
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::arrow {

namespace {

// TODO: The initial supported conversions only use one buffer for nulls and
// one for value, but we'll need more once we support strings and complex types.
static constexpr size_t kMaxBuffers{2};

// Structure that will hold the buffers needed by ArrowArray. This is opaquely
// carried by ArrowArray.private_data
struct VeloxToArrowBridgeHolder {
  // Holds a shared_ptr to the vector being bridged, to ensure its lifetime.
  VectorPtr vector;

  // Holds the pointers to buffers.
  const void* buffers[kMaxBuffers];
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

void exportFlatVector(const VectorPtr& vector, ArrowArray& arrowArray) {
  switch (vector->typeKind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      arrowArray.buffers[1] = vector->valuesAsVoid();
      break;

    default:
      VELOX_NYI(
          "Conversion of FlatVector of {} is not supported yet.",
          vector->typeKind());
  }
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
    case TypeKind::VARCHAR:
      return "u"; // utf-8 string
    case TypeKind::VARBINARY:
      return "z"; // binary
    case TypeKind::TIMESTAMP:
      // TODO: need to figure out how we'll map this since in Velox we currently
      // store timestamps as two int64s (epoch in sec and nanos).
      return "ttn"; // time64 [nanoseconds]

    // Complex/nested types.
    case TypeKind::ARRAY:
      return "+L"; // large list
    case TypeKind::MAP:
      return "+m"; // map
    case TypeKind::ROW:
      return "+s"; // struct

    default:
      VELOX_NYI("Unable to map type '{}' to ArrowSchema.", type->kind());
  }
}

} // namespace

void exportToArrow(const VectorPtr& vector, ArrowArray& arrowArray) {
  // Bridge holder is stored in private_data, which is a C-compatible naked
  // pointer. However, since this function can throw (unsupported conversion
  // type, for instance), we temporarily use a unique_ptr to ensure the bridge
  // holder is released in case this function fails.
  //
  // Since this unique_ptr dies with this function and we'll need this bridge
  // alive, the last step in this function is to release this unique_ptr.
  auto bridgeHolder = std::make_unique<VeloxToArrowBridgeHolder>();
  bridgeHolder->vector = vector;
  arrowArray.n_buffers = kMaxBuffers;
  arrowArray.buffers = bridgeHolder->buffers;
  arrowArray.release = bridgeRelease;
  arrowArray.length = vector->size();

  // getNullCount() returns a std::optional. -1 means we don't have the count
  // available yet (and we don't want to count it here).
  arrowArray.null_count = vector->getNullCount().value_or(-1);

  // Velox does not support offset'ed vectors yet.
  arrowArray.offset = 0;

  // Setting up buffer pointers. First one is always nulls.
  arrowArray.buffers[0] = vector->rawNulls();

  // Second buffer is values. Only support flat for now.
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      exportFlatVector(vector, arrowArray);
      break;

    default:
      VELOX_NYI("Only FlatVectors can be exported to Arrow for now.");
      break;
  }

  // TODO: No nested types, strings, or dictionaries for now.
  arrowArray.n_children = 0;
  arrowArray.children = nullptr;
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
  const size_t numChildren = type->size();

  if (numChildren > 0) {
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
      break;

    // Complex types.
    case '+': {
      switch (format[1]) {
        // Array/list.
        case 'L':
          VELOX_CHECK_EQ(arrowSchema.n_children, 1);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[0]);
          return ARRAY(importFromArrow(*arrowSchema.children[0]));

        // Map.
        case 'm':
          VELOX_CHECK_EQ(arrowSchema.n_children, 2);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[0]);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[1]);
          return MAP(
              importFromArrow(*arrowSchema.children[0]),
              importFromArrow(*arrowSchema.children[1]));

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

} // namespace facebook::velox::arrow
