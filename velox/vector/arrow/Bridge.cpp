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

// Main release function. Arrow standard requires it to recurse down to children
// and dictionary arrays, and set release and private_data to null to signal it
// has been released.
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

void exportFlatVector(const VectorPtr& vector, ArrowArray& arrowArray) {
  switch (vector->typeKind()) {
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

} // namespace facebook::velox::arrow
