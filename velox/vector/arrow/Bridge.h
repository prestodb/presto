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

#pragma once

#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"

/// These 2 definitions should be included by user from either
///   1. <arrow/c/abi.h> or
///   2. "velox/vector/arrow/Abi.h"
struct ArrowArray;
struct ArrowSchema;

namespace facebook::velox {

/// Export a generic Velox Vector to an ArrowArray, as defined by Arrow's C data
/// interface:
///
///   https://arrow.apache.org/docs/format/CDataInterface.html
///
/// The output ArrowArray needs to be allocated by the consumer (either in the
/// heap or stack), and after usage, the standard REQUIRES the client to call
/// the release() function (or memory will leak).
///
/// After exporting, the ArrowArray will hold ownership to the underlying Vector
/// being referenced, so the consumer does not need to explicitly hold on to the
/// input Vector shared_ptr.
///
/// The function takes a memory pool where allocations will be made (in cases
/// where the conversion is not zero-copy, e.g. for strings) and throws in case
/// the conversion is not implemented yet.
///
/// Example usage:
///
///   ArrowArray arrowArray;
///   arrow::exportToArrow(inputVector, arrowArray);
///   inputVector.reset(); // don't need to hold on to this shared_ptr.
///
///   (use arrowArray)
///
///   arrowArray.release(&arrowArray);
///
void exportToArrow(
    const VectorPtr& vector,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Export the type of a Velox vector to an ArrowSchema.
///
/// The guidelines on API usage and memory management are the same as the ones
/// described for the VectorPtr->ArrowArray export function.
///
/// The function throws in case there was no valid conversion available.
///
/// Example usage:
///
///   ArrowSchema arrowSchema;
///   arrow::exportToArrow(inputType, arrowSchema);
///   inputType.reset(); // don't need to hold on to this shared_ptr.
///
///   (use arrowSchema)
///
///   arrowSchema.release(&arrowSchema);
///
/// NOTE: Since Arrow couples type and encoding, we need both Velox type and
/// actual data (containing encoding) to create an ArrowSchema.
void exportToArrow(const VectorPtr&, ArrowSchema&);

/// Import an ArrowSchema into a Velox Type object.
///
/// This function does the exact opposite of the function above. TypePtr carries
/// all buffers they need to represent types, so after this function returns,
/// the client is free to release any buffers associated with the input
/// ArrowSchema object.
///
/// The function throws in case there was no valid conversion available.
///
/// Example usage:
///
///   ArrowSchema arrowSchema;
///   ... // fills arrowSchema
///   auto type = arrow::importToArrow(arrowSchema);
///
///   arrowSchema.release(&arrowSchema);
///
TypePtr importFromArrow(const ArrowSchema& arrowSchema);

/// Import an ArrowArray and ArrowSchema into a Velox vector.
///
/// This function takes both an ArrowArray (which contains the buffers) and an
/// ArrowSchema (which describes the data type), since a Velox vector needs
/// both (buffer and type). A memory pool is also required, since all vectors
/// carry a pointer to it, but not really used in most cases - unless the
/// conversion itself requires a new allocation. In most cases no new
/// allocations are required, unless for arrays of varchars (or varbinaries) and
/// complex types written out of order.
///
/// The new Velox vector returned contains only references to the underlying
/// buffers, so it's the client's responsibility to ensure the buffer's
/// lifetime.
///
/// The function throws in case the conversion fails.
///
/// Example usage:
///
///   ArrowSchema arrowSchema;
///   ArrowArray arrowArray;
///   ... // fills structures
///   auto vector = arrow::importToArrow(arrowSchema, arrowArray, pool);
///   ... // ensure buffers in arrowArray remain alive while vector is used.
///
VectorPtr importFromArrowAsViewer(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Import an ArrowArray and ArrowSchema into a Velox vector, acquiring
/// ownership over the input data.
///
/// Similar to importFromArrowAsViewer but the ownership of arrowSchema and
/// arrowArray will be taken over. Specifically, the returned Vector will own a
/// copy of arrowSchema and arrowArray.
///
/// The inputs arrowSchema and arrowArray will be marked as released by setting
/// their release callback to nullptr
/// (https://arrow.apache.org/docs/format/CDataInterface.html). Afterwards, the
/// returned Vector will be responsible for calling the release callbacks when
/// destructed.
VectorPtr importFromArrowAsOwner(
    ArrowSchema& arrowSchema,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

} // namespace facebook::velox
