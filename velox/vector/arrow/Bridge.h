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

#include "velox/vector/BaseVector.h"
#include "velox/vector/arrow/Abi.h"

namespace facebook::velox::arrow {

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
/// being referenced, so the consumer don't need to explicitly hold on to the
/// input Vector shared_ptr.
///
/// The function throws in case the conversion is not implemented yet.
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
void exportToArrow(const VectorPtr& vector, ArrowArray& arrowArray);

} // namespace facebook::velox::arrow
