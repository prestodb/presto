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

#include "velox/experimental/wave/dwio/StructColumnReader.h"

namespace facebook ::velox::wave {

bool StructColumnReader::isChildConstant(
    const velox::common::ScanSpec& childSpec) const {
  // Returns true if the child has a constant set in the ScanSpec, or if the
  // file doesn't have this child (in which case it will be treated as null).
  return childSpec.isConstant() ||
      // The below check is trying to determine if this is a missing field in a
      // struct that should be constant null.
      (!isRoot_ && // If we're in the root struct channel is meaningless in this
                   // context and it will be a null constant anyway if it's
                   // missing.
       childSpec.channel() !=
           velox::common::ScanSpec::kNoChannel && // This can happen if there's
                                                  // a filter on a subfield of a
                                                  // row type that doesn't exist
                                                  // in the output.
       fileType_->type()->kind() !=
           TypeKind::MAP && // If this is the case it means this is a flat map,
                            // so it can't have "missing" fields.
       childSpec.channel() >= fileType_->size());
}

} // namespace facebook::velox::wave
