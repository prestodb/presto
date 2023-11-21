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

#include "velox/vector/SimpleVector.h"

namespace facebook::velox {

template <>
void SimpleVector<StringView>::validate(
    const VectorValidateOptions& options) const {
  BaseVector::validate(options);

  // We only validate the right size of ascii info, if it has any selection.
  auto rlockedAsciiComputedRows{asciiInfo.readLockedAsciiComputedRows()};
  if (rlockedAsciiComputedRows->hasSelections()) {
    VELOX_CHECK_GE(rlockedAsciiComputedRows->size(), size());
  }
}

} // namespace facebook::velox
