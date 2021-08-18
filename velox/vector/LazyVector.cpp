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

#include "velox/vector/LazyVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox {

void VectorLoader::load(
    const SelectivityVector& rows,
    ValueHook* hook,
    VectorPtr* result) {
  if (rows.isAllSelected()) {
    const auto& indices = DecodedVector::consecutiveIndices();
    assert(!indices.empty());
    if (rows.end() <= indices.size()) {
      load(
          RowSet(&indices[rows.begin()], rows.end() - rows.begin()),
          hook,
          result);
      return;
    }
  }
  std::vector<vector_size_t> positions(rows.countSelected());
  int index = 0;
  rows.applyToSelected([&](vector_size_t row) { positions[index++] = row; });
  load(positions, hook, result);
}
} // namespace facebook::velox
