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

#include "velox/dwio/dwrf/writer/Writer.h"

namespace facebook::velox::dwrf {

void Writer::write(const VectorPtr& slice) {
  auto& context = getContext();
  vector_size_t offset = 0;
  // Calculate length increment based on linear projection of micro batch size.
  // Total length is capped later.
  const vector_size_t lengthIncrement = std::max(
      1,
      slice->retainedSize() > 0 ? folly::to<vector_size_t>(std::floor(
                                      1.0 * context.rawDataSizePerBatch /
                                      slice->retainedSize() * slice->size()))
                                : slice->size());
  while (offset < slice->size()) {
    vector_size_t length = lengthIncrement;
    if (context.isIndexEnabled) {
      length = std::min(
          length,
          folly::to<vector_size_t>(
              context.indexStride - context.indexRowCount));
    }

    length = std::min(length, slice->size() - offset);
    VELOX_CHECK_GT(length, 0);
    if (shouldFlush(context, length)) {
      flush();
    }

    auto rawSize = writer_->write(slice, Ranges::of(offset, offset + length));
    offset += length;
    getContext().incRawSize(rawSize);

    if (context.isIndexEnabled &&
        context.indexRowCount >= context.indexStride) {
      createRowIndexEntry();
    }
  }
}

} // namespace facebook::velox::dwrf
