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
  size_t offset = 0;
  while (offset < slice->size()) {
    size_t length = slice->size() - offset;
    // Length can be further chunked down if we estimate the slice to be too
    // large. Challenging for first write, though, so we still assume reasonable
    // slice size for now, and let the application deal with memory capping
    // exception and retries.
    if (context.isIndexEnabled) {
      length = std::min(
          length,
          static_cast<size_t>(context.indexStride - context.indexRowCount));
    }

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
