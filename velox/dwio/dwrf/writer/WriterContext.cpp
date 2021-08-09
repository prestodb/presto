/*
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

#include "velox/dwio/dwrf/writer/WriterContext.h"

namespace facebook::velox::dwrf {

constexpr uint32_t MIN_INDEX_STRIDE = 1000;

void WriterContext::validateConfigs() const {
  // the writer is implemented with strong assumption that index is enabled.
  // Things like dictionary encoding will fail if not. Before we clean that up,
  // always require index to be enabled.
  DWIO_ENSURE(isIndexEnabled, "index is required");
  if (isIndexEnabled) {
    DWIO_ENSURE_GE(indexStride, MIN_INDEX_STRIDE);
    // Java works with signed integer and setting anything above the
    // int32_max will make the java reader fail.
    DWIO_ENSURE_LE(indexStride, INT32_MAX);
  }
  DWIO_ENSURE_GE(
      compressionBlockSize, getConfig(Config::COMPRESSION_BLOCK_SIZE_MIN));
  DWIO_ENSURE_GE(
      getConfig(Config::COMPRESSION_BLOCK_SIZE_EXTEND_RATIO),
      MIN_PAGE_GROW_RATIO);
}

} // namespace facebook::velox::dwrf
