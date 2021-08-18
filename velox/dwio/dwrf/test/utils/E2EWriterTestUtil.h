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

#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/dwrf/writer/Writer.h"

namespace facebook::velox::dwrf {

class E2EWriterTestUtil {
 public:
  static std::unique_ptr<Writer> writeData(
      std::unique_ptr<dwio::common::DataSink> sink,
      const std::shared_ptr<const Type>& type,
      const std::vector<VectorPtr>& batches,
      const std::shared_ptr<Config>& config,
      const bool useDefaultFlushPolicy,
      const bool flushPerBatch,
      const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max());

  static void testWriter(
      memory::MemoryPool& pool,
      const std::shared_ptr<const Type>& type,
      const std::vector<VectorPtr>& batches,
      // Memory footprint for F14 map under different compilers could be
      // different.
      size_t numStripesLower,
      size_t numStripesUpper,
      const std::shared_ptr<Config>& config,
      const bool useDefaultFlushPolicy = false,
      const bool flushPerBatch = true,
      const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max(),
      const bool verifyContent = true);

  static std::vector<VectorPtr> generateBatches(
      const std::shared_ptr<const Type>& type,
      size_t batchCount,
      size_t size,
      uint32_t seed,
      memory::MemoryPool& pool);

  static std::vector<VectorPtr> generateBatches(VectorPtr batch);
};

} // namespace facebook::velox::dwrf
