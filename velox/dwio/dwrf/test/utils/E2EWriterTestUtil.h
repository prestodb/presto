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
  /**
   * Writes data and returns the writer so that the caller can control
   * its life cycle. The writer is constructed with the supplied parameters.
   *    sink                    the container for writer output, could be
   *                            in-memory or on-disk.
   *    type                    schema of the output data
   *    batches                 generated data
   *    config                  ORC configs
   *    flushPolicyFactory      supplies the policy writer use to determine
   *                            when to flush the current stripe and start a
   *                            new one
   *    layoutPlannerFactory    supplies the layout planner and determine how
   *                            order of the data streams prior to flush
   *    writerMemoryCap         total memory budget for the writer
   */
  static std::unique_ptr<Writer> writeData(
      std::unique_ptr<dwio::common::DataSink> sink,
      const std::shared_ptr<const Type>& type,
      const std::vector<VectorPtr>& batches,
      const std::shared_ptr<Config>& config,
      std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory =
          nullptr,
      std::function<
          std::unique_ptr<LayoutPlanner>(StreamList, const EncodingContainer&)>
          layoutPlannerFactory = nullptr,
      const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max());

  /**
   * Creates a writer with the supplied configuration and check the IO
   * characteristics and the content of its output. Uses writeData to perform
   * the data write and pass through most of the parameters.
   */
  static void testWriter(
      memory::MemoryPool& pool,
      const std::shared_ptr<const Type>& type,
      const std::vector<VectorPtr>& batches,
      // Memory footprint for F14 map under different compilers could be
      // different.
      size_t numStripesLower,
      size_t numStripesUpper,
      const std::shared_ptr<Config>& config,
      std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory =
          nullptr,
      std::function<
          std::unique_ptr<LayoutPlanner>(StreamList, const EncodingContainer&)>
          layoutPlannerFactory = nullptr,
      const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max(),
      const bool verifyContent = true);

  static std::vector<VectorPtr> generateBatches(
      const std::shared_ptr<const Type>& type,
      size_t batchCount,
      size_t size,
      uint32_t seed,
      memory::MemoryPool& pool);

  static std::vector<VectorPtr> generateBatches(VectorPtr batch);

  static std::function<std::unique_ptr<DWRFFlushPolicy>()>
  simpleFlushPolicyFactory(bool flushPerBatch) {
    return [flushPerBatch]() {
      return std::make_unique<LambdaFlushPolicy>(
          [flushPerBatch]() { return flushPerBatch; });
    };
  }
};
} // namespace facebook::velox::dwrf
