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

#include "velox/dwio/parquet/tests/writer/ParquetWriterTestBase.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::parquet {

std::vector<RowVectorPtr> ParquetWriterTestBase::createBatches(
    const RowTypePtr& rowType,
    uint64_t numBatches,
    uint64_t vectorSize) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  VectorFuzzer fuzzer({.vectorSize = vectorSize}, leafPool_.get());
  for (auto i = 0; i < numBatches; ++i) {
    batches.emplace_back(fuzzer.fuzzInputFlatRow(rowType));
  }
  return batches;
}

std::unique_ptr<dwio::common::FileSink> ParquetWriterTestBase::createSink(
    const std::string& filePath) {
  auto sink = dwio::common::FileSink::create(
      fmt::format("file:{}", filePath), {.pool = rootPool_.get()});
  EXPECT_TRUE(sink->isBuffered());
  EXPECT_TRUE(fs::exists(filePath));
  EXPECT_FALSE(sink->isClosed());
  return sink;
}

std::unique_ptr<Writer> ParquetWriterTestBase::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    std::function<std::unique_ptr<DefaultFlushPolicy>()> flushPolicy) {
  facebook::velox::parquet::WriterOptions options;
  options.memoryPool = rootPool_.get();
  options.flushPolicyFactory = flushPolicy;
  return std::make_unique<Writer>(std::move(sink), options);
}
} // namespace facebook::velox::parquet
