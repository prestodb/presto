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

#include <gtest/gtest.h>
#include <string>
#include "velox/common/base/Fs.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::parquet {

class ParquetWriterTestBase : public testing::Test {
 protected:
  void SetUp() override {
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::defaultMemoryManager().addRootPool("ParquetWriterTest");
    leafPool_ = rootPool_->addLeafChild("SinkTest");
    tempPath_ = exec::test::TempDirectoryPath::create();
  }

  std::vector<RowVectorPtr> createBatches(
      const std::shared_ptr<const Type>& rowType,
      uint64_t batchNum,
      uint64_t capacity) {
    std::vector<RowVectorPtr> batches;
    for (auto i = 0; i < batchNum; ++i) {
      batches.push_back(
          std::static_pointer_cast<RowVector>(test::BatchMaker::createBatch(
              rowType, capacity, *leafPool_, nullptr, 0)));
    }
    return batches;
  }

  std::unique_ptr<dwio::common::FileSink> createSink(
      const std::string& filePath) {
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", filePath), {.pool = rootPool_.get()});
    EXPECT_TRUE(sink->isBuffered());
    EXPECT_TRUE(fs::exists(filePath));
    EXPECT_FALSE(sink->isClosed());
    return sink;
  }

  std::unique_ptr<Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      std::function<std::unique_ptr<DefaultFlushPolicy>()> flushPolicy) {
    facebook::velox::parquet::WriterOptions options;
    options.memoryPool = rootPool_.get();
    options.flushPolicyFactory = flushPolicy;
    return std::make_unique<Writer>(std::move(sink), options);
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
};
} // namespace facebook::velox::parquet
