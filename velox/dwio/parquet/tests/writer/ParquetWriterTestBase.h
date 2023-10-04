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
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

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
      const RowTypePtr& rowType,
      uint64_t numBatches,
      uint64_t vectorSize);

  std::unique_ptr<dwio::common::FileSink> createSink(
      const std::string& filePath);

  std::unique_ptr<Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      std::function<std::unique_ptr<DefaultFlushPolicy>()> flushPolicy);

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
};
} // namespace facebook::velox::parquet
