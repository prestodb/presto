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

#include "velox/dwio/parquet/tests/ParquetTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

class SinkTest : public ParquetTestBase {};

TEST_F(SinkTest, close) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
  auto batches = createBatches(rowType, 2, 3);
  auto filePath =
      fs::path(fmt::format("{}/test_close.txt", tempPath_->getPath()));
  auto sink = createSink(filePath.string());
  auto sinkPtr = sink.get();
  auto writer = createWriter(
      std::move(sink),
      [&]() {
        return std::make_unique<LambdaFlushPolicy>(
            kRowsInRowGroup, kBytesInRowGroup, [&]() { return false; });
      },
      rowType);

  for (auto& batch : batches) {
    writer->write(batch);
  }
  writer->flush();

  ASSERT_EQ(fs::file_size(filePath), sinkPtr->size());

  for (auto& batch : batches) {
    writer->write(batch);
  }

  // Close would flush
  writer->close();
  ASSERT_EQ(fs::file_size(filePath), sinkPtr->size());
}

TEST_F(SinkTest, abort) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
  auto batches = createBatches(rowType, 2, 3);
  auto filePath =
      fs::path(fmt::format("{}/test_abort.txt", tempPath_->getPath()));
  auto sink = createSink(filePath.string());
  auto sinkPtr = sink.get();
  auto writer = createWriter(
      std::move(sink),
      [&]() {
        return std::make_unique<LambdaFlushPolicy>(
            kRowsInRowGroup, kBytesInRowGroup, [&]() { return false; });
      },
      rowType);

  for (auto& batch : batches) {
    writer->write(batch);
  }
  writer->flush();

  auto size = sinkPtr->size();
  ASSERT_EQ(size, fs::file_size(filePath));

  for (auto& batch : batches) {
    writer->write(batch);
  }

  // Abort would not flush.
  writer->abort();
  ASSERT_EQ(size, fs::file_size(filePath));
}
