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
#include "velox/exec/Spill.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class SpillTest : public testing::Test,
                  public facebook::velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    mappedMemory_ = memory::MappedMemory::getInstance();
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    filesystems::registerLocalFileSystem();
  }

  memory::MappedMemory* mappedMemory_;
};

TEST_F(SpillTest, spillState) {
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  // We make a state that has 2 partitions, each with its own file
  // list. We write 10 sorted vectors in each partition. The vectors
  // have the ith element = i * 20 + sequence, where sequence is the
  // sequence number of the vector in the partition. When read back,
  // both partitions produce an ascending sequence of integers without
  // gaps.
  SpillState state(
      tempDirectory->path + "/test",
      2,
      1,
      10000, // small target file size. Makes a new file for each batch.
      *pool(),
      *mappedMemory_);

  EXPECT_EQ(2, state.maxPartitions());
  state.setNumPartitions(2);
  for (auto partition = 0; partition < state.maxPartitions(); ++partition) {
    for (auto batch = 0; batch < 10; ++batch) {
      // We add a sorted run in two pieces: 1, 11, 21,,, followed by 100001 ,
      // 100011, 100021   etc. where the last digit is the batch number. Each
      // sorted run has 20000 rows.
      state.appendToPartition(
          partition,
          makeRowVector({makeFlatVector<int64_t>(
              10000, [&](auto row) { return row * 10 + batch; })}));

      state.appendToPartition(
          partition,
          makeRowVector({makeFlatVector<int64_t>(
              10000, [&](auto row) { return row * 10 + batch + 100000; })}));
      // Indicates that the next additions to 'partition' are not sorted with
      // respect to the values added so far.
      state.finishWrite(partition);
    }
  }
  EXPECT_LT(200'000 * sizeof(int64_t), state.spilledBytes());
  for (auto partition = 0; partition < state.maxPartitions(); ++partition) {
    auto merge = state.startMerge(partition, nullptr);
    // We expect 10 * 20000 rows in dense increasing order.
    for (auto i = 0; i < 200000; ++i) {
      auto stream = merge->next();
      ASSERT_NE(nullptr, stream);
      EXPECT_EQ(
          i,
          stream->current()
              .childAt(0)
              ->asUnchecked<FlatVector<int64_t>>()
              ->valueAt(stream->currentIndex()));
      EXPECT_EQ(i, stream->decoded(0).valueAt<int64_t>(stream->currentIndex()));

      stream->pop();
    }
    ASSERT_EQ(nullptr, merge->next());
  }
}
