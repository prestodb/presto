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

#include "velox/exec/Spiller.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/RowContainerTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::testutil;

class SpillerTest : public exec::test::RowContainerTestBase,
                    public testing::WithParamInterface<int> {
 public:
  static void SetUpTestCase() {
    TestValue::enable();
  }

 protected:
  void testSpill(int32_t spillPct, bool makeError = false) {
    constexpr int32_t kNumRows = 100000;
    std::vector<char*> rows(kNumRows);
    RowVectorPtr batch;
    auto data = makeSpillData(kNumRows, rows, batch);
    std::vector<uint64_t> hashes(kNumRows);
    auto keys = data->keyTypes();
    // Calculate a hash for every key in 'rows'.
    for (auto i = 0; i < keys.size(); ++i) {
      data->hash(
          i, folly::Range<char**>(rows.data(), kNumRows), i > 0, hashes.data());
    }

    // We divide the rows in 4 partitions according to 2 low bits of the hash.
    std::vector<std::vector<int32_t>> partitions(4);
    for (auto i = 0; i < kNumRows; ++i) {
      partitions[hashes[i] & 3].push_back(i);
    }

    // We sort the rows in each partition in key order.
    for (auto& partition : partitions) {
      std::sort(
          partition.begin(),
          partition.end(),
          [&](int32_t leftIndex, int32_t rightIndex) {
            return data->compareRows(rows[leftIndex], rows[rightIndex]) < 0;
          });
    }

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    // We spill 'data' in 4 sorted partitions.
    auto spiller = std::make_unique<Spiller>(
        *data,
        [&](folly::Range<char**> rows) { data->eraseRows(rows); },
        std::static_pointer_cast<const RowType>(batch->type()),
        HashBitRange(0, 2),
        keys.size(),
        makeError ? "/bad/path" : tempDirectory->path,
        2000000,
        *pool_,
        executor());

    // We have a bit range of two bits , so up to 4 spilled partitions.
    EXPECT_EQ(4, spiller->state().maxPartitions());

    RowContainerIterator iter;

    // We spill spillPct% of the data in 10% increments.
    auto initialBytes = data->allocatedBytes();
    auto initialRows = data->numRows();
    for (int32_t pct = 10; pct <= spillPct; pct += 10) {
      try {
        spiller->spill(
            initialRows - (initialRows * pct / 100),
            initialBytes - (initialBytes * pct / 100),
            iter);
        EXPECT_FALSE(makeError);
      } catch (const std::exception& e) {
        if (!makeError) {
          throw;
        }
        return;
      }
    }
    auto unspilledPartitionRows = spiller->finishSpill();
    if (spillPct == 100) {
      EXPECT_TRUE(unspilledPartitionRows.empty());
      EXPECT_EQ(0, data->numRows());
    }
    // We read back the spilled and not spilled data in each of the
    // partitions. We check that the data comes back in key order.
    for (auto partitionIndex = 0; partitionIndex < 4; ++partitionIndex) {
      if (!spiller->isSpilled(partitionIndex)) {
        continue;
      }
      // We make a merge reader that merges the spill files and the rows that
      // are still in the RowContainer.
      auto merge = spiller->startMerge(partitionIndex);

      // We read the spilled data back and check that it matches the sorted
      // order of the partition.
      auto& indices = partitions[partitionIndex];
      for (auto i = 0; i < indices.size(); ++i) {
        auto stream = merge->next();
        if (!stream) {
          FAIL() << "Stream ends after " << i << " entries";
          break;
        }
        EXPECT_TRUE(batch->equalValueAt(
            &stream->current(), indices[i], stream->currentIndex()));
        stream->pop();
      }
    }
  }

  std::unique_ptr<RowContainer> makeSpillData(
      int32_t numRows,
      std::vector<char*>& rows,
      RowVectorPtr& batch) {
    batch = makeDataset(
        ROW({
            {"bool_val", BOOLEAN()},
            {"tiny_val", TINYINT()},
            {"small_val", SMALLINT()},
            {"int_val", INTEGER()},
            {"long_val", BIGINT()},
            {"ordinal", BIGINT()},
            {"float_val", REAL()},
            {"double_val", DOUBLE()},
            {"string_val", VARCHAR()},
            {"array_val", ARRAY(VARCHAR())},
            {"struct_val",
             ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
            {"map_val",
             MAP(VARCHAR(),
                 MAP(BIGINT(),
                     ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))},
        }),
        numRows,
        [](RowVectorPtr /*rows&*/) {});
    const auto& types = batch->type()->as<TypeKind::ROW>().children();
    std::vector<TypePtr> keys;
    keys.insert(keys.begin(), types.begin(), types.begin() + 6);

    std::vector<TypePtr> dependents;
    dependents.insert(dependents.begin(), types.begin() + 6, types.end());
    // Set ordinal so that the sorted order is unambiguous

    auto ordinal = batch->childAt(5)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < numRows; ++i) {
      ordinal->set(i, i);
    }
    // Make non-join build container so that spill runs are sorted. Note
    // that a distinct or group by hash table can have dependents if
    // some keys are known to be unique by themselves. Aggregation
    // spilling will be tested separately.
    auto data = makeRowContainer(keys, dependents, false);
    rows.resize(numRows);
    for (int i = 0; i < numRows; ++i) {
      rows[i] = data->newRow();
    }

    SelectivityVector allRows(numRows);
    for (auto column = 0; column < batch->childrenSize(); ++column) {
      DecodedVector decoded(*batch->childAt(column), allRows);
      for (auto index = 0; index < numRows; ++index) {
        data->store(decoded, index, rows[index], column);
      }
    }

    return data;
  }

  folly::IOThreadPoolExecutor* executor() {
    static std::mutex mutex;
    std::lock_guard<std::mutex> l(mutex);
    if (GetParam() == 0) {
      return nullptr;
    }
    if (executor_ == nullptr) {
      executor_ = std::make_unique<folly::IOThreadPoolExecutor>(GetParam());
    }
    return executor_.get();
  }

  const int numPartitions_ = 4;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
};

TEST_P(SpillerTest, spilFew) {
  testSpill(10);
}

TEST_P(SpillerTest, spilMost) {
  testSpill(60);
}

TEST_P(SpillerTest, spillAll) {
  testSpill(100);
}

TEST_P(SpillerTest, error) {
  testSpill(100, true);
}

#ifndef NDEBUG
/// This test verifies if the spilling partition is incrementally added.
TEST_P(SpillerTest, incrementalSpillRunCheck) {
  if (GetParam() != 8) {
    // Test with spill execution pool size of 8 is sufficient and skip the other
    // test settings.
    GTEST_SKIP();
  }
  std::set<int> spillRunSizeSet;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Spiller::spill",
      std::function<void(const int*)>(
          [&](const int* runSize) { spillRunSizeSet.emplace(*runSize); }));
  testSpill(100);
  // Spill all the data and expect the spill runs has been built up with one
  // partition at a time.
  EXPECT_EQ(numPartitions_, spillRunSizeSet.size());
  EXPECT_EQ(1, *spillRunSizeSet.begin());
  EXPECT_EQ(numPartitions_, *spillRunSizeSet.rbegin());
}
#endif

// Test with different spill executor pool size. If the size is zero, then spill
// write path is executed inline with spiller control code path.
VELOX_INSTANTIATE_TEST_SUITE_P(SpillerTest, SpillerTest, testing::Values(0, 8));
