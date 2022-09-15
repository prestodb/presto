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

#include "velox/exec/HashPartitionFunction.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class HashPartitionFunctionTest : public test::VectorTestBase,
                                  public testing::Test {};

TEST_F(HashPartitionFunctionTest, HashPartitionFunction) {
  const int numRows = 10'000;
  RowVectorPtr vector = makeRowVector(
      {makeFlatVector<int32_t>(numRows, [](auto row) { return row * 100 / 3; }),
       makeFlatVector<int32_t>(numRows, [](auto row) { return row * 128; })});
  RowTypePtr rowType = asRowType(vector->type());

  // The test case the two hash partition functions having the same config.
  {
    std::vector<uint32_t> partitonsWithoutBits(numRows);
    HashPartitionFunction functionWithoutBits(4, rowType, {0});
    functionWithoutBits.partition(*vector, partitonsWithoutBits);
    EXPECT_EQ(4, functionWithoutBits.numPartitions());

    std::vector<uint32_t> partitonsWithBits(numRows);
    HashPartitionFunction functionWithBits(HashBitRange{0, 2}, rowType, {0});
    functionWithBits.partition(*vector, partitonsWithBits);
    EXPECT_EQ(partitonsWithoutBits, partitonsWithBits);
    EXPECT_EQ(4, functionWithBits.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitonsWithoutBits(numRows);
    HashPartitionFunction functionWithoutBits(4, rowType, {0});
    functionWithoutBits.partition(*vector, partitonsWithoutBits);
    EXPECT_EQ(4, functionWithoutBits.numPartitions());

    std::vector<uint32_t> partitonsWithBits(numRows);
    HashPartitionFunction functionWithBits(HashBitRange{0, 3}, rowType, {0});
    functionWithBits.partition(*vector, partitonsWithBits);
    EXPECT_NE(partitonsWithoutBits, partitonsWithBits);
    EXPECT_EQ(8, functionWithBits.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitonsWithoutBits(numRows);
    HashPartitionFunction functionWithoutBits(4, rowType, {0});
    functionWithoutBits.partition(*vector, partitonsWithoutBits);
    EXPECT_EQ(4, functionWithoutBits.numPartitions());

    std::vector<uint32_t> partitonsWithBits(numRows);
    HashPartitionFunction functionWithBits(HashBitRange{29, 31}, rowType, {0});
    functionWithBits.partition(*vector, partitonsWithBits);
    EXPECT_NE(partitonsWithoutBits, partitonsWithBits);
    EXPECT_EQ(4, functionWithBits.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitonsWithBits1(numRows);
    HashPartitionFunction functionWithBits1(HashBitRange{40, 42}, rowType, {0});
    functionWithBits1.partition(*vector, partitonsWithBits1);
    EXPECT_EQ(4, functionWithBits1.numPartitions());

    std::vector<uint32_t> partitonsWithBits2(numRows);
    HashPartitionFunction functionWithBits2(HashBitRange{29, 31}, rowType, {0});
    functionWithBits2.partition(*vector, partitonsWithBits2);
    EXPECT_NE(partitonsWithBits1, partitonsWithBits2);
    EXPECT_EQ(4, functionWithBits2.numPartitions());
  }

  // The test case the two hash partition functions with different configs.
  {
    std::vector<uint32_t> partitonsWithBits1(numRows);
    HashPartitionFunction functionWithBits1(HashBitRange{20, 31}, rowType, {0});
    functionWithBits1.partition(*vector, partitonsWithBits1);
    EXPECT_EQ(1 << 11, functionWithBits1.numPartitions());

    std::vector<uint32_t> partitonsWithBits2(numRows);
    HashPartitionFunction functionWithBits2(HashBitRange{29, 31}, rowType, {0});
    functionWithBits2.partition(*vector, partitonsWithBits2);
    EXPECT_NE(partitonsWithBits1, partitonsWithBits2);
    EXPECT_EQ(4, functionWithBits2.numPartitions());
  }

  // The test case the two hash partition functions having the same config.
  {
    std::vector<uint32_t> partitonsWithBits1(numRows);
    HashPartitionFunction functionWithBits1(HashBitRange{29, 31}, rowType, {0});
    functionWithBits1.partition(*vector, partitonsWithBits1);
    EXPECT_EQ(4, functionWithBits1.numPartitions());

    std::vector<uint32_t> partitonsWithBits2(numRows);
    HashPartitionFunction functionWithBits2(HashBitRange{29, 31}, rowType, {0});
    functionWithBits2.partition(*vector, partitonsWithBits2);
    EXPECT_EQ(partitonsWithBits1, partitonsWithBits2);
    EXPECT_EQ(4, functionWithBits2.numPartitions());
  }
}
