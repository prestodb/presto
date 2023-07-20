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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/parquet/tests/ParquetTpchTestBase.h"

class ParquetTpchTest : public ParquetTpchTestBase {
 public:
  ParquetTpchTest() : ParquetTpchTestBase() {}
};

TEST_F(ParquetTpchTest, Q1) {
  assertQuery(1);
}

TEST_F(ParquetTpchTest, Q3) {
  std::vector<uint32_t> sortingKeys{1, 2};
  assertQuery(3, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q5) {
  std::vector<uint32_t> sortingKeys{1};
  assertQuery(5, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q6) {
  assertQuery(6);
}

TEST_F(ParquetTpchTest, Q7) {
  std::vector<uint32_t> sortingKeys{0, 1, 2};
  assertQuery(7, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q8) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(8, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q9) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(9, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q10) {
  std::vector<uint32_t> sortingKeys{2};
  assertQuery(10, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q12) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(12, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q13) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(13, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q14) {
  assertQuery(14);
}

TEST_F(ParquetTpchTest, Q15) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(15, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q16) {
  std::vector<uint32_t> sortingKeys{0, 1, 2, 3};
  assertQuery(16, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q17) {
  assertQuery(17);
}

TEST_F(ParquetTpchTest, Q18) {
  assertQuery(18);
}

TEST_F(ParquetTpchTest, Q19) {
  assertQuery(19);
}

TEST_F(ParquetTpchTest, Q20) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(20, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q21) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(21, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q22) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(22, std::move(sortingKeys));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
