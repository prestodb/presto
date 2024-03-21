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

#include "velox/dwio/parquet/tests/reader/ParquetReaderBenchmark.h"
#include <gtest/gtest.h>

namespace facebook::velox::parquet::test {
namespace {
TEST(ParquetReaderBenchmarkTest, basic) {
  memory::MemoryManager::testingSetInstance({});
  run(1, "BigInt", BIGINT(), 20, 0, 500, false);
  run(2, "ShortDecimal", DECIMAL(18, 3), 0, 20, 500, false);
  run(3, "LongDecimal", DECIMAL(38, 3), 10, 5, 500, false);
  run(4, "Double", DOUBLE(), 5, 10, 500, false);
  run(5, "Varchar", VARCHAR(), 0, 0, 500, false);
  run(6, "Map", MAP(BIGINT(), BIGINT()), 100, 20, 500, false);
  run(7, "Array", ARRAY(BIGINT()), 100, 0, 500, false);
}
} // namespace
} // namespace facebook::velox::parquet::test
