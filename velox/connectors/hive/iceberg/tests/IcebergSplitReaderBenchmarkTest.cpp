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

#include "velox/connectors/hive/iceberg/tests/IcebergSplitReaderBenchmark.h"
#include <gtest/gtest.h>

namespace facebook::velox::iceberg::reader::test {
namespace {
TEST(IcebergSplitReaderBenchmarkTest, basic) {
  memory::MemoryManager::testingSetInstance({});
  run(1, "BigInt", BIGINT(), 20, 0, 500);
  run(1, "BigInt", BIGINT(), 50, 20, 500);
  run(1, "BigInt", BIGINT(), 100, 20, 500);
  run(1, "BigInt", BIGINT(), 100, 100, 500);
}
} // namespace
} // namespace facebook::velox::iceberg::reader::test
