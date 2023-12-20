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
#include "velox/exec/RoundRobinPartitionFunction.h"
#include <gtest/gtest.h>
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class RoundRobinPartitionFunctionTest : public test::VectorTestBase,
                                        public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(RoundRobinPartitionFunctionTest, basic) {
  exec::RoundRobinPartitionFunction partitionFunction(10);

  auto pool = memory::memoryManager()->addLeafPool();
  test::VectorMaker vm(pool.get());

  auto data = vm.rowVector(ROW({}, {}), 1024);
  std::vector<uint32_t> partitions;
  for (auto i = 0; i < 31; ++i) {
    SCOPED_TRACE(i);
    auto partition = partitionFunction.partition(*data, partitions);
    ASSERT_TRUE(partition.has_value());
    ASSERT_EQ(i % 10, partition.value());
    ASSERT_TRUE(partitions.empty());
  }
}

TEST_F(RoundRobinPartitionFunctionTest, spec) {
  auto roundRobinSpec = std::make_unique<RoundRobinPartitionFunctionSpec>();
  auto serialized = roundRobinSpec->serialize();
  auto copy = RoundRobinPartitionFunctionSpec::deserialize(serialized, pool());
  ASSERT_EQ(roundRobinSpec->toString(), copy->toString());
}
