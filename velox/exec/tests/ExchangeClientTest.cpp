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
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Exchange.h"

namespace facebook::velox::exec {

namespace {

TEST(ExchangeClientTest, nonVeloxCreateExchangeSourceException) {
  std::shared_ptr<memory::MemoryPool> rootPool{
      memory::defaultMemoryManager().addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

  ExchangeSource::registerFactory(
      [](const auto& taskId, auto destination, auto queue, auto pool)
          -> std::shared_ptr<ExchangeSource> {
        throw std::runtime_error("Testing error");
      });

  ExchangeClient client(1, pool.get());
  VELOX_ASSERT_THROW(
      client.addRemoteTaskId("task.1.2.3"),
      "Failed to create ExchangeSource: Testing error. Task ID: task.1.2.3.");

  // Test with a very long task ID. Make sure it is truncated.
  VELOX_ASSERT_THROW(
      client.addRemoteTaskId(std::string(1024, 'x')),
      "Failed to create ExchangeSource: Testing error. "
      "Task ID: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.");
}

} // namespace
} // namespace facebook::velox::exec
