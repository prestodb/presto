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
#include "velox/core/QueryCtx.h"

namespace facebook::velox::core::test {

class QueryCtxTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(QueryCtxTest, withSysRootPool) {
  auto queryCtx = QueryCtx::create(
      nullptr,
      QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      nullptr,
      memory::deprecatedRootPool().shared_from_this());
  auto* queryPool = queryCtx->pool();
  ASSERT_EQ(&memory::deprecatedRootPool(), queryPool);
  ASSERT_NE(queryPool->reclaimer(), nullptr);
  try {
    VELOX_FAIL("Trigger Error");
  } catch (const velox::VeloxRuntimeError& e) {
    VELOX_ASSERT_THROW(
        queryPool->reclaimer()->abort(queryPool, std::current_exception()),
        "SysMemoryReclaimer::abort is not supported");
  }
  ASSERT_EQ(queryPool->reclaimer()->priority(), 0);
  memory::MemoryReclaimer::Stats stats;
  ASSERT_EQ(queryPool->reclaimer()->reclaim(queryPool, 1'000, 1'000, stats), 0);
  uint64_t reclaimableBytes{0};
  ASSERT_FALSE(
      queryPool->reclaimer()->reclaimableBytes(*queryPool, reclaimableBytes));
}
} // namespace facebook::velox::core::test
