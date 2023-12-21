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

#include "velox/common/caching/CacheTTLController.h"

#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/MmapAllocator.h"

using namespace facebook::velox;
using namespace facebook::velox::memory;

namespace facebook::velox::cache {

class CacheTTLControllerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    allocator_ = std::make_shared<MmapAllocator>(
        MmapAllocator::Options{.capacity = 1024L * 1024L});
    cache_ = AsyncDataCache::create(allocator_.get());
  }

  std::shared_ptr<MemoryAllocator> allocator_;
  std::shared_ptr<AsyncDataCache> cache_;
};

TEST_F(CacheTTLControllerTest, addOpenFileInfo) {
  CacheTTLController::create(*cache_);

  EXPECT_TRUE(CacheTTLController::getInstance()->addOpenFileInfo(123L));
  EXPECT_FALSE(CacheTTLController::getInstance()->addOpenFileInfo(123L));

  EXPECT_TRUE(CacheTTLController::getInstance()->addOpenFileInfo(456L));
}

TEST_F(CacheTTLControllerTest, getCacheAgeStats) {
  CacheTTLController::create(*cache_);

  int64_t fileOpenTime = getCurrentTimeSec();
  for (auto i = 0; i < 1000; i++) {
    CacheTTLController::getInstance()->addOpenFileInfo(i, fileOpenTime + i);
  }

  int64_t current = getCurrentTimeSec();
  EXPECT_GE(
      CacheTTLController::getInstance()->getCacheAgeStats().maxAgeSecs,
      current - fileOpenTime);
}
} // namespace facebook::velox::cache
