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

#include "velox/common/base/ScopedLock.h"

#include <gtest/gtest.h>

namespace facebook::velox {

TEST(ScopedLockTest, basic) {
  int count = 0;
  std::mutex mu;
  {
    ScopedLock sl(&mu);
    sl.addCallback([&]() {
      std::lock_guard<std::mutex> l(mu);
      ++count;
    });
    ++count;
  }
  ASSERT_EQ(count, 2);
}

TEST(ScopedLockTest, multiCallbacks) {
  int count = 0;
  const int numCallbacks = 10;
  std::mutex mu;
  {
    ScopedLock sl(&mu);
    for (int i = 0; i < numCallbacks; ++i) {
      sl.addCallback([&]() {
        std::lock_guard<std::mutex> l(mu);
        ++count;
      });
    }
    ++count;
  }
  ASSERT_EQ(count, 1 + numCallbacks);
}

} // namespace facebook::velox
