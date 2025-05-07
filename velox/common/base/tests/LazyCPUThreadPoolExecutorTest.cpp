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
#include "velox/common/base/LazyCPUThreadPoolExecutor.h"

#include <folly/executors/ThreadPoolExecutor.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook::velox {
namespace {
TEST(LazyCPUThreadPoolExecutorTest, delayedInitialization) {
  const std::string prefix = "MyUnIqUeNaMePrEfIx";

  // All ThreadPoolExecutor instances get placed on a global list at
  // their construction time.  This signal lets us know if the delayed
  // initialization of the underlying CPUThreadPoolExecutor occurred.

  facebook::velox::LazyCPUThreadPoolExecutor lazy(10, prefix);

  // First, make sure we do not already have a ThreadPoolExecutor with
  // a matching prefix before we force its initialization.

  std::vector<std::string> before;
  folly::ThreadPoolExecutor::withAll(
      [&](folly::ThreadPoolExecutor& ex) { before.push_back(ex.getName()); });

  ASSERT_THAT(before, testing::Not(testing::Contains(prefix)));

  // Second, force the initialization of the CPUThreadPoolExecutor

  lazy.add([] { return; });

  // Finally, make sure we have one and only one ThreadPoolExecutor with the
  // matching prefix.

  std::vector<std::string> after;
  folly::ThreadPoolExecutor::withAll(
      [&](folly::ThreadPoolExecutor& ex) { after.push_back(ex.getName()); });

  auto count = std::count(after.begin(), after.end(), prefix);
  ASSERT_THAT(count, testing::Eq(1));
}
} // namespace
} // namespace facebook::velox
