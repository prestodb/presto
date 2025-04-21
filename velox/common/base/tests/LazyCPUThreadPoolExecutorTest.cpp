#include "velox/common/base/LazyCPUThreadPoolExecutor.h"

#include <folly/executors/ThreadPoolExecutor.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(LazyCPUThreadPoolExecutor, DelayedInitialization) {
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

  ASSERT_THAT(after, testing::Contains(prefix).Times(1));
}
