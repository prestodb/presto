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
#include "folly/json.h"
#include "velox/common/serialization/Serializable.h"

using namespace ::facebook::velox;

namespace {
TEST(Serializable, TestSerializableOpts) {
  auto opts = getSerializationOptions();

  // A very large negative number that is out of folly Json integer bound.
  // With opts.double_fallback enabled, it should be correctly handled as double
  // type.
  auto largeNumber = folly::parseJson("-21820650861507478000", opts);
  EXPECT_TRUE(largeNumber.isDouble());
  EXPECT_EQ(largeNumber, -21820650861507478000.0);
}
} // namespace
