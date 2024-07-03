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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class RaiseErrorTest : public SparkFunctionBaseTest {
 protected:
  void raiseError(const std::optional<std::string>& message) {
    evaluateOnce<UnknownValue>("raise_error(c0)", message);
  }
};

TEST_F(RaiseErrorTest, basic) {
  VELOX_ASSERT_USER_THROW(raiseError(""), "");
  VELOX_ASSERT_USER_THROW(raiseError("0 > 1 is not true"), "0 > 1 is not true");
  VELOX_ASSERT_USER_THROW(raiseError(std::nullopt), "");
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
