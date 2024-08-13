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
#include "velox/functions/lib/tests/SliceTestBase.h"
#include "velox/functions/sparksql/Register.h"

namespace facebook::velox::functions::sparksql::test {

namespace {
class SliceTest : public ::facebook::velox::functions::test::SliceTestBase {
 protected:
  static void SetUpTestCase() {
    SliceTestBase::SetUpTestCase();
    sparksql::registerFunctions("");
  }

  void SetUp() override {
    // Parses integer literals as INTEGER, not BIGINT.
    options_.parseIntegerAsBigint = false;
  }
};

TEST_F(SliceTest, basic) {
  basicTestCases();
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
