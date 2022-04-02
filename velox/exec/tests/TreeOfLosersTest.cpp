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
#include "velox/exec/tests/utils/MergeTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class TreeOfLosersTest : public testing::Test, public MergeTestBase {
 protected:
  void SetUp() override {
    seed(1);
  }

  void testBoth(int32_t numValues, int32_t numStreams) {
    TestData testData = makeTestData(numValues, numStreams);
    test<TreeOfLosers<TestingStream>>(testData, true);
    test<MergeArray<TestingStream>>(testData, true);
  }
};

TEST_F(TreeOfLosersTest, merge) {
  testBoth(11, 2);
  testBoth(16, 32);
  testBoth(17, 17);
  testBoth(0, 9);
  testBoth(5000000, 37);
}
