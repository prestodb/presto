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
#include "velox/vector/VectorPrinter.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

class VectorPrinterTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

// Sanity check that printVector doesn't fail or crash.
TEST_F(VectorPrinterTest, basic) {
  VectorFuzzer::Options options;
  options.vectorSize = 100;
  options.nullRatio = 0.1;

  VectorFuzzer fuzzer(options, pool());
  SelectivityVector rows(options.vectorSize);
  for (auto i = 0; i < rows.size(); i += 2) {
    rows.setValid(i, true);
  }
  rows.updateBounds();

  for (auto i = 0; i < 50; ++i) {
    auto data = fuzzer.fuzz(fuzzer.randType());
    ASSERT_NO_THROW(printVector(*data));

    ASSERT_NO_THROW(printVector(*data, 0, 1'000));
    ASSERT_NO_THROW(printVector(*data, 34, 10));

    ASSERT_NO_THROW(printVector(*data, rows));
  }
}

TEST_F(VectorPrinterTest, map) {
  auto data = makeMapVector<int64_t, int64_t>({
      {},
      {{1, 10}},
  });
  ASSERT_NO_THROW(printVector(*data));
}
} // namespace facebook::velox::test
