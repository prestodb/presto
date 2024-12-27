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

#include "velox/expression/fuzzer/FuzzerToolkit.h"

#include <gtest/gtest.h>
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::fuzzer::test {
class FuzzerToolKitTest : public testing::Test,
                          public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  bool compareBuffers(const BufferPtr& lhs, const BufferPtr& rhs) {
    if (!lhs && !rhs) {
      return true;
    }
    if ((lhs && !rhs) || (!lhs && rhs) || lhs->size() != rhs->size()) {
      return false;
    }
    return memcmp(lhs->as<char>(), rhs->as<char>(), lhs->size()) == 0;
  }

  bool equals(const InputRowMetadata& lhs, const InputRowMetadata& rhs) {
    return lhs.columnsToWrapInLazy == rhs.columnsToWrapInLazy &&
        lhs.columnsToWrapInCommonDictionary ==
        rhs.columnsToWrapInCommonDictionary;
  }
};

TEST_F(FuzzerToolKitTest, inputRowMetadataRoundTrip) {
  InputRowMetadata metadata;
  metadata.columnsToWrapInLazy = {1, -2, 3, -4, 5};
  metadata.columnsToWrapInCommonDictionary = {1, 2, 3, 4, 5};

  {
    auto path = exec::test::TempFilePath::create();
    metadata.saveToFile(path->getPath().c_str());
    auto copy =
        InputRowMetadata::restoreFromFile(path->getPath().c_str(), pool());
    ASSERT_TRUE(equals(metadata, copy));
  }
}
} // namespace facebook::velox::fuzzer::test
