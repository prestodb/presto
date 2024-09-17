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

#pragma once

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::functions::test {

class SliceTestBase : public FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    memory::MemoryManager::testingSetInstance({});
  }

  virtual void testSlice(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      const ArrayVectorPtr& expectedArrayVector) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(parameters));
    ::facebook::velox::test::assertEqualVectors(expectedArrayVector, result);
    EXPECT_FALSE(expectedArrayVector->hasOverlappingRanges());
  }

  void basicTestCases() {
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4, 5}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      testSlice("slice(C0, 1, 4)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{1, 2}});
      testSlice("slice(C0, 1, 4)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4, 5}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{3, 4}});
      testSlice("slice(C0, 3, 2)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{3, 4}});
      testSlice("slice(C0, 3, 3)", {arrayVector}, expectedArrayVector);
    }
    // Negative start index.
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{2, 3, 4}});
      testSlice("slice(C0, -3, 3)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{2, 3, 4}});
      testSlice("slice(C0, -3, 5)", {arrayVector}, expectedArrayVector);
    }
    // Negative length.
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      VELOX_ASSERT_THROW(
          testSlice(
              "slice(C0, 1, -1)",
              {arrayVector, arrayVector, expectedArrayVector},
              expectedArrayVector),
          "The value of length argument of slice() function should not be negative");
    }
    // 0 start index.
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      VELOX_ASSERT_THROW(
          testSlice(
              "slice(C0, 0, 1)",
              {arrayVector, arrayVector, expectedArrayVector},
              expectedArrayVector),
          "SQL array indices start at 1");
    }
    // 0 length.
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      testSlice("slice(C0, 1, 0)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      testSlice("slice(C0, -2, 0)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      testSlice("slice(C0, -2, 0)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      testSlice("slice(C0, -5, 5)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      testSlice("slice(C0, -6, 5)", {arrayVector}, expectedArrayVector);
    }
    {
      auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
      auto expectedArrayVector = makeArrayVector<int64_t>({{}});
      testSlice("slice(C0, -6, 5)", {arrayVector}, expectedArrayVector);
    }
    {
      // The implementation is zero-copy, so floating point number comparison
      // won't have issue here since numerical representation remains the same.
      auto arrayVector = makeArrayVector<double>({{2.3, 2.3, 2.2}});
      auto expectedArrayVector = makeArrayVector<double>({{2.3, 2.2}});
      testSlice("slice(C0, 2, 3)", {arrayVector}, expectedArrayVector);
    }
    // String array.
    {
      auto arrayVector = makeArrayVector<StringView>({{"a", "b", "c", "d"}});
      auto expectedArrayVector = makeArrayVector<StringView>({{"b", "c"}});
      testSlice("slice(C0, 2, 2)", {arrayVector}, expectedArrayVector);
    }
    // Out of bound start index.
    {
      auto arrayVector = makeArrayVector<StringView>({{"a", "b", "c", "d"}});
      auto expectedArrayVector = makeArrayVector<StringView>({{}});
      testSlice("slice(C0, 5, 2)", {arrayVector}, expectedArrayVector);
    }
    // Out of bound length.
    {
      auto arrayVector = makeArrayVector<StringView>({{"a", "b", "c", "d"}});
      auto expectedArrayVector = makeArrayVector<StringView>({{"b", "c", "d"}});
      testSlice("slice(C0, 2, 5)", {arrayVector}, expectedArrayVector);
    }
  }
};

} // namespace facebook::velox::functions::test
