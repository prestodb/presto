/*
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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include "velox/experimental/codegen/vector_function/StringTypes.h"
namespace facebook::velox::codegen {
class TempAllocatorTest : public testing::Test {};

TEST_F(TempAllocatorTest, construction) {
  TempsAllocator allocator;
  TempStringNullable string1(allocator);
  ASSERT_EQ(string1.has_value(), false);

  TempStringNullable string2(std::in_place, allocator);
  ASSERT_EQ(string2.has_value(), true);
  ASSERT_EQ((*string2).size(), 0);
}

TEST_F(TempAllocatorTest, Assignment) {
  auto checkNull = [](auto& str) { ASSERT_EQ(str.has_value(), false); };
  auto refString = StringView("test-string");
  auto checkValue = [&](auto& str) {
    ASSERT_EQ(str.has_value(), true);
    ASSERT_EQ(
        std::string((*str).data(), (*str).size()),
        std::string(refString.data(), refString.size()));
  };
  TempsAllocator allocator;
  TempStringNullable string1(allocator);
  checkNull(string1);

  // Assignments to input ref
  string1 = InputReferenceStringNullable{InputReferenceString(refString)};
  checkValue(string1);

  string1 = InputReferenceStringNullable{};
  checkNull(string1);

  string1 = TempStringNullable(std::in_place, allocator);
  *string1 = InputReferenceString(refString);
  checkValue(string1);

  string1 = std::nullopt;
  checkNull(string1);

  // Assignments to const ref
  string1 = ConstantStringNullable{ConstantString(refString)};
  checkValue(string1);

  string1 = ConstantStringNullable{};
  checkNull(string1);

  string1 = TempStringNullable(std::in_place, allocator);
  *string1 = ConstantString(refString);
  checkValue(string1);

  TempStringNullable string2(allocator);
  string2 = string1;
  checkValue(string2);

  string1 = std::nullopt;
  string2 = string1;
  checkNull(string1);

  // UDF proxy interface
  string1 = TempStringNullable(std::in_place, allocator);
  (*string1).reserve(refString.size());
  (*string1).resize(refString.size());
  std::memcpy((*string1).data(), refString.data(), refString.size());
  checkValue(string1);
}
} // namespace facebook::velox::codegen
