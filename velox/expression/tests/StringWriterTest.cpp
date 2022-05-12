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

#include "velox/expression/StringWriter.h"
#include "folly/Range.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace facebook::velox::expressions::test {

class StringWriterTest : public functions::test::FunctionBaseTest {};

TEST_F(StringWriterTest, append) {
  auto vector = makeFlatVector<StringView>(2);
  auto writer = exec::StringWriter<>(vector.get(), 0);
  writer.append("1 "_sv);
  writer.append(std::string_view("2 "));
  writer.append("3 "_sv);
  writer.append(std::string("4 "));
  writer.append(folly::StringPiece("5 "));

  writer.finalize();

  ASSERT_EQ(vector->valueAt(0), StringView("1 2 3 4 5 "));
}

TEST_F(StringWriterTest, plusOperator) {
  auto vector = makeFlatVector<StringView>(1);
  auto writer = exec::StringWriter<>(vector.get(), 0);
  writer += "1 "_sv;
  writer += "2 ";
  writer += std::string_view("3 ");
  writer += std::string("4 ");
  writer += folly::StringPiece("5 ");

  writer.finalize();

  ASSERT_EQ(vector->valueAt(0), "1 2 3 4 5 "_sv);
}

TEST_F(StringWriterTest, copyFromStringView) {
  auto vector = makeFlatVector<StringView>(1);
  auto writer = exec::StringWriter<>(vector.get(), 0);
  writer.copy_from("1 2 3 4 5 "_sv);
  writer.finalize();

  ASSERT_EQ(vector->valueAt(0), "1 2 3 4 5 "_sv);
}

TEST_F(StringWriterTest, copyFromStdString) {
  auto vector = makeFlatVector<StringView>(1);
  auto writer = exec::StringWriter<>(vector.get(), 0);
  writer.copy_from(std::string("1 2 3 4 5 "));
  writer.finalize();

  ASSERT_EQ(vector->valueAt(0), "1 2 3 4 5 "_sv);
}

TEST_F(StringWriterTest, copyFromCString) {
  auto vector = makeFlatVector<StringView>(4);
  auto writer = exec::StringWriter<>(vector.get(), 0);
  writer.copy_from("1 2 3 4 5 ");
  writer.finalize();

  ASSERT_EQ(vector->valueAt(0), "1 2 3 4 5 "_sv);
}
} // namespace facebook::velox::expressions::test
