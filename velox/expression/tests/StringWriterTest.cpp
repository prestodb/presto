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
#include <glog/logging.h>
#include "folly/Range.h"
#include "gtest/gtest.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::expressions::test {
using namespace facebook::velox::test;

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

TEST_F(StringWriterTest, assignment) {
  auto vector = makeFlatVector<StringView>(4);

  auto writer0 = exec::StringWriter<>(vector.get(), 0);
  writer0 = "string0"_sv;
  writer0.finalize();

  auto writer1 = exec::StringWriter<>(vector.get(), 1);
  writer1 = std::string("string1");
  writer1.finalize();

  auto writer2 = exec::StringWriter<>(vector.get(), 2);
  writer2 = std::string_view("string2");
  writer2.finalize();

  auto writer3 = exec::StringWriter<>(vector.get(), 3);
  writer3 = folly::StringPiece("string3");
  writer3.finalize();

  ASSERT_EQ(vector->valueAt(0), "string0"_sv);
  ASSERT_EQ(vector->valueAt(1), "string1"_sv);
  ASSERT_EQ(vector->valueAt(2), "string2"_sv);
  ASSERT_EQ(vector->valueAt(3), "string3"_sv);
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

TEST_F(StringWriterTest, vectorWriter) {
  auto vector = makeFlatVector<StringView>(3);
  exec::VectorWriter<Varchar> writer;
  writer.init(*vector);
  writer.setOffset(0);
  writer.current().copy_from("1 2 3");
  writer.commitNull();

  writer.setOffset(1);
  writer.current().copy_from("4 5 6");
  writer.commit(true);

  writer.setOffset(2);
  writer.current().copy_from("7 8 9");
  writer.commit(false);
  writer.finish();

  auto expected = std::vector<std::optional<std::string>>{
      std::nullopt, "4 5 6", std::nullopt};
  assertEqualVectors(vector, makeNullableFlatVector(expected));
}
} // namespace facebook::velox::expressions::test
