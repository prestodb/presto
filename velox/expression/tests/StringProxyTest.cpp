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

#include "folly/Range.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace facebook::velox::expressions::test {

class StringProxyTest : public functions::test::FunctionBaseTest {};

TEST_F(StringProxyTest, testAppend) {
  auto vector = makeFlatVector<StringView>(2);
  auto proxy = exec::StringProxy<FlatVector<StringView>, /*reuse input*/ false>(
      vector.get(), 0);
  proxy.append(StringView("1 "));
  proxy.append("2 ");
  proxy.append(std::string_view("3 "));
  proxy.append(std::string("4 "));
  proxy.append(folly::StringPiece("5 "));

  proxy.finalize();

  ASSERT_EQ(vector->valueAt(0), StringView("1 2 3 4 5 "));
}

TEST_F(StringProxyTest, testPlusOperator) {
  auto vector = makeFlatVector<StringView>(1);
  auto proxy = exec::StringProxy<FlatVector<StringView>, /*reuse input*/ false>(
      vector.get(), 0);
  proxy += StringView("1 ");
  proxy += "2 ";
  proxy += std::string_view("3 ");
  proxy += std::string("4 ");
  proxy += folly::StringPiece("5 ");

  proxy.finalize();

  ASSERT_EQ(vector->valueAt(0), StringView("1 2 3 4 5 "));
}

} // namespace facebook::velox::expressions::test
