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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions {
namespace {

class MapKeyExistsTest : public test::FunctionBaseTest {};

TEST_F(MapKeyExistsTest, general) {
  // numeric key
  auto data = makeRowVector({makeMapVectorFromJson<int64_t, int32_t>(
      {"{1:4, 10:5, 3:6}", "{-2:5}", "{}"})});

  auto result = evaluate("map_key_exists(c0, 1)", data);
  auto expected = makeFlatVector<bool>({true, false, false});

  assertEqualVectors(expected, result);

  // string key
  data = makeRowVector({makeMapVectorFromJson<std::string, std::string>({
      "{\"ad\":null, \"bc\":null, \"cd\":null}",
  })});

  result = evaluate("map_key_exists(c0, 'bc')", data);
  expected = makeFlatVector<bool>(true);

  assertEqualVectors(expected, result);
}

TEST_F(MapKeyExistsTest, testFloats) {
  auto data = makeRowVector({makeMapVectorFromJson<double, int32_t>({
      "{1:10, NaN:20, 0:null, 4:40, 5:50, 6:60}",
      "{NaN:20, -0:20}",
  })});

  auto result = evaluate("map_key_exists(c0, cast('NaN' as REAL))", data);
  auto expected = makeFlatVector<bool>({true, true});

  result = evaluate("map_key_exists(c0, cast('0' as REAL))", data);
  expected = makeFlatVector<bool>({true, false});

  result = evaluate("map_key_exists(c0, cast('-0' as REAL))", data);
  expected = makeFlatVector<bool>({true, true});

  assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::functions
