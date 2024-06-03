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

class MapRemoveNullValuesTest : public test::FunctionBaseTest {};

TEST_F(MapRemoveNullValuesTest, general) {
  // bigint key
  auto data = makeRowVector({makeMapVectorFromJson<int64_t, int32_t>({
      "{1:4, 2:5, 3:6}",
      "{-1:null, -2:5, -3:null}",
      "{}",
      "null",
  })});

  auto result = evaluate("map_remove_null_values(c0)", data);

  auto expected = makeMapVectorFromJson<int64_t, int32_t>(
      {"{1:4, 2:5, 3:6}", "{-2:5}", "{}", "null"});

  assertEqualVectors(expected, result);

  // string key
  data = makeRowVector({makeMapVectorFromJson<std::string, std::string>({
      "{\"ad\":null, \"bc\":null, \"cd\":null}",
  })});

  result = evaluate("map_remove_null_values(c0)", data);

  expected = makeMapVectorFromJson<std::string, std::string>({"{}"});

  assertEqualVectors(expected, result);

  // empty input map
  data =
      makeRowVector({makeMapVectorFromJson<std::string, std::string>({"{}"})});
  expected = makeMapVectorFromJson<std::string, std::string>({"{}"});
  assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::functions
