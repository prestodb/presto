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

class MapNormalizeTest : public test::FunctionBaseTest {};

TEST_F(MapNormalizeTest, basic) {
  auto data = makeRowVector({
      makeMapVectorFromJson<std::string, double>({
          "{\"a\": 1.0, \"b\": 2.0, \"c\": 2.0}",
          "{\"a\": 2.0, \"b\": 2.0, \"c\": null, \"d\": 1.0}",
          "{\"a\": 1.0, \"b\": -1.0, \"c\": null, \"d\": 0.0}",
          "{\"a\": null, \"b\": null}",
          "{}",
      }),
  });

  auto result = evaluate("map_normalize(c0)", data);

  auto expected = makeMapVectorFromJson<std::string, double>({
      "{\"a\": 0.2, \"b\": 0.4, \"c\": 0.4}",
      "{\"a\": 0.4, \"b\": 0.4, \"c\": null, \"d\": 0.2}",
      "{\"a\": Infinity, \"b\": -Infinity, \"c\": null, \"d\": NaN}",
      "{\"a\": null, \"b\": null}",
      "{}",
  });

  assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::functions
