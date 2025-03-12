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

#include "velox/type/TypeUtil.h"

#include <gtest/gtest.h>

namespace facebook::velox::type {
namespace {

TEST(TypeUtilTest, ConcatRowTypes) {
  auto keyType = velox::ROW({"k0", "k1"}, {velox::BIGINT(), velox::INTEGER()});
  auto valueType = velox::ROW(
      {"v0", "v1"}, {velox::VARBINARY(), velox::ARRAY(velox::BIGINT())});
  auto type = concatRowTypes({keyType, valueType});
  auto expected = velox::ROW(
      {"k0", "k1", "v0", "v1"},
      {velox::BIGINT(),
       velox::INTEGER(),
       velox::VARBINARY(),
       velox::ARRAY(velox::BIGINT())});
  EXPECT_EQ(type->toString(), expected->toString());
}

} // namespace
} // namespace facebook::velox::type
