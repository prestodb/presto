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

#include "velox/type/TypeCoercer.h"
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox {
namespace {

void testCoercion(const TypePtr& fromType, const TypePtr& toType) {
  auto coercion = TypeCoercer::coerceTypeBase(fromType, toType->name());
  ASSERT_TRUE(coercion.has_value());
  VELOX_EXPECT_EQ_TYPES(coercion->type, toType);
}

void testBaseCoercion(const TypePtr& fromType) {
  auto coercion = TypeCoercer::coerceTypeBase(fromType, fromType->kindName());
  ASSERT_TRUE(coercion.has_value());
  VELOX_EXPECT_EQ_TYPES(coercion->type, fromType);
}

void testNoCoercion(const TypePtr& fromType, const TypePtr& toType) {
  auto coercion = TypeCoercer::coerceTypeBase(fromType, toType->name());
  ASSERT_FALSE(coercion.has_value());
}

TEST(TypeCoercerTest, basic) {
  testCoercion(TINYINT(), TINYINT());
  testCoercion(TINYINT(), BIGINT());
  testCoercion(TINYINT(), REAL());

  testNoCoercion(TINYINT(), VARCHAR());
  testNoCoercion(TINYINT(), DATE());

  testBaseCoercion(ARRAY(TINYINT()));
  testNoCoercion(ARRAY(TINYINT()), MAP(INTEGER(), REAL()));
}

} // namespace
} // namespace facebook::velox
