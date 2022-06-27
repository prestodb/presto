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
#include "velox/type/Variant.h"
#include <gtest/gtest.h>
#include <velox/type/Type.h>

using namespace facebook::velox;

TEST(Variant, arrayInferType) {
  EXPECT_EQ(*ARRAY(UNKNOWN()), *variant(TypeKind::ARRAY).inferType());
  EXPECT_EQ(*ARRAY(UNKNOWN()), *variant::array({}).inferType());
  EXPECT_EQ(
      *ARRAY(BIGINT()),
      *variant::array({variant(TypeKind::BIGINT)}).inferType());
  EXPECT_EQ(
      *ARRAY(VARCHAR()),
      *variant::array({variant(TypeKind::VARCHAR)}).inferType());
  EXPECT_EQ(
      *ARRAY(ARRAY(DOUBLE())),
      *variant::array({variant::array({variant(TypeKind::DOUBLE)})})
           .inferType());
}

TEST(Variant, mapInferType) {
  EXPECT_EQ(*variant::map({{1LL, 1LL}}).inferType(), *MAP(BIGINT(), BIGINT()));
  EXPECT_EQ(*variant::map({}).inferType(), *MAP(UNKNOWN(), UNKNOWN()));

  const variant nullBigint = variant::null(TypeKind::BIGINT);
  const variant nullReal = variant::null(TypeKind::REAL);
  EXPECT_EQ(
      *variant::map({{nullBigint, nullReal}, {1LL, 1.0f}}).inferType(),
      *MAP(BIGINT(), REAL()));
  EXPECT_EQ(
      *variant::map({{nullBigint, 1.0f}, {1LL, nullReal}}).inferType(),
      *MAP(BIGINT(), REAL()));
  EXPECT_EQ(
      *variant::map({{nullBigint, 1.0f}}).inferType(), *MAP(UNKNOWN(), REAL()));
}

struct Foo {};

struct Bar {};

TEST(Variant, opaque) {
  auto foo = std::make_shared<Foo>();
  auto foo2 = std::make_shared<Foo>();
  auto bar = std::make_shared<Bar>();
  {
    variant v = variant::opaque(foo);
    EXPECT_TRUE(v.isSet());
    EXPECT_EQ(TypeKind::OPAQUE, v.kind());
    EXPECT_EQ(foo, v.opaque<Foo>());
    EXPECT_THROW(v.opaque<Bar>(), std::exception);
    EXPECT_EQ(*v.inferType(), *OPAQUE<Foo>());
  }
  {
    EXPECT_EQ(1, foo.use_count());
    variant v = variant::opaque(foo);
    EXPECT_EQ(2, foo.use_count());
    variant vv = v;
    EXPECT_EQ(3, foo.use_count());
    { variant tmp = std::move(vv); }
    EXPECT_EQ(2, foo.use_count());
    v = 0;
    EXPECT_EQ(1, foo.use_count());
  }
  {
    variant v1 = variant::opaque(foo);
    variant vv1 = variant::opaque(foo);
    variant v2 = variant::opaque(foo2);
    variant v3 = variant::opaque(bar);
    variant vint = 123;
    EXPECT_EQ(v1, vv1);
    EXPECT_NE(v1, v2);
    EXPECT_NE(v1, v3);
    EXPECT_NE(v1, vint);
  }
}
