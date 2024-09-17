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

#include "velox/common/base/Pointers.h"

#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;
namespace {
TEST(PointersTest, uniquePtrConvert) {
  class Foo1 {
   public:
    explicit Foo1(int32_t a) : a_(a) {}
    virtual ~Foo1() = default;

    int32_t a() const {
      return a_;
    }

   protected:
    int32_t a_;
  };

  class Foo2 : public Foo1 {
   public:
    Foo2(int32_t a, int32_t b) : Foo1(a), b_(b) {}

    int32_t b() const {
      return b_;
    }

   private:
    int32_t b_;
  };

  Foo2* rawFoo2 = new Foo2(10, 20);
  ASSERT_EQ(rawFoo2->a(), 10);
  ASSERT_EQ(rawFoo2->b(), 20);
  std::unique_ptr<Foo1> foo1(rawFoo2);
  std::unique_ptr<Foo2> foo2;
  castUniquePointer(std::move(foo1), foo2);
  ASSERT_TRUE(foo2 != nullptr);
  ASSERT_EQ(foo2->a(), 10);
  ASSERT_EQ(foo2->b(), 20);

  class Foo3 {
   public:
    explicit Foo3(int32_t c) : c_(c) {}

   private:
    int32_t c_;
  };
  std::unique_ptr<Foo3> foo3;

  ASSERT_ANY_THROW(castUniquePointer(std::move(foo2), foo3));
  ASSERT_TRUE(foo3 == nullptr);
}
} // namespace
