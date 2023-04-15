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
#include <stdio.h>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "velox/type/Variant.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/VariantToVector.h"

using namespace facebook::velox;

class VariantToVectorTest : public testing::Test {
  std::shared_ptr<memory::MemoryPool> pool_{memory::addDefaultLeafMemoryPool()};

 public:
  template <TypeKind KIND>
  void testCreateVector(
      const TypePtr& type,
      const std::vector<variant>& inputArray) {
    using TCpp = typename TypeTraits<KIND>::NativeType;

    auto varArray = variant::array(inputArray);
    auto arrayVector =
        core::variantArrayToVector(type, varArray.array(), pool_.get());
    ASSERT_TRUE(arrayVector != nullptr);
    ASSERT_EQ(1, arrayVector->size());

    auto elements = arrayVector->elements()->as<FlatVector<TCpp>>();
    if (!inputArray.empty()) {
      ASSERT_TRUE(elements != nullptr);
    }

    for (size_t i = 0; i < inputArray.size(); i++) {
      auto& var = inputArray[i];
      if (var.isNull()) {
        ASSERT_TRUE(elements->isNullAt(i));
      } else if constexpr (std::is_same_v<TCpp, StringView>) {
        ASSERT_EQ(
            var.template value<KIND>(), std::string(elements->valueAt(i)));
      } else {
        ASSERT_EQ(var.template value<KIND>(), elements->valueAt(i));
      }
    }
  }
};

TEST_F(VariantToVectorTest, bigint) {
  testCreateVector<TypeKind::BIGINT>(
      ARRAY(BIGINT()),
      {
          variant::create<TypeKind::BIGINT>(4432),
          variant::null(TypeKind::BIGINT),
          variant::create<TypeKind::BIGINT>(-123456789),
      });
}

TEST_F(VariantToVectorTest, integer) {
  testCreateVector<TypeKind::INTEGER>(
      ARRAY(INTEGER()),
      {
          variant::create<TypeKind::INTEGER>(122133),
          variant::create<TypeKind::INTEGER>(35121),
      });
}

TEST_F(VariantToVectorTest, smallint) {
  testCreateVector<TypeKind::SMALLINT>(
      ARRAY(SMALLINT()),
      {
          variant::create<TypeKind::SMALLINT>(123),
          variant::create<TypeKind::SMALLINT>(-63),
          variant::null(TypeKind::SMALLINT),
      });
}

TEST_F(VariantToVectorTest, tinyint) {
  testCreateVector<TypeKind::TINYINT>(
      ARRAY(TINYINT()),
      {
          variant::create<TypeKind::TINYINT>(-12),
          variant::create<TypeKind::TINYINT>(51),
      });
}

TEST_F(VariantToVectorTest, boolean) {
  testCreateVector<TypeKind::BOOLEAN>(
      ARRAY(BOOLEAN()),
      {
          variant::null(TypeKind::BOOLEAN),
          variant::create<TypeKind::BOOLEAN>(false),
          variant::create<TypeKind::BOOLEAN>(true),
      });
}

TEST_F(VariantToVectorTest, real) {
  testCreateVector<TypeKind::REAL>(
      ARRAY(REAL()),
      {
          variant::create<TypeKind::REAL>(-4.78),
          variant::create<TypeKind::REAL>(123.45),
      });
}

TEST_F(VariantToVectorTest, double) {
  testCreateVector<TypeKind::DOUBLE>(
      ARRAY(DOUBLE()),
      {
          variant::create<TypeKind::DOUBLE>(-99.948),
          variant::create<TypeKind::DOUBLE>(-123.456),
          variant::create<TypeKind::DOUBLE>(78.91),
          variant::null(TypeKind::DOUBLE),
      });
}

TEST_F(VariantToVectorTest, varchar) {
  testCreateVector<TypeKind::VARCHAR>(
      ARRAY(VARCHAR()),
      {
          variant::create<TypeKind::VARCHAR>("hello"),
          variant::create<TypeKind::VARCHAR>("world"),
          variant::create<TypeKind::VARCHAR>(
              "Some longer string that doesn't get inlined..."),
      });
}

TEST_F(VariantToVectorTest, varbinary) {
  testCreateVector<TypeKind::VARBINARY>(
      ARRAY(VARBINARY()),
      {
          variant::create<TypeKind::VARBINARY>("hello"),
          variant::create<TypeKind::VARBINARY>("world"),
          variant::create<TypeKind::VARBINARY>(
              "Some longer string that doesn't get inlined..."),
      });
}

TEST_F(VariantToVectorTest, empty) {
  testCreateVector<TypeKind::BIGINT>(ARRAY(BIGINT()), {});
}
