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

#include "velox/vector/fuzzer/ConstrainedVectorGenerator.h"

#include <gtest/gtest.h>

#include "velox/common/fuzzer/ConstrainedGenerators.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::fuzzer::test {

class ConstrainedVectorGeneratorTest : public testing::Test,
                                       public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  template <TypeKind KIND>
  void testGenerateVectorsPrimitive(
      const TypePtr& type,
      const variant& excludedValue) {
    using T = typename TypeTraits<KIND>::NativeType;
    const uint32_t kSize = 1000;
    AbstractInputGeneratorPtr generator =
        std::make_shared<NotEqualConstrainedGenerator>(
            0,
            type,
            excludedValue,
            std::make_unique<RandomInputGenerator<T>>(0, type, 0.5));
    auto vector =
        ConstrainedVectorGenerator::generateConstant(generator, kSize, pool());
    EXPECT_EQ(vector->size(), kSize);
    EXPECT_EQ(vector->typeKind(), KIND);
    EXPECT_TRUE(vector->isConstantEncoding());
    EXPECT_TRUE(
        vector->isNullAt(0) ||
        vector->as<ConstantVector<T>>()->valueAt(0) != excludedValue);

    vector = ConstrainedVectorGenerator::generateFlat(generator, kSize, pool());
    EXPECT_EQ(vector->size(), kSize);
    EXPECT_EQ(vector->typeKind(), KIND);
    EXPECT_TRUE(vector->isFlatEncoding());
    bool hasNull = false;
    for (auto i = 0; i < kSize; ++i) {
      if (vector->isNullAt(i)) {
        hasNull = true;
      } else {
        EXPECT_NE(vector->as<FlatVector<T>>()->valueAt(i), excludedValue);
      }
    }
    EXPECT_TRUE(hasNull);
  }

  template <TypeKind KIND>
  void testGenerateVectorsComplex(const TypePtr& type) {
    using T = typename TypeTraits<KIND>::ImplType;
    const uint32_t kSize = 1000;
    AbstractInputGeneratorPtr generator =
        std::make_shared<RandomInputGenerator<T>>(0, type, 0.5);
    auto vector =
        ConstrainedVectorGenerator::generateFlat(generator, kSize, pool());
    EXPECT_EQ(vector->size(), kSize);
    EXPECT_EQ(vector->type(), type);
  }
};

TEST_F(ConstrainedVectorGeneratorTest, generateVectors) {
  testGenerateVectorsPrimitive<TypeKind::BIGINT>(BIGINT(), variant(0));
  testGenerateVectorsPrimitive<TypeKind::VARCHAR>(VARCHAR(), variant(""));

  testGenerateVectorsComplex<TypeKind::ARRAY>(
      ARRAY(ROW({MAP(VARCHAR(), BIGINT())})));
  testGenerateVectorsComplex<TypeKind::MAP>(
      MAP(ARRAY(BIGINT()), ROW({VARCHAR()})));
}

} // namespace facebook::velox::fuzzer::test
