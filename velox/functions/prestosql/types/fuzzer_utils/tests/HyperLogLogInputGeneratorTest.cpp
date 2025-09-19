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

#include "velox/functions/prestosql/types/fuzzer_utils/HyperLogLogInputGenerator.h"

#include <gtest/gtest.h>

#include "velox/common/hyperloglog/DenseHll.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/Variant.h"

namespace facebook::velox::fuzzer::test {

class HyperLogLogInputGeneratorTest : public functions::test::FunctionBaseTest {
};

TEST_F(HyperLogLogInputGeneratorTest, generate) {
  HyperLogLogInputGenerator generator(123, 0.1, pool());

  size_t numTrials = 100;
  for (size_t i = 0; i < numTrials; ++i) {
    variant generated = generator.generate();

    if (generated.isNull()) {
      continue;
    }

    generated.checkIsKind(TypeKind::VARBINARY);
    const auto& value = generated.value<TypeKind::VARBINARY>();
    HashStringAllocator allocator{pool_.get()};

    if (common::hll::SparseHlls::canDeserialize(value.data())) {
      common::hll::SparseHll<> hll(value.data(), &allocator);
      hll.cardinality();
    } else if (common::hll::DenseHlls::canDeserialize(value.data())) {
      common::hll::DenseHll<> hll(value.data(), &allocator);
      hll.cardinality();
    } else {
      VELOX_FAIL("Invalid HLL value");
    }
  }
}

} // namespace facebook::velox::fuzzer::test
