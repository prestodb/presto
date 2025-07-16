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

#include "velox/functions/prestosql/types/fuzzer_utils/SfmSketchInputGenerator.h"
#include <gtest/gtest.h>
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/Variant.h"

namespace facebook::velox::fuzzer::test {
class SfmSketchInputGeneratorTest : public functions::test::FunctionBaseTest {};

TEST_F(SfmSketchInputGeneratorTest, generateTest) {
  SfmSketchInputGenerator gen(123, 0.1, pool());

  size_t numTrials = 100;
  for (size_t i = 0; i < numTrials; ++i) {
    variant generated = gen.generate();
    if (generated.isNull()) {
      continue;
    }
    generated.checkIsKind(TypeKind::VARBINARY);
    const auto& value = generated.value<TypeKind::VARBINARY>();
    HashStringAllocator allocator{pool_.get()};
    auto sketch = SfmSketch::deserialize(value.data(), &allocator);
    // Test that the sketch can be deserialized from the generated data.
    // We don't know anything about the fuzzer generated data.
    sketch.cardinality();
  }
}
} // namespace facebook::velox::fuzzer::test
