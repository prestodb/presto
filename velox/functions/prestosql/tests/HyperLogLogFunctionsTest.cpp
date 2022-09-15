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
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#define XXH_INLINE_ALL
#include <xxhash.h>

using facebook::velox::common::hll::DenseHll;
using facebook::velox::common::hll::SparseHll;

namespace facebook::velox {
namespace {

template <typename T>
uint64_t hashOne(T value) {
  return XXH64(&value, sizeof(value), 0);
}

class HyperLogLogFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  static std::string serialize(
      int8_t indexBitLength,
      const SparseHll& sparseHll) {
    std::string serialized;
    serialized.resize(sparseHll.serializedSize());
    sparseHll.serialize(indexBitLength, serialized.data());
    return serialized;
  }

  static std::string serialize(DenseHll& denseHll) {
    std::string serialized;
    serialized.resize(denseHll.serializedSize());
    denseHll.serialize(serialized.data());
    return serialized;
  }

  HashStringAllocator allocator_{memory::MappedMemory::getInstance()};
};

TEST_F(HyperLogLogFunctionsTest, cardinalitySparse) {
  const auto cardinality = [&](const std::string& input) {
    return evaluateOnce<int64_t, StringView>(
        "cardinality(c0)", {StringView(input)}, {HYPERLOGLOG()});
  };

  SparseHll sparseHll{&allocator_};
  for (int i = 0; i < 1'000; i++) {
    sparseHll.insertHash(hashOne(i % 17));
  }

  auto serialized = serialize(11, sparseHll);
  EXPECT_EQ(17, cardinality(serialized));
}

TEST_F(HyperLogLogFunctionsTest, cardinalityDense) {
  const auto cardinality = [&](const std::string& input) {
    return evaluateOnce<int64_t, StringView>(
        "cardinality(c0)", {StringView(input)}, {HYPERLOGLOG()});
  };

  DenseHll denseHll{12, &allocator_};
  for (int i = 0; i < 10'000'000; i++) {
    denseHll.insertHash(hashOne(i));
  }

  auto serialized = serialize(denseHll);
  EXPECT_EQ(denseHll.cardinality(), cardinality(serialized));
}

TEST_F(HyperLogLogFunctionsTest, emptyApproxSet) {
  EXPECT_EQ(
      0,
      evaluateOnce<int64_t>(
          "cardinality(empty_approx_set())", makeRowVector(ROW({}), 1)));

  EXPECT_EQ(
      0,
      evaluateOnce<int64_t>(
          "cardinality(empty_approx_set(0.1))", makeRowVector(ROW({}), 1)));
}

} // namespace
} // namespace facebook::velox
