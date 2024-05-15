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

#include <gtest/gtest.h>
#include "velox/expression/fuzzer/DecimalArgGeneratorBase.h"
#include "velox/expression/fuzzer/tests/ArgGeneratorTestUtils.h"

namespace facebook::velox::fuzzer::test {

class DecimalArgGeneratorTest : public testing::Test {
 protected:
  class UnaryArgGenerator : public DecimalArgGeneratorBase {
   public:
    UnaryArgGenerator() {
      initialize(1);
    }

   protected:
    std::optional<std::pair<int, int>> toReturnType(int p, int s) override {
      auto precision = std::min(38, p + s + 1);
      auto scale = std::min(s + 1, 18);
      return {{precision, scale}};
    }
  };

  class BinaryArgGenerator : public DecimalArgGeneratorBase {
   public:
    BinaryArgGenerator() {
      initialize(2);
    }

   protected:
    std::optional<std::pair<int, int>>
    toReturnType(int p1, int s1, int p2, int s2) override {
      auto s = std::max(s1, s2);
      auto p = std::min(38, std::max(p1 - s1, p2 - s2) + std::max(s1, s2) + 1);
      return {{p, s}};
    }
  };
};

TEST_F(DecimalArgGeneratorTest, unary) {
  auto signature =
      exec::FunctionSignatureBuilder()
          .integerVariable("scale")
          .integerVariable("precision")
          .integerVariable("r_precision", "min(38, precision + scale + 1)")
          .integerVariable("r_scale", "min(scale + 1, 18)")
          .returnType("decimal(r_precision, r_scale)")
          .argumentType("decimal(precision, scale)")
          .build();

  const auto generator = std::make_shared<UnaryArgGenerator>();
  for (auto returnType : {DECIMAL(10, 2), DECIMAL(38, 18)}) {
    assertReturnType(generator, *signature, returnType);
  }
  assertEmptyArgs(generator, *signature, DECIMAL(38, 20));
}

TEST_F(DecimalArgGeneratorTest, binary) {
  auto signature =
      exec::FunctionSignatureBuilder()
          .integerVariable("a_scale")
          .integerVariable("b_scale")
          .integerVariable("a_precision")
          .integerVariable("b_precision")
          .integerVariable(
              "r_precision",
              "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)")
          .integerVariable("r_scale", "max(a_scale, b_scale)")
          .returnType("decimal(r_precision, r_scale)")
          .argumentType("decimal(a_precision, a_scale)")
          .argumentType("decimal(b_precision, b_scale)")
          .build();

  const auto generator = std::make_shared<BinaryArgGenerator>();
  for (auto returnType :
       {DECIMAL(10, 2), DECIMAL(38, 20), DECIMAL(38, 38), DECIMAL(38, 0)}) {
    assertReturnType(generator, *signature, returnType);
  }
}

} // namespace facebook::velox::fuzzer::test
