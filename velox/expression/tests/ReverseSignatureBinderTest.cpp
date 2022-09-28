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
#include "velox/expression/ReverseSignatureBinder.h"

#include <gtest/gtest.h>

namespace facebook::velox {
namespace {

class ReverseSignatureBinderTest : public testing::Test {
 protected:
  void testBindingSuccess(
      const std::shared_ptr<exec::FunctionSignature>& signature,
      const TypePtr& returnType,
      const std::vector<std::pair<std::string, TypePtr>>& expectedBindings) {
    exec::ReverseSignatureBinder binder{*signature, returnType};
    ASSERT_TRUE(binder.tryBind());

    auto actualBindings = binder.bindings();
    ASSERT_EQ(actualBindings.size(), expectedBindings.size());

    for (const auto& pair : expectedBindings) {
      ASSERT_EQ(actualBindings.count(pair.first), 1);
      if (pair.second) {
        ASSERT_TRUE(pair.second->equivalent(*actualBindings[pair.first]));
      } else {
        ASSERT_TRUE(actualBindings[pair.first] == nullptr);
      }
    }
  }

  void testBindingFailure(
      const std::shared_ptr<exec::FunctionSignature>& signature,
      const TypePtr& returnType) {
    exec::ReverseSignatureBinder binder{*signature, returnType};
    ASSERT_FALSE(binder.tryBind());
  }
};

TEST_F(ReverseSignatureBinderTest, concreteSignature) {
  auto signature = exec::FunctionSignatureBuilder()
                       .returnType("array(varchar)")
                       .argumentType("varchar")
                       .argumentType("bigint")
                       .build();

  testBindingSuccess(signature, ARRAY(VARCHAR()), {});
  testBindingFailure(signature, VARCHAR());
}

TEST_F(ReverseSignatureBinderTest, any) {
  auto signature = exec::FunctionSignatureBuilder()
                       .returnType("array(any)")
                       .argumentType("varchar")
                       .argumentType("bigint")
                       .build();

  testBindingSuccess(signature, ARRAY(VARCHAR()), {});
  testBindingFailure(signature, VARCHAR());
}

TEST_F(ReverseSignatureBinderTest, signatureTemplateFullBinding) {
  auto signature = exec::FunctionSignatureBuilder()
                       .typeVariable("K")
                       .typeVariable("V")
                       .returnType("map(K, V)")
                       .argumentType("K")
                       .argumentType("V")
                       .build();

  testBindingSuccess(
      signature, MAP(VARCHAR(), BIGINT()), {{"K", VARCHAR()}, {"V", BIGINT()}});
  testBindingFailure(signature, VARCHAR());
}

TEST_F(ReverseSignatureBinderTest, signatureTemplatePartialBinding) {
  auto signature = exec::FunctionSignatureBuilder()
                       .typeVariable("K")
                       .typeVariable("V")
                       .returnType("array(K)")
                       .argumentType("K")
                       .argumentType("V")
                       .build();

  testBindingSuccess(
      signature, ARRAY(VARCHAR()), {{"K", VARCHAR()}, {"V", nullptr}});
  testBindingFailure(signature, VARCHAR());
}

TEST_F(ReverseSignatureBinderTest, unsupported) {
  // Constraints on the return type is not supported.
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
          .returnType("row(array(decimal(r_precision, r_scale)))")
          .argumentType("decimal(a_precision, a_scale)")
          .argumentType("decimal(b_precision, b_scale)")
          .build();

  testBindingFailure(signature, DECIMAL(13, 6));
}

} // namespace
} // namespace facebook::velox
