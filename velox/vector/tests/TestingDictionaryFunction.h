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
#pragma once

#include "velox/expression/VectorFunction.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

/// Wraps input in a dictionary that reverses the order of rows and add nulls at
/// the end of the encoding if trailingNulls_ is not 0. By default, no trailing
/// null is added.
class TestingDictionaryFunction : public exec::VectorFunction {
 public:
  TestingDictionaryFunction(vector_size_t trailingNulls = 0)
      : trailingNulls_(trailingNulls) {}

  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto size = rows.size();
    auto indices = makeIndicesInReverse(size, context.pool());

    auto nulls = allocateNulls(size, context.pool());
    if (trailingNulls_) {
      VELOX_CHECK_GT(size, trailingNulls_);
      auto rawNulls = nulls->asMutable<uint64_t>();
      for (auto i = 1; i <= trailingNulls_; ++i) {
        bits::setNull(rawNulls, size - i);
      }
    }

    result = BaseVector::wrapInDictionary(nulls, indices, size, args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }

 private:
  const vector_size_t trailingNulls_;
};

} // namespace facebook::velox::test
