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

namespace facebook::velox::test {
template <typename T>
struct TestingAlwaysThrowsFunction {
  template <typename TResult, typename TInput>
  void call(TResult&, const TInput&) {
    VELOX_USER_FAIL();
  }
};

template <typename T>
struct TestingThrowsAtOddFunction {
  void call(bool& out, const int64_t& input) {
    if (input % 2) {
      VELOX_USER_FAIL();
    } else {
      out = 1;
    }
  }
};

// Throw a VeloxException if veloxException_ is true. Throw an std exception
// otherwise.
class TestingAlwaysThrowsVectorFunction : public exec::VectorFunction {
 public:
  static constexpr const char* kVeloxErrorMessage = "Velox Exception: Expected";
  static constexpr const char* kStdErrorMessage = "Std Exception: Expected";

  explicit TestingAlwaysThrowsVectorFunction(bool veloxException)
      : veloxException_{veloxException} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& /* result */) const override {
    if (veloxException_) {
      auto error =
          std::make_exception_ptr(std::invalid_argument(kVeloxErrorMessage));
      context.setErrors(rows, error);
      return;
    }
    throw std::invalid_argument(kStdErrorMessage);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("boolean")
                .argumentType("integer")
                .build()};
  }

 private:
  const bool veloxException_;
};

} // namespace facebook::velox::test
