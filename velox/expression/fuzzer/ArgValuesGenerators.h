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

#include "velox/expression/fuzzer/FuzzerToolkit.h"

namespace facebook::velox::fuzzer {

class JsonParseArgValuesGenerator : public ArgValuesGenerator {
 public:
  ~JsonParseArgValuesGenerator() override = default;

  std::vector<core::TypedExprPtr> generate(
      const CallableSignature& signature,
      const VectorFuzzer::Options& options,
      FuzzerGenerator& rng,
      ExpressionFuzzerState& state) override;
};

class StringEscapeArgValuesGenerator : public ArgValuesGenerator {
 public:
  ~StringEscapeArgValuesGenerator() override = default;

  std::vector<core::TypedExprPtr> generate(
      const CallableSignature& signature,
      const VectorFuzzer::Options& options,
      FuzzerGenerator& rng,
      ExpressionFuzzerState& state) override;
};

} // namespace facebook::velox::fuzzer
