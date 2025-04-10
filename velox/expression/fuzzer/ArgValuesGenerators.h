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

class PhoneNumberArgValuesGenerator : public ArgValuesGenerator {
 public:
  explicit PhoneNumberArgValuesGenerator(std::string functionName) {
    functionName_ = std::move(functionName);
  }
  ~PhoneNumberArgValuesGenerator() override = default;

  std::vector<core::TypedExprPtr> generate(
      const CallableSignature& signature,
      const VectorFuzzer::Options& options,
      FuzzerGenerator& rng,
      ExpressionFuzzerState& state) override;

 private:
  std::string functionName_;
};

// This input generator randomly choose a data type that the JSON argument
// represents, and use the JsonInputGenerator and JsonPathGenerator to generate
// the values for this type. When generating the JSON and JSON-path argument, it
// makes the following two assumptions:
// 2. the size of all container types in the JSON argument do not exceed a
// randomly generated maxContainerSize.
// 1. either all map keys in all maps in the JSON argument are VARCHAR or they
// are BIGINT, and all map-key values are choosen from a randomly generated
// candidate set. The size of the candidate map-key set is slightly larger than
// maxContainerSize, so that access into maps by keys has matches and misses.
//
// 10% of times makes a random variation to both the JSON argument and JSON-path
// argument to test invalid cases.
class JsonExtractArgValuesGenerator : public ArgValuesGenerator {
 public:
  ~JsonExtractArgValuesGenerator() override = default;

  std::vector<core::TypedExprPtr> generate(
      const CallableSignature& signature,
      const VectorFuzzer::Options& options,
      FuzzerGenerator& rng,
      ExpressionFuzzerState& state) override;
};

} // namespace facebook::velox::fuzzer
