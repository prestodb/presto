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

#include "velox/expression/FunctionSignature.h"
#include "velox/vector/fuzzer/Utils.h"

namespace facebook::velox::fuzzer {

/// Generates random, but valid input types for a specified function signature
/// with the return type.
class ArgGenerator {
 public:
  virtual ~ArgGenerator() = default;

  /// Given a signature and a concrete return type returns randomly selected
  /// valid input types. Returns empty vector if no input types can produce the
  /// specified result type.
  virtual std::vector<TypePtr> generateArgs(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      FuzzerGenerator& rng) = 0;
};

} // namespace facebook::velox::fuzzer
