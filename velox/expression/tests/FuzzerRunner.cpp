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

#include "velox/expression/tests/FuzzerRunner.h"

// static
int FuzzerRunner::run(
    size_t seed,
    const std::unordered_set<std::string>& skipFunctions) {
  runFromGtest(seed, skipFunctions);
  return RUN_ALL_TESTS();
}

// static
void FuzzerRunner::runFromGtest(
    size_t seed,
    const std::unordered_set<std::string>& skipFunctions) {
  auto signatures = facebook::velox::getFunctionSignatures();
  facebook::velox::test::ExpressionFuzzerVerifier(
      signatures, seed, skipFunctions)
      .go();
}
