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

#include "velox/expression/fuzzer/DecimalArgGeneratorBase.h"
#include "velox/functions/sparksql/DecimalUtil.h"

namespace facebook::velox::functions::sparksql::fuzzer {

class MultiplyArgGenerator : public velox::fuzzer::DecimalArgGeneratorBase {
 public:
  MultiplyArgGenerator() {
    initialize(2);
  }

 protected:
  std::optional<std::pair<int, int>>
  toReturnType(int p1, int s1, int p2, int s2) override {
    return DecimalUtil::adjustPrecisionScale(p1 + p2 + 1, s1 + s2);
  }
};

} // namespace facebook::velox::functions::sparksql::fuzzer
