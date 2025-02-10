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

#include "velox/expression/fuzzer/DecimalArgTypesGeneratorBase.h"
#include "velox/functions/sparksql/DecimalUtil.h"

namespace facebook::velox::functions::sparksql::fuzzer {

class AddSubtractArgTypesGenerator
    : public velox::fuzzer::DecimalArgTypesGeneratorBase {
 public:
  AddSubtractArgTypesGenerator(bool allowPrecisionLoss)
      : allowPrecisionLoss_{allowPrecisionLoss} {
    initialize(2);
  }

 protected:
  std::optional<std::pair<int, int>>
  toReturnType(int p1, int s1, int p2, int s2) override {
    auto precision = std::max(p1 - s1, p2 - s2) + std::max(s1, s2) + 1;
    auto scale = std::max(s1, s2);
    if (allowPrecisionLoss_) {
      return DecimalUtil::adjustPrecisionScale(precision, scale);
    }

    if (precision > 38) {
      precision = 38;
      scale = std::min(38, scale);
    }
    return {{precision, scale}};
  }

 private:
  const bool allowPrecisionLoss_;
};

} // namespace facebook::velox::functions::sparksql::fuzzer
