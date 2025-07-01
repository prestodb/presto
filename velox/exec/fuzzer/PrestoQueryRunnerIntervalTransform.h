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

#include "velox/exec/fuzzer/PrestoQueryRunnerIntermediateTypeTransforms.h"

namespace facebook::velox::exec::test {

// This transform converts negative interval values to NULL due to the
// constraints in parse_duration. Although constants and inputs to
// to_milliseconds allow for negatives, parse_duration does not, the try
// will capture this error and NULL the output. The behavior is the same
// in Presto Java and Prestissimo.
class IntervalDayTimeTransform : public IntermediateTypeTransform {
 public:
  IntervalDayTimeTransform()
      : IntermediateTypeTransform(INTERVAL_DAY_TIME(), BIGINT()) {}

  core::ExprPtr projectToTargetType(
      const core::ExprPtr& inputExpr,
      const std::string& columnAlias) const override;

  core::ExprPtr projectToIntermediateType(
      const core::ExprPtr& inputExpr,
      const std::string& columnAlias) const override;
};

} // namespace facebook::velox::exec::test
