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
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/SfmSketchFunctions.h"
#include "velox/functions/prestosql/types/SfmSketchRegistration.h"

namespace facebook::velox::functions {

void registerSfmSketchFunctions(const std::string& prefix) {
  registerSfmSketchType();

  registerFunction<SfmSketchCardinality, int64_t, SfmSketch>(
      {prefix + "cardinality"});

  registerFunction<NoisyEmptyApproxSetSfm, SfmSketch, Constant<double>>(
      {prefix + "noisy_empty_approx_set_sfm"});
  registerFunction<
      NoisyEmptyApproxSetSfm,
      SfmSketch,
      Constant<double>,
      Constant<int64_t>>({prefix + "noisy_empty_approx_set_sfm"});
  registerFunction<
      NoisyEmptyApproxSetSfm,
      SfmSketch,
      Constant<double>,
      Constant<int64_t>,
      Constant<int64_t>>({prefix + "noisy_empty_approx_set_sfm"});

  registerFunction<mergeSfmSketchArray, SfmSketch, Array<SfmSketch>>(
      {prefix + "merge_sfm"});
}
} // namespace facebook::velox::functions
