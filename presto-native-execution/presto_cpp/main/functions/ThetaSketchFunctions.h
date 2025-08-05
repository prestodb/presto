/*
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

#include "DataSketches/theta_sketch.hpp"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::functions {

namespace {

template <typename T>
struct ThetaSketchEstimateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<velox::Varbinary>& in) {
    auto compactSketch =
        datasketches::wrapped_compact_theta_sketch::wrap(in.data(), in.size());
    result = compactSketch.get_estimate();
  }
};

template <typename T>
struct ThetaSketchSummaryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(
      out_type<velox::Row<double, double, double, double, int32_t>>& result,
      const arg_type<velox::Varbinary>& in) {
    auto compactSketch =
        datasketches::wrapped_compact_theta_sketch::wrap(in.data(), in.size());
    double estimate = compactSketch.get_estimate();
    double theta = compactSketch.get_theta();
    double upperBound = compactSketch.get_upper_bound(1);
    double lowerBound = compactSketch.get_lower_bound(1);
    int32_t retainedEntries = compactSketch.get_num_retained();

    result.copy_from(std::make_tuple(
        estimate, theta, upperBound, lowerBound, retainedEntries));
  }
};
} // namespace

void registerThetaSketchFunctions(const std::string& prefix = "") {
  velox::
      registerFunction<ThetaSketchEstimateFunction, double, velox::Varbinary>(
          {prefix + "sketch_theta_estimate"});
  velox::registerFunction<
      ThetaSketchSummaryFunction,
      velox::Row<double, double, double, double, int32_t>,
      velox::Varbinary>({prefix + "sketch_theta_summary"});
}
} // namespace facebook::presto::functions
