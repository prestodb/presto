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

#include "presto_cpp/main/functions/theta_sketch/ThetaSketchRegistration.h"

#include "DataSketches/theta_sketch.hpp"

#include "velox/velox/functions/Macros.h"
#include "velox/velox/functions/Registerer.h"

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
    result.copy_from(
        std::make_tuple(
            compactSketch.get_estimate(),
            compactSketch.get_theta(),
            compactSketch.get_upper_bound(1),
            compactSketch.get_lower_bound(1),
            compactSketch.get_num_retained()));
  }
};
} // namespace

void registerThetaSketchFunctions(const std::string& prefix) {
  velox::
      registerFunction<ThetaSketchEstimateFunction, double, velox::Varbinary>(
          {prefix + "sketch_theta_estimate"});
  velox::registerFunction<
      ThetaSketchSummaryFunction,
      velox::Row<double, double, double, double, int32_t>,
      velox::Varbinary>({prefix + "sketch_theta_summary"});
}
} // namespace facebook::presto::functions
