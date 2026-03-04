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

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"

#include "DataSketches/kll_sketch.hpp"

#include "velox/velox/functions/Macros.h"
#include "velox/velox/functions/Registerer.h"

namespace facebook::presto::functions {

namespace {

// Helper to convert input value to sketch-compatible type
template <typename SketchType, typename InputType>
inline SketchType convertToSketchType(const InputType& value) {
  return static_cast<SketchType>(value);
}

// Specialization for VARCHAR -> std::string conversion
template <>
inline std::string convertToSketchType<std::string, velox::StringView>(
    const velox::StringView& value) {
  return std::string(value.data(), static_cast<std::string::size_type>(value.size()));
}

// Generic sketch_kll_rank function with inclusive parameter
template <typename SketchType, typename QuantileType>
struct KllSketchRankFunction {
  template <typename TExec>
  struct udf {
    VELOX_DEFINE_FUNCTION_TYPES(TExec);

    FOLLY_ALWAYS_INLINE void call(
        out_type<double>& result,
        const arg_type<velox::Varbinary>& rawSketch,
        const arg_type<QuantileType>& quantile,
        const arg_type<bool>& inclusive) {

      auto sketch = datasketches::kll_sketch<SketchType>::deserialize(
          rawSketch.data(), rawSketch.size());

      auto sketchQuantile = convertToSketchType<SketchType>(quantile);

      result = sketch.get_rank(sketchQuantile, inclusive);
    }
  };
};

// Generic sketch_kll_rank function with default inclusive=true
template <typename SketchType, typename QuantileType>
struct KllSketchRankDefaultFunction {
  template <typename TExec>
  struct udf {
    VELOX_DEFINE_FUNCTION_TYPES(TExec);

    FOLLY_ALWAYS_INLINE void call(
        out_type<double>& result,
        const arg_type<velox::Varbinary>& rawSketch,
        const arg_type<QuantileType>& quantile) {

      auto sketch = datasketches::kll_sketch<SketchType>::deserialize(
          rawSketch.data(), rawSketch.size());

      auto sketchQuantile = convertToSketchType<SketchType>(quantile);

      result = sketch.get_rank(sketchQuantile, true);
    }
  };
};

// Type aliases for cleaner registration
using KllSketchRankDouble =
    KllSketchRankFunction<double, double>;

using KllSketchRankDoubleDefault =
    KllSketchRankDefaultFunction<double, double>;

using KllSketchRankBigint =
    KllSketchRankFunction<int64_t, int64_t>;

using KllSketchRankBigintDefault =
    KllSketchRankDefaultFunction<int64_t, int64_t>;

using KllSketchRankVarchar =
    KllSketchRankFunction<std::string, velox::Varchar>;

using KllSketchRankVarcharDefault =
    KllSketchRankDefaultFunction<std::string, velox::Varchar>;

using KllSketchRankBoolean =
    KllSketchRankFunction<bool, bool>;

using KllSketchRankBooleanDefault =
    KllSketchRankDefaultFunction<bool, bool>;

} // namespace

void registerKllSketchFunctions(const std::string& prefix) {
  const std::string funcName = prefix + "sketch_kll_rank";

  // Register DOUBLE variants
  velox::registerFunction<KllSketchRankDouble, double, velox::Varbinary, double, bool>({funcName});
  velox::registerFunction<KllSketchRankDoubleDefault, double, velox::Varbinary, double>({funcName});

  // Register BIGINT variants
  velox::registerFunction<KllSketchRankBigint, double, velox::Varbinary, int64_t, bool>({funcName});
  velox::registerFunction<KllSketchRankBigintDefault, double, velox::Varbinary, int64_t>({funcName});

  // Register VARCHAR variants
  velox::registerFunction<KllSketchRankVarchar, double, velox::Varbinary, velox::Varchar, bool>({funcName});
  velox::registerFunction<KllSketchRankVarcharDefault, double, velox::Varbinary, velox::Varchar>({funcName});

  // Register BOOLEAN variants
  velox::registerFunction<KllSketchRankBoolean, double, velox::Varbinary, bool, bool>({funcName});
  velox::registerFunction<KllSketchRankBooleanDefault, double, velox::Varbinary, bool>({funcName});
}

} // namespace facebook::presto::functions
