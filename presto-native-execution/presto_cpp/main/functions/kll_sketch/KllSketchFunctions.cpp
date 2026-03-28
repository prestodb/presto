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

#include "DataSketches/kll_sketch.hpp"

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"
#include "presto_cpp/main/types/KllSketchType.h"
#include "velox/velox/functions/Macros.h"
#include "velox/velox/functions/Registerer.h"

namespace facebook::presto::functions {

namespace {

//---------------------------------------------
// Type mapping: Velox types -> DataSketches types
//---------------------------------------------

template <typename T>
struct SketchTypeMapper {
  using type = T;
};

// VARCHAR in Velox maps to std::string in DataSketches
template <>
struct SketchTypeMapper<velox::Varchar> {
  using type = std::string;
};

template <typename T>
using SketchType = typename SketchTypeMapper<T>::type;

//---------------------------------------------
// Type conversion helpers
//---------------------------------------------

template <typename T>
inline SketchType<T> convertValue(const T& value) {
  return value;
}

// Velox VARCHAR → std::string for DataSketches
inline std::string convertValue(const velox::StringView& value) {
  return std::string(value.data(), value.size());
}

//---------------------------------------------
// Sketch deserialization helper
//---------------------------------------------

template <typename T>
inline datasketches::kll_sketch<SketchType<T>> deserializeSketch(
    const velox::StringView& rawSketch) {
  try {
    return datasketches::kll_sketch<SketchType<T>>::deserialize(
        rawSketch.data(), rawSketch.size());
  } catch (const std::out_of_range& e) {
    // Thrown by DataSketches for insufficient buffer size
    VELOX_USER_FAIL(
        "Invalid KLL sketch data - buffer out of range: {}", e.what());
  } catch (const std::logic_error& e) {
    // Thrown by DataSketches for corrupted data (size mismatch, etc.)
    VELOX_USER_FAIL(
        "Failed to deserialize KLL sketch - corrupted data or logic error: {}",
        e.what());
  } catch (const std::exception& e) {
    // Catch any other unexpected exceptions during deserialization
    VELOX_USER_FAIL("Failed to deserialize KLL sketch: {}", e.what());
  }
}

//---------------------------------------------
// Rank Function (supports both 2 and 3 args)
//---------------------------------------------

template <typename T>
struct KllSketchRankFunction {
  template <typename TExec>
  struct udf {
    VELOX_DEFINE_FUNCTION_TYPES(TExec);

    FOLLY_ALWAYS_INLINE bool callNullable(
        out_type<double>& result,
        const arg_type<velox::Varbinary>* rawSketch,
        const arg_type<T>* quantile,
        const arg_type<bool>* inclusive) {
      if (!rawSketch || !quantile || !inclusive) {
        return false;
      }

      auto sketch = deserializeSketch<T>(*rawSketch);
      auto value = convertValue(*quantile);

      result = sketch.get_rank(value, *inclusive);
      return true;
    }

    FOLLY_ALWAYS_INLINE bool callNullable(
        out_type<double>& result,
        const arg_type<velox::Varbinary>* rawSketch,
        const arg_type<T>* quantile) {
      if (!rawSketch || !quantile) {
        return false;
      }

      auto sketch = deserializeSketch<T>(*rawSketch);
      auto value = convertValue(*quantile);

      result = sketch.get_rank(value, true);
      return true;
    }
  };
};

//---------------------------------------------
// Quantile Function (supports both 2 and 3 args)
//---------------------------------------------

template <typename T>
struct KllSketchQuantileFunction {
  template <typename TExec>
  struct udf {
    VELOX_DEFINE_FUNCTION_TYPES(TExec);

    // Helper to write result (handles VARCHAR differently)
    FOLLY_ALWAYS_INLINE void writeResult(
        out_type<T>& result,
        const SketchType<T>& value) {
      if constexpr (std::is_same_v<T, velox::Varchar>) {
        result.append(value);
      } else {
        result = value;
      }
    }

    FOLLY_ALWAYS_INLINE bool callNullable(
        out_type<T>& result,
        const arg_type<velox::Varbinary>* rawSketch,
        const arg_type<double>* rank,
        const arg_type<bool>* inclusive) {
      if (!rawSketch || !rank || !inclusive) {
        return false;
      }

      auto sketch = deserializeSketch<T>(*rawSketch);

      auto quantile = sketch.get_quantile(*rank, *inclusive);
      writeResult(result, quantile);
      return true;
    }

    FOLLY_ALWAYS_INLINE bool callNullable(
        out_type<T>& result,
        const arg_type<velox::Varbinary>* rawSketch,
        const arg_type<double>* rank) {
      if (!rawSketch || !rank) {
        return false;
      }

      auto sketch = deserializeSketch<T>(*rawSketch);

      auto quantile = sketch.get_quantile(*rank, true);
      writeResult(result, quantile);
      return true;
    }
  };
};

} // namespace

//---------------------------------------------
// Registration Helper
//---------------------------------------------

template <typename T, typename SketchArg>
void registerKllType(
    const std::string& rankFuncName,
    const std::string& quantileFuncName) {
  // Register rank with 3 args (sketch, value, inclusive)
  velox::registerFunction<
      KllSketchRankFunction<T>,
      double,
      SimpleKllSketch<SketchArg>,
      T,
      bool>({rankFuncName});

  // Register rank with 2 args (sketch, value) - defaults inclusive=true
  velox::registerFunction<
      KllSketchRankFunction<T>,
      double,
      SimpleKllSketch<SketchArg>,
      T>({rankFuncName});

  // Register quantile with 3 args (sketch, rank, inclusive)
  velox::registerFunction<
      KllSketchQuantileFunction<T>,
      T,
      SimpleKllSketch<SketchArg>,
      double,
      bool>({quantileFuncName});

  // Register quantile with 2 args (sketch, rank) - defaults inclusive=true
  velox::registerFunction<
      KllSketchQuantileFunction<T>,
      T,
      SimpleKllSketch<SketchArg>,
      double>({quantileFuncName});
}

//---------------------------------------------
// Function Registration
//---------------------------------------------

void registerKllSketchFunctions(const std::string& prefix) {
  const std::string rankFuncName = prefix + "sketch_kll_rank";
  const std::string quantileFuncName = prefix + "sketch_kll_quantile";

  // Register for all supported types
  registerKllType<double, double>(rankFuncName, quantileFuncName);
  registerKllType<int64_t, int64_t>(rankFuncName, quantileFuncName);
  registerKllType<bool, bool>(rankFuncName, quantileFuncName);

  // VARCHAR special case: Velox uses StringView, DataSketches uses std::string
  registerKllType<velox::Varchar, velox::StringView>(
      rankFuncName, quantileFuncName);
}

} // namespace facebook::presto::functions
