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
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"

#include "DataSketches/kll_sketch.hpp"

namespace facebook::presto::functions {

namespace {

/* ---------------- QUANTILE ---------------- */

template <typename T>
struct KllQuantileDoubleInclusive {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank,
      const arg_type<bool>& inclusive) {

    auto k = datasketches::kll_sketch<double>::deserialize(
        sketch.data(), sketch.size());

    result = k.get_quantile(rank, inclusive);
  }
};

template <typename T>
struct KllQuantileDoubleDefault {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank) {

    auto k = datasketches::kll_sketch<double>::deserialize(
        sketch.data(), sketch.size());

    result = k.get_quantile(rank);
  }
};

template <typename T>
struct KllQuantileBigintInclusive {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<int64_t>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank,
      const arg_type<bool>& inclusive) {

    auto k = datasketches::kll_sketch<int64_t>::deserialize(
        sketch.data(), sketch.size());

    result = k.get_quantile(rank, inclusive);
  }
};

template <typename T>
struct KllQuantileBigintDefault {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<int64_t>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank) {

    auto k = datasketches::kll_sketch<int64_t>::deserialize(
        sketch.data(), sketch.size());

    result = k.get_quantile(rank);
  }
};

template <typename T>
struct KllQuantileStringInclusive {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank,
      const arg_type<bool>& inclusive) {

    auto k = datasketches::kll_sketch<std::string>::deserialize(
        sketch.data(), sketch.size());

    auto value = k.get_quantile(rank, inclusive);
    result = value;
  }
};

template <typename T>
struct KllQuantileStringDefault {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank) {

    auto k = datasketches::kll_sketch<std::string>::deserialize(
        sketch.data(), sketch.size());

    result = k.get_quantile(rank);
  }
};

template <typename T>
struct KllQuantileBooleanInclusive {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank,
      const arg_type<bool>& inclusive) {

    auto k = datasketches::kll_sketch<bool>::deserialize(
        sketch.data(), sketch.size());

    result = k.get_quantile(rank, inclusive);
  }
};

template <typename T>
struct KllQuantileBooleanDefault {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<velox::Varbinary>& sketch,
      const arg_type<double>& rank) {

    auto k = datasketches::kll_sketch<bool>::deserialize(
        sketch.data(), sketch.size());

    result = k.get_quantile(rank);
  }
};

///* ---------------- RANK ---------------- */
//
//template <typename T>
//struct KllRankDoubleInclusive {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<double>& quantile,
//      const arg_type<bool>& inclusive) {
//
//    auto k = datasketches::kll_sketch<double>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile), inclusive);
//  }
//};
//
//template <typename T>
//struct KllRankDoubleDefault {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<double>& quantile) {
//
//    auto k = datasketches::kll_sketch<double>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile));
//  }
//};
//
//template <typename T>
//struct KllRankBigintInclusive {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<int64_t>& quantile,
//      const arg_type<bool>& inclusive) {
//
//    auto k = datasketches::kll_sketch<int64_t>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile), inclusive);
//  }
//};
//
//template <typename T>
//struct KllRankBigintDefault {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<int64_t>& quantile) {
//
//    auto k = datasketches::kll_sketch<int64_t>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile));
//  }
//};
//
//template <typename T>
//struct KllRankStringInclusive {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<velox::Varchar>& quantile,
//      const arg_type<bool>& inclusive) {
//
//    auto k = datasketches::kll_sketch<std::string>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile), inclusive);
//  }
//};
//
//template <typename T>
//struct KllRankStringDefault {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<velox::Varchar>& quantile) {
//
//    auto k = datasketches::kll_sketch<std::string>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile));
//  }
//};
//
//template <typename T>
//struct KllRankBooleanInclusive {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<bool>& quantile,
//      const arg_type<bool>& inclusive) {
//
//    auto k = datasketches::kll_sketch<bool>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile), inclusive);
//  }
//};
//
//template <typename T>
//struct KllRankBooleanDefault {
//  VELOX_DEFINE_FUNCTION_TYPES(T);
//
//  FOLLY_ALWAYS_INLINE void call(
//      out_type<double>& result,
//      const arg_type<velox::Varbinary>& sketch,
//      const arg_type<bool>& quantile) {
//
//    auto k = datasketches::kll_sketch<bool>::deserialize(
//        sketch.data(), sketch.size());
//
//    result = k.get_rank(std::string(quantile));
//  }
//};

} // namespace

void registerKllSketchFunctions(const std::string& prefix) {

  /* quantile */
  velox::registerFunction<KllQuantileDoubleInclusive,double,velox::Varbinary,double,bool>({prefix+"sketch_kll_quantile"});
  velox::registerFunction<KllQuantileDoubleDefault,double,velox::Varbinary,double>({prefix+"sketch_kll_quantile"});

  velox::registerFunction<KllQuantileBigintInclusive,int64_t,velox::Varbinary,double,bool>({prefix+"sketch_kll_quantile"});
  velox::registerFunction<KllQuantileBigintDefault,int64_t,velox::Varbinary,double>({prefix+"sketch_kll_quantile"});

  velox::registerFunction<KllQuantileStringInclusive,velox::Varchar,velox::Varbinary,double,bool>({prefix+"sketch_kll_quantile"});
  velox::registerFunction<KllQuantileStringDefault,velox::Varchar,velox::Varbinary,double>({prefix+"sketch_kll_quantile"});

  velox::registerFunction<KllQuantileBooleanInclusive,bool,velox::Varbinary,double,bool>({prefix+"sketch_kll_quantile"});
  velox::registerFunction<KllQuantileBooleanDefault,bool,velox::Varbinary,double>({prefix+"sketch_kll_quantile"});

//  /* rank */
//  velox::registerFunction<KllRankDoubleInclusive,double,velox::Varbinary,double,bool>({prefix+"sketch_kll_rank"});
//  velox::registerFunction<KllRankDoubleDefault,double,velox::Varbinary,double>({prefix+"sketch_kll_rank"});
//
//  velox::registerFunction<KllRankBigintInclusive,double,velox::Varbinary,int64_t,bool>({prefix+"sketch_kll_rank"});
//  velox::registerFunction<KllRankBigintDefault,double,velox::Varbinary,int64_t>({prefix+"sketch_kll_rank"});
//
//  velox::registerFunction<KllRankStringInclusive,double,velox::Varbinary,velox::Varchar,bool>({prefix+"sketch_kll_rank"});
//  velox::registerFunction<KllRankStringDefault,double,velox::Varbinary,velox::Varchar>({prefix+"sketch_kll_rank"});
//
//  velox::registerFunction<KllRankBooleanInclusive,double,velox::Varbinary,bool,bool>({prefix+"sketch_kll_rank"});
//  velox::registerFunction<KllRankBooleanDefault,double,velox::Varbinary,bool>({prefix+"sketch_kll_rank"});
}

}