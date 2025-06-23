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
#include "velox/functions/prestosql/TDigestFunctions.h"
#include "velox/functions/prestosql/types/TDigestRegistration.h"
#include "velox/functions/prestosql/types/TDigestType.h"
namespace facebook::velox::functions {

void registerTDigestFunctions(const std::string& prefix) {
  facebook::velox::registerTDigestType();
  registerFunction<
      ValueAtQuantileFunction,
      double,
      SimpleTDigest<double>,
      double>({prefix + "value_at_quantile"});
  registerFunction<
      ValuesAtQuantilesFunction,
      Array<double>,
      SimpleTDigest<double>,
      Array<double>>({prefix + "values_at_quantiles"});
  registerFunction<
      MergeTDigestFunction,
      SimpleTDigest<double>,
      Array<SimpleTDigest<double>>>({prefix + "merge_tdigest"});
  registerFunction<
      ScaleTDigestFunction,
      SimpleTDigest<double>,
      SimpleTDigest<double>,
      double>({prefix + "scale_tdigest"});
  registerFunction<
      QuantileAtValueFunction,
      double,
      SimpleTDigest<double>,
      double>({prefix + "quantile_at_value"});
  registerFunction<
      QuantilesAtValuesFunction,
      Array<double>,
      SimpleTDigest<double>,
      Array<double>>({prefix + "quantiles_at_values"});
  registerFunction<
      ConstructTDigestFunction,
      SimpleTDigest<double>,
      Array<double>,
      Array<double>,
      double,
      double,
      double,
      double,
      int64_t>({prefix + "construct_tdigest"});
  registerFunction<
      DestructureTDigestFunction,
      Row<Array<double>,
          Array<int32_t>,
          double,
          double,
          double,
          double,
          int64_t>,
      SimpleTDigest<double>>({prefix + "destructure_tdigest"});
  registerFunction<
      TrimmedMeanFunction,
      double,
      SimpleTDigest<double>,
      double,
      double>({prefix + "trimmed_mean"});
}
} // namespace facebook::velox::functions
