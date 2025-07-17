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
#include "velox/functions/prestosql/QDigestFunctions.h"
#include "velox/functions/prestosql/types/QDigestRegistration.h"
#include "velox/functions/prestosql/types/QDigestType.h"

namespace facebook::velox::functions {

void registerQDigestFunctions(const std::string& prefix) {
  facebook::velox::registerQDigestType();

  registerFunction<
      ValueAtQuantileFunction,
      int64_t,
      SimpleQDigest<int64_t>,
      double>({prefix + "value_at_quantile"});

  registerFunction<
      ValueAtQuantileFunction,
      double,
      SimpleQDigest<double>,
      double>({prefix + "value_at_quantile"});

  registerFunction<
      ValueAtQuantileFunction,
      float,
      SimpleQDigest<float>,
      double>({prefix + "value_at_quantile"});

  registerFunction<
      ValuesAtQuantilesFunction,
      Array<int64_t>,
      SimpleQDigest<int64_t>,
      Array<double>>({prefix + "values_at_quantiles"});

  registerFunction<
      ValuesAtQuantilesFunction,
      Array<double>,
      SimpleQDigest<double>,
      Array<double>>({prefix + "values_at_quantiles"});

  registerFunction<
      ValuesAtQuantilesFunction,
      Array<float>,
      SimpleQDigest<float>,
      Array<double>>({prefix + "values_at_quantiles"});

  registerFunction<
      QuantileAtValueFunction,
      double,
      SimpleQDigest<double>,
      double>({prefix + "quantile_at_value"});

  registerFunction<
      QuantileAtValueFunction,
      double,
      SimpleQDigest<int64_t>,
      int64_t>({prefix + "quantile_at_value"});

  registerFunction<
      QuantileAtValueFunction,
      double,
      SimpleQDigest<float>,
      float>({prefix + "quantile_at_value"});

  registerFunction<
      ScaleQDigestDoubleFunction,
      SimpleQDigest<double>,
      SimpleQDigest<double>,
      double>({prefix + "scale_qdigest"});

  registerFunction<
      ScaleQDigestBigintFunction,
      SimpleQDigest<int64_t>,
      SimpleQDigest<int64_t>,
      double>({prefix + "scale_qdigest"});

  registerFunction<
      ScaleQDigestRealFunction,
      SimpleQDigest<float>,
      SimpleQDigest<float>,
      double>({prefix + "scale_qdigest"});
}

} // namespace facebook::velox::functions
