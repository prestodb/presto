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
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/IsNull.h"
#include "velox/functions/prestosql/Cardinality.h"
#include "velox/functions/prestosql/Fail.h"
#include "velox/functions/prestosql/GreatestLeast.h"
#include "velox/functions/prestosql/InPredicate.h"

namespace facebook::velox::functions {

namespace {

void registerFailFunction(const std::vector<std::string>& names) {
  registerFunction<FailFunction, UnknownValue, Varchar>(names);
  registerFunction<FailFunction, UnknownValue, int32_t, Varchar>(names);
  registerFunction<FailFromJsonFunction, UnknownValue, Json>(names);
  registerFunction<FailFromJsonFunction, UnknownValue, int32_t, Json>(names);
}

template <typename T>
void registerGreatestLeastFunction(const std::string& prefix) {
  registerFunction<ParameterBinder<GreatestFunction, T>, T, T, Variadic<T>>(
      {prefix + "greatest"});

  registerFunction<ParameterBinder<LeastFunction, T>, T, T, Variadic<T>>(
      {prefix + "least"});
}

void registerAllGreatestLeastFunctions(const std::string& prefix) {
  registerGreatestLeastFunction<bool>(prefix);
  registerGreatestLeastFunction<int8_t>(prefix);
  registerGreatestLeastFunction<int16_t>(prefix);
  registerGreatestLeastFunction<int32_t>(prefix);
  registerGreatestLeastFunction<int64_t>(prefix);
  registerGreatestLeastFunction<float>(prefix);
  registerGreatestLeastFunction<double>(prefix);
  registerGreatestLeastFunction<Varchar>(prefix);
  registerGreatestLeastFunction<LongDecimal<P1, S1>>(prefix);
  registerGreatestLeastFunction<ShortDecimal<P1, S1>>(prefix);
  registerGreatestLeastFunction<Date>(prefix);
  registerGreatestLeastFunction<Timestamp>(prefix);
}
} // namespace

extern void registerSubscriptFunction(
    const std::string& name,
    bool enableCaching);
extern void registerElementAtFunction(
    const std::string& name,
    bool enableCaching);

// Special form functions don't have any prefix.
void registerAllSpecialFormGeneralFunctions() {
  exec::registerFunctionCallToSpecialForms();
  VELOX_REGISTER_VECTOR_FUNCTION(udf_in, "in");
  registerFunction<
      GenericInPredicateFunction,
      bool,
      Generic<T1>,
      Variadic<Generic<T1>>>({"in"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat_row, "row_constructor");
  registerIsNullFunction("is_null");
}

void registerGeneralFunctions(const std::string& prefix) {
  registerSubscriptFunction(prefix + "subscript", true);
  registerElementAtFunction(prefix + "element_at", true);

  VELOX_REGISTER_VECTOR_FUNCTION(udf_transform, prefix + "transform");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reduce, prefix + "reduce");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_filter, prefix + "filter");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_typeof, prefix + "typeof");

  registerAllGreatestLeastFunctions(prefix);

  registerFunction<CardinalityFunction, int64_t, Array<Any>>(
      {prefix + "cardinality"});
  registerFunction<CardinalityFunction, int64_t, Map<Any, Any>>(
      {prefix + "cardinality"});

  registerFailFunction({prefix + "fail"});

  registerAllSpecialFormGeneralFunctions();
}

} // namespace facebook::velox::functions
