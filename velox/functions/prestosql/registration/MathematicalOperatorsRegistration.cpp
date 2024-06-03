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
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/DecimalFunctions.h"

namespace facebook::velox::functions {

namespace {
void registerMathOperators(const std::string& prefix = "") {
  registerBinaryFloatingPoint<PlusFunction>({prefix + "plus"});
  registerFunction<
      PlusFunction,
      IntervalDayTime,
      IntervalDayTime,
      IntervalDayTime>({prefix + "plus"});
  registerBinaryFloatingPoint<MinusFunction>({prefix + "minus"});
  registerFunction<
      MinusFunction,
      IntervalDayTime,
      IntervalDayTime,
      IntervalDayTime>({prefix + "minus"});
  registerBinaryFloatingPoint<MultiplyFunction>({prefix + "multiply"});
  registerFunction<MultiplyFunction, IntervalDayTime, IntervalDayTime, int64_t>(
      {prefix + "multiply"});
  registerFunction<MultiplyFunction, IntervalDayTime, int64_t, IntervalDayTime>(
      {prefix + "multiply"});
  registerFunction<
      IntervalMultiplyFunction,
      IntervalDayTime,
      IntervalDayTime,
      double>({prefix + "multiply"});
  registerFunction<
      IntervalMultiplyFunction,
      IntervalDayTime,
      double,
      IntervalDayTime>({prefix + "multiply"});
  registerBinaryFloatingPoint<DivideFunction>({prefix + "divide"});
  registerFunction<
      IntervalDivideFunction,
      IntervalDayTime,
      IntervalDayTime,
      double>({prefix + "divide"});
  registerBinaryFloatingPoint<ModulusFunction>({prefix + "mod"});
}

} // namespace

void registerMathematicalOperators(const std::string& prefix = "") {
  registerMathOperators(prefix);

  registerDecimalPlus(prefix);
  registerDecimalMinus(prefix);
  registerDecimalMultiply(prefix);
  registerDecimalDivide(prefix);
  registerDecimalModulus(prefix);
}

} // namespace facebook::velox::functions
