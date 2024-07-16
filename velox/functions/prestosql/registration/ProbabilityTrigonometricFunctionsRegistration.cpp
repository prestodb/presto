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
#include "velox/functions/prestosql/Probability.h"

// Register Presto Probability, Trigonometric, and Statistical functions.

namespace facebook::velox::functions {

namespace {
void registerProbTrigFunctions(const std::string& prefix) {
  registerFunction<CosFunction, double, double>({prefix + "cos"});
  registerFunction<CoshFunction, double, double>({prefix + "cosh"});
  registerFunction<AcosFunction, double, double>({prefix + "acos"});
  registerFunction<SinFunction, double, double>({prefix + "sin"});
  registerFunction<AsinFunction, double, double>({prefix + "asin"});
  registerFunction<TanFunction, double, double>({prefix + "tan"});
  registerFunction<TanhFunction, double, double>({prefix + "tanh"});
  registerFunction<AtanFunction, double, double>({prefix + "atan"});
  registerFunction<Atan2Function, double, double, double>({prefix + "atan2"});

  registerFunction<BetaCDFFunction, double, double, double, double>(
      {prefix + "beta_cdf"});
  registerFunction<NormalCDFFunction, double, double, double, double>(
      {prefix + "normal_cdf"});
  registerFunction<BinomialCDFFunction, double, int64_t, double, int64_t>(
      {prefix + "binomial_cdf"});
  registerFunction<BinomialCDFFunction, double, int32_t, double, int32_t>(
      {prefix + "binomial_cdf"});
  registerFunction<CauchyCDFFunction, double, double, double, double>(
      {prefix + "cauchy_cdf"});
  registerFunction<ChiSquaredCDFFunction, double, double, double>(
      {prefix + "chi_squared_cdf"});
  registerFunction<FCDFFunction, double, double, double, double>(
      {prefix + "f_cdf"});
  registerFunction<InverseBetaCDFFunction, double, double, double, double>(
      {prefix + "inverse_beta_cdf"});
  registerFunction<InverseNormalCDFFunction, double, double, double, double>(
      {prefix + "inverse_normal_cdf"});
  registerFunction<PoissonCDFFunction, double, double, int64_t>(
      {prefix + "poisson_cdf"});
  registerFunction<PoissonCDFFunction, double, double, int32_t>(
      {prefix + "poisson_cdf"});
  registerFunction<GammaCDFFunction, double, double, double, double>(
      {prefix + "gamma_cdf"});
  registerFunction<LaplaceCDFFunction, double, double, double, double>(
      {prefix + "laplace_cdf"});
  registerFunction<
      WilsonIntervalUpperFunction,
      double,
      int64_t,
      int64_t,
      double>({prefix + "wilson_interval_upper"});
  registerFunction<
      WilsonIntervalLowerFunction,
      double,
      int64_t,
      int64_t,
      double>({prefix + "wilson_interval_lower"});

  registerFunction<WeibullCDFFunction, double, double, double, double>(
      {prefix + "weibull_cdf"});
  registerFunction<InverseWeibullCDFFunction, double, double, double, double>(
      {prefix + "inverse_weibull_cdf"});
  registerFunction<InverseCauchyCDFFunction, double, double, double, double>(
      {prefix + "inverse_cauchy_cdf"});
  registerFunction<InverseLaplaceCDFFunction, double, double, double, double>(
      {prefix + "inverse_laplace_cdf"});
}

} // namespace

void registerProbabilityTrigonometryFunctions(const std::string& prefix = "") {
  registerProbTrigFunctions(prefix);
}

} // namespace facebook::velox::functions
