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
#pragma once

#include <boost/math/distributions.hpp>
#include <boost/math/distributions/cauchy.hpp>
#include <boost/math/distributions/laplace.hpp>
#include <boost/math/distributions/weibull.hpp>
#include "boost/math/distributions/beta.hpp"
#include "boost/math/distributions/binomial.hpp"
#include "boost/math/distributions/cauchy.hpp"
#include "boost/math/distributions/chi_squared.hpp"
#include "boost/math/distributions/fisher_f.hpp"
#include "boost/math/distributions/gamma.hpp"
#include "boost/math/distributions/poisson.hpp"
#include "boost/math/special_functions/erf.hpp"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions {

namespace {

template <typename T>
struct BetaCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(double& result, double a, double b, double value) {
    constexpr double kInf = std::numeric_limits<double>::infinity();

    VELOX_USER_CHECK_GT(a, 0, "a must be > 0");
    VELOX_USER_CHECK_GT(b, 0, "b must be > 0");
    VELOX_USER_CHECK_GE(value, 0, "value must be in the interval [0, 1]");
    VELOX_USER_CHECK_LE(value, 1, "value must be in the interval [0, 1]");

    if ((a == kInf) || (b == kInf)) {
      result = 0.0;
    } else {
      boost::math::beta_distribution<> dist(a, b);
      result = boost::math::cdf(dist, value);
    }
  }
};

template <typename T>
struct NormalCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Normal cumulative distribution is computed as per the reference at
  // https://mathworld.wolfram.com/NormalDistribution.html.

  FOLLY_ALWAYS_INLINE void
  call(double& result, double m, double sd, double value) {
    VELOX_USER_CHECK_GT(sd, 0, "standardDeviation must be > 0");

    static const double kSqrtOfTwo = sqrt(2);
    result = 0.5 * (1 + erf((value - m) / (sd * kSqrtOfTwo)));
  }
};

template <typename T>
struct BinomialCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TValue>
  FOLLY_ALWAYS_INLINE void
  call(double& result, TValue numOfTrials, double successProb, TValue value) {
    static constexpr TValue kInf = std::numeric_limits<TValue>::max();

    VELOX_USER_CHECK(
        (successProb >= 0) && (successProb <= 1),
        "successProbability must be in the interval [0, 1]");
    VELOX_USER_CHECK_GT(
        numOfTrials, 0, "numberOfTrials must be greater than 0");

    if ((value < 0) || (numOfTrials == kInf)) {
      result = 0.0;
      return;
    }

    if (value == kInf) {
      result = 1.0;
      return;
    }

    boost::math::binomial_distribution<> dist(numOfTrials, successProb);
    result = boost::math::cdf(dist, value);
  }
};

template <typename T>
struct CauchyCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(double& result, double median, double scale, double value) {
    static constexpr double kInf = std::numeric_limits<double>::infinity();
    static constexpr double kDoubleMax = std::numeric_limits<double>::max();

    if (std::isnan(median) || std::isnan(value)) {
      result = std::numeric_limits<double>::quiet_NaN();
    } else if (median == kInf || median == kDoubleMax) {
      result = 0.0;
    } else if (scale == kInf) {
      result = 0.5;
    } else if (value == kInf) {
      result = 1.0;
    } else {
      VELOX_USER_CHECK_GE(scale, 0, "scale must be greater than 0");

      boost::math::cauchy_distribution<> dist(median, scale);
      result = boost::math::cdf(dist, value);
    }
  }
};

template <typename T>
struct GammaCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(double& result, double shape, double scale, double value) {
    static constexpr double kInf = std::numeric_limits<double>::infinity();

    VELOX_USER_CHECK_GE(value, 0, "value must be greater than, or equal to, 0");
    VELOX_USER_CHECK_GT(shape, 0, "shape must be greater than 0");
    VELOX_USER_CHECK_GT(scale, 0, "scale must be greater than 0");

    if (scale == kInf && value == kInf) {
      result = 1.0;
    } else if (shape == kInf || scale == kInf) {
      result = 0.0;
    } else if (value == kInf) {
      result = 1.0;
    } else {
      boost::math::gamma_distribution<> gammaDist(shape, scale);
      result = boost::math::cdf(gammaDist, value);
    }
  }
};

template <typename T>
struct LaplaceCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void
  call(double& result, double location, double scale, double x) {
    if (std::isnan(location) || std::isnan(scale) || std::isnan(x)) {
      result = std::numeric_limits<double>::quiet_NaN();
    } else {
      VELOX_USER_CHECK_GT(scale, 0, "scale must be greater than 0");
      boost::math::laplace_distribution<> laplaceDist(location, scale);
      result = boost::math::cdf(laplaceDist, x);
    }
  }
};

template <typename T>
struct InverseBetaCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double a, double b, double p) {
    static constexpr double kInf = std::numeric_limits<double>::infinity();

    VELOX_USER_CHECK(
        (p >= 0) && (p <= 1) && (p != kInf),
        "p must be in the interval [0, 1]");
    VELOX_USER_CHECK((a > 0) && (a != kInf), "a must be > 0");
    VELOX_USER_CHECK((b > 0) && (b != kInf), "b must be > 0");

    boost::math::beta_distribution<> dist(a, b);
    result = boost::math::quantile(dist, p);
  }
};

template <typename T>
struct ChiSquaredCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double df, double value) {
    VELOX_USER_CHECK_GT(df, 0, "df must be greater than 0");
    VELOX_USER_CHECK_GE(value, 0, "value must non-negative");

    boost::math::chi_squared_distribution<> dist(df);
    result = boost::math::cdf(dist, value);
  }
};

template <typename T>
struct FCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(double& result, double df1, double df2, double value) {
    VELOX_USER_CHECK_GE(value, 0, "value must non-negative");
    VELOX_USER_CHECK_GT(df1, 0, "numerator df must be greater than 0");
    VELOX_USER_CHECK_GT(df2, 0, "denominator df must be greater than 0");

    boost::math::fisher_f_distribution<> dist(df1, df2);
    result = boost::math::cdf(dist, value);
  }
};

template <typename T>
struct PoissonCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TValue>
  FOLLY_ALWAYS_INLINE void call(double& result, double lambda, TValue value) {
    VELOX_USER_CHECK_GE(value, 0, "value must be a non-negative integer");
    VELOX_USER_CHECK_GT(lambda, 0, "lambda must be greater than 0");

    boost::math::poisson_distribution<double> poisson(lambda);
    result = boost::math::cdf(poisson, value);
  }
};

template <typename T>
struct WeibullCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(double& result, double a, double b, double value) {
    static constexpr double kInf = std::numeric_limits<double>::infinity();

    VELOX_USER_CHECK_GT(a, 0, "a must be greater than 0");
    VELOX_USER_CHECK_GT(b, 0, "b must be greater than 0");

    if (std::isnan(value)) {
      result = std::numeric_limits<double>::quiet_NaN();
    } else if (b == kInf) {
      result = 0.0;
    } else if (a == kInf || value == kInf) {
      result = 1.0;
    } else {
      boost::math::weibull_distribution<> dist(a, b);
      result = boost::math::cdf(dist, value);
    }
  }
};

template <typename T>
struct InverseNormalCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double m, double sd, double p) {
    VELOX_USER_CHECK((p >= 0) && (p <= 1), "p must be 0 > p > 1");
    VELOX_USER_CHECK_GT(sd, 0, "standardDeviation must be > 0");

    static const double kSqrtOfTwo = std::sqrt(2);
    result = m + sd * kSqrtOfTwo * boost::math::erf_inv(2 * p - 1);
  }
};

template <typename T>
struct InverseWeibullCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double a, double b, double p) {
    static constexpr double kInf = std::numeric_limits<double>::infinity();

    VELOX_USER_CHECK((p >= 0) && (p <= 1), "p must be in the interval [0, 1]");
    VELOX_USER_CHECK_GT(a, 0, "a must be greater than 0");
    VELOX_USER_CHECK_GT(b, 0, "b must be greater than 0");

    if (b == kInf) {
      result = kInf;
    } else {
      // https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/distribution/WeibullDistribution.html#inverseCumulativeProbability(double)
      result = b * std::pow(-std::log1p(-p), 1.0 / a);
    }
  }
};

template <typename T>
struct InverseCauchyCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void
  call(double& result, double median, double scale, double p) {
    static constexpr double kInf = std::numeric_limits<double>::infinity();
    static constexpr double kDoubleMax = std::numeric_limits<double>::max();
    static constexpr double kNan = std::numeric_limits<double>::quiet_NaN();

    VELOX_USER_CHECK(p >= 0 && p <= 1, "p must be in the interval [0, 1]");
    VELOX_USER_CHECK_GT(scale, 0, "scale must be greater than 0");

    if (p == 1.0) {
      result = kInf;
    } else if (scale == kInf) {
      result = median;
    } else if (std::isnan(median)) {
      result = kNan;
    } else if (median == kInf) {
      result = kInf;
    } else {
      boost::math::cauchy_distribution<> cauchyDist(median, scale);
      result = boost::math::quantile(cauchyDist, p);
    }
  }
};

template <typename T>
struct InverseLaplaceCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void
  call(double& result, double location, double scale, double p) {
    VELOX_USER_CHECK_GT(scale, 0, "scale must be greater than 0");
    VELOX_USER_CHECK(p >= 0 && p <= 1, "p must be in the interval [0, 1]");

    if (std::isnan(location) || std::isinf(location)) {
      result = std::numeric_limits<double>::quiet_NaN();
    } else {
      boost::math::laplace_distribution<> laplaceDist(location, scale);
      result = boost::math::quantile(laplaceDist, p);
    }
  }
};

} // namespace
} // namespace facebook::velox::functions
