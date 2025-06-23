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

#include "velox/functions/Macros.h"
#include "velox/functions/lib/TDigest.h"
#include "velox/functions/prestosql/types/TDigestType.h"

namespace facebook::velox::functions {

template <typename T>
struct ValueAtQuantileFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<SimpleTDigest<double>>& input,
      const arg_type<double>& quantile) {
    VELOX_USER_CHECK(0 <= quantile && quantile <= 1);
    auto digest = TDigest<>::fromSerialized(input.data());
    result = digest.estimateQuantile(quantile);
  }
};

template <typename T>
struct ValuesAtQuantilesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<double>>& result,
      const arg_type<SimpleTDigest<double>>& input,
      const arg_type<Array<double>>& quantiles) {
    auto digest = TDigest<>::fromSerialized(input.data());
    result.resize(quantiles.size());
    for (size_t i = 0; i < quantiles.size(); ++i) {
      VELOX_USER_CHECK(
          quantiles[i].has_value(), "All quantiles should be non-null.");
      double quantile = quantiles[i].value();
      VELOX_USER_CHECK(0 <= quantile && quantile <= 1);
      result[i] = digest.estimateQuantile(quantile);
    }
  }
};

template <typename T>
struct MergeTDigestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<SimpleTDigest<double>>& result,
      const arg_type<Array<SimpleTDigest<double>>>& input) {
    TDigest<> digest;
    std::vector<int16_t> positions;
    bool hasValidInput = false;
    for (auto i = 0; i < input.size(); i++) {
      if (!input[i].has_value()) {
        continue;
      }
      hasValidInput = true;
      const auto& tdigest = input[i].value();
      digest.mergeDeserialized(positions, tdigest.data());
    }
    if (!hasValidInput) {
      return false;
    }
    digest.compress(positions);
    int64_t size = digest.serializedByteSize();
    result.resize(size);
    digest.serialize(result.data());
    return true;
  }
};

template <typename T>
struct ScaleTDigestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<SimpleTDigest<double>>& result,
      const arg_type<SimpleTDigest<double>>& input,
      const arg_type<double>& scaleFactor) {
    VELOX_USER_CHECK(scaleFactor > 0, "Scale factor should be positive.");
    auto digest = TDigest<>::fromSerialized(input.data());
    digest.scale(scaleFactor);
    int64_t size = digest.serializedByteSize();
    result.resize(size);
    digest.serialize(result.data());
  }
};
template <typename T>
struct QuantileAtValueFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<SimpleTDigest<double>>& input,
      const arg_type<double>& value) {
    if (std::isnan(value)) {
      return false;
    }
    auto digest = TDigest<>::fromSerialized(input.data());
    result = digest.getCdf(value);
    return true;
  }
};
template <typename T>
struct QuantilesAtValuesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<double>>& result,
      const arg_type<SimpleTDigest<double>>& input,
      const arg_type<Array<double>>& values) {
    auto digest = TDigest<>::fromSerialized(input.data());
    result.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      VELOX_USER_CHECK(values[i].has_value(), "All values should be non-null.");
      double value = values[i].value();
      result[i] = digest.getCdf(value);
    }
  }
};
template <typename T>
struct ConstructTDigestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<SimpleTDigest<double>>& result,
      const arg_type<Array<double>>& centroidMeans,
      const arg_type<Array<double>>& centroidWeights,
      const arg_type<double>& compression,
      const arg_type<double>& min,
      const arg_type<double>& max,
      const arg_type<double>& sum,
      const arg_type<int64_t>& count) {
    VELOX_USER_CHECK(
        !std::isnan(compression), "Compression factor must not be NaN.");
    VELOX_USER_CHECK_LE(
        compression, 1000, "Compression factor cannot exceed 1000");
    // Ensure compression is at least 10.
    double compressionValue = std::max(compression, 10.0);
    TDigest<> digest;
    digest.setCompression(compressionValue);
    // Copy centroid means and weights
    std::vector<int16_t> positions;
    std::vector<double> means(centroidMeans.size());
    std::vector<double> weights(centroidWeights.size());
    for (size_t i = 0; i < centroidMeans.size(); i++) {
      means[i] = centroidMeans[i].value();
      weights[i] = centroidWeights[i].value();
    }
    // Merge the centroids
    for (size_t i = 0; i < means.size(); i++) {
      digest.add(positions, means[i], weights[i]);
    }
    // Compress and Serialize
    digest.compress(positions);
    int64_t size = digest.serializedByteSize();
    result.resize(size);
    digest.serialize(result.data());
    return true;
  }
};

template <typename T>
struct DestructureTDigestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<
          Row<Array<double>,
              Array<int32_t>,
              double,
              double,
              double,
              double,
              int64_t>>& result,
      const arg_type<SimpleTDigest<double>>& input) {
    // Deserialize the TDigest
    auto digest = TDigest<>::fromSerialized(input.data());
    // Extract the components
    double min = digest.min();
    double max = digest.max();
    double sum = digest.sum();
    double compression = digest.compression();
    int64_t count = 0;
    // Get the centroids from the TDigest
    std::vector<double> means;
    std::vector<int32_t> weights;
    const double* tDigestWeights = digest.weights();
    const double* tDigestMeans = digest.means();
    size_t numCentroids = digest.size();
    for (int i = 0; i < numCentroids; i++) {
      means.push_back(tDigestMeans[i]);
      weights.push_back(static_cast<int32_t>(tDigestWeights[i]));
      count += tDigestWeights[i];
    }
    // Create the result row
    result.copy_from(std::make_tuple(
        std::move(means),
        std::move(weights),
        compression,
        min,
        max,
        sum,
        count));

    return true;
  }
};

template <typename T>
struct TrimmedMeanFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<SimpleTDigest<double>>& input,
      const arg_type<double>& lowerQuantileBound,
      const arg_type<double>& upperQuantileBound) {
    VELOX_USER_CHECK(
        0 <= lowerQuantileBound && lowerQuantileBound <= 1,
        "Lower quantile bound must be between 0 and 1");
    VELOX_USER_CHECK(
        0 <= upperQuantileBound && upperQuantileBound <= 1,
        "Upper quantile bound must be between 0 and 1");
    VELOX_USER_CHECK(
        lowerQuantileBound <= upperQuantileBound,
        "Lower quantile bound must be less than or equal to upper quantile bound");
    // Deserialize the TDigest
    auto digest = TDigest<>::fromSerialized(input.data());
    if (digest.size() == 0) {
      return false;
    }
    result = digest.trimmedMean(lowerQuantileBound, upperQuantileBound);
    if (std::isnan(result)) {
      return false;
    }
    return true;
  }
};
} // namespace facebook::velox::functions
