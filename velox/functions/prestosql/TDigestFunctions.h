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

} // namespace facebook::velox::functions
