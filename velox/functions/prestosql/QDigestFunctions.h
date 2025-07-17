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
#include "velox/functions/lib/QuantileDigest.h"
#include "velox/functions/prestosql/types/QDigestType.h"

namespace facebook::velox::functions {

template <typename TExec>
struct ValueAtQuantileFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<SimpleQDigest<double>>& input,
      const arg_type<double>& quantile) {
    callImpl<double>(result, input, quantile);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<int64_t>& result,
      const arg_type<SimpleQDigest<int64_t>>& input,
      const arg_type<double>& quantile) {
    callImpl<int64_t>(result, input, quantile);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<float>& result,
      const arg_type<SimpleQDigest<float>>& input,
      const arg_type<double>& quantile) {
    callImpl<float>(result, input, quantile);
  }

 private:
  template <typename T>
  FOLLY_ALWAYS_INLINE void callImpl(
      out_type<T>& result,
      const arg_type<SimpleQDigest<T>>& input,
      const arg_type<double>& quantile) {
    VELOX_USER_CHECK(
        quantile >= 0.0 && quantile <= 1.0,
        "Quantile must be between 0 and 1, got: {}",
        quantile);

    std::allocator<T> allocator;
    qdigest::QuantileDigest<T, std::allocator<T>> digest(
        allocator, input.data());
    result = digest.estimateQuantile(quantile);
  }
};

template <typename TExec>
struct ValuesAtQuantilesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<double>>& result,
      const arg_type<SimpleQDigest<double>>& input,
      const arg_type<Array<double>>& quantiles) {
    callImpl<double>(result, input, quantiles);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<int64_t>>& result,
      const arg_type<SimpleQDigest<int64_t>>& input,
      const arg_type<Array<double>>& quantiles) {
    callImpl<int64_t>(result, input, quantiles);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<float>>& result,
      const arg_type<SimpleQDigest<float>>& input,
      const arg_type<Array<double>>& quantiles) {
    callImpl<float>(result, input, quantiles);
  }

 private:
  template <typename T>
  FOLLY_ALWAYS_INLINE void callImpl(
      out_type<Array<T>>& result,
      const arg_type<SimpleQDigest<T>>& input,
      const arg_type<Array<double>>& quantiles) {
    std::allocator<T> allocator;
    qdigest::QuantileDigest<T, std::allocator<T>> digest(
        allocator, input.data());

    result.reserve(quantiles.size());
    for (size_t i = 0; i < quantiles.size(); ++i) {
      VELOX_USER_CHECK(
          quantiles[i].has_value(), "All quantiles should be non - null.");
      double quantile = quantiles[i].value();
      VELOX_USER_CHECK(
          quantile >= 0.0 && quantile <= 1.0,
          "Quantile must be between 0 and 1, got: {}",
          quantile);
      result.push_back(digest.estimateQuantile(quantile));
    }
  }
};

template <typename TExec>
struct QuantileAtValueFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<SimpleQDigest<double>>& input,
      const arg_type<double>& value) {
    return callImpl<double>(result, input, value);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<SimpleQDigest<int64_t>>& input,
      const arg_type<int64_t>& value) {
    return callImpl<int64_t>(result, input, value);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<SimpleQDigest<float>>& input,
      const arg_type<float>& value) {
    return callImpl<float>(result, input, value);
  }

 private:
  template <typename T>
  FOLLY_ALWAYS_INLINE bool callImpl(
      out_type<double>& result,
      const arg_type<SimpleQDigest<T>>& input,
      const arg_type<T>& value) {
    std::allocator<T> allocator;
    qdigest::QuantileDigest<T, std::allocator<T>> digest(
        allocator, input.data());

    auto quantile = digest.quantileAtValue(value);
    if (quantile.has_value()) {
      result = quantile.value();
      return true;
    }
    return false; // Return false for null result when value is out of range
  }
};

struct ScaleQDigestBase {
 protected:
  template <typename T, typename ResultT, typename InputT>
  FOLLY_ALWAYS_INLINE void
  callImpl(ResultT& result, const InputT& input, double scaleFactor) {
    std::allocator<T> allocator;
    qdigest::QuantileDigest<T, std::allocator<T>> digest(
        allocator, input.data());
    digest.scale(scaleFactor);
    int64_t size = digest.serializedByteSize();
    result.resize(size);
    digest.serialize(result.data());
  }
};

template <typename TExec>
struct ScaleQDigestDoubleFunction : public ScaleQDigestBase {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<SimpleQDigest<double>>& result,
      const arg_type<SimpleQDigest<double>>& input,
      const arg_type<double>& scaleFactor) {
    this->template callImpl<double>(result, input, scaleFactor);
  }
};

template <typename TExec>
struct ScaleQDigestBigintFunction : public ScaleQDigestBase {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<SimpleQDigest<int64_t>>& result,
      const arg_type<SimpleQDigest<int64_t>>& input,
      const arg_type<double>& scaleFactor) {
    this->template callImpl<int64_t>(result, input, scaleFactor);
  }
};

template <typename TExec>
struct ScaleQDigestRealFunction : public ScaleQDigestBase {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      out_type<SimpleQDigest<float>>& result,
      const arg_type<SimpleQDigest<float>>& input,
      const arg_type<double>& scaleFactor) {
    this->template callImpl<float>(result, input, scaleFactor);
  }
};

} // namespace facebook::velox::functions
