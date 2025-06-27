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

#include <limits>
#include <vector>

#ifdef VELOX_ENABLE_FAISS
#include "faiss/utils/distances.h"
#endif

namespace facebook::velox::functions {
namespace {

#ifdef VELOX_ENABLE_FAISS

template <typename T>
struct CosineSimilarityFunctionMap {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void callNullFree(
      out_type<double>& result,
      const null_free_arg_type<Map<Varchar, double>>& leftMap,
      const null_free_arg_type<Map<Varchar, double>>& rightMap) {
    if (leftMap.empty() || rightMap.empty()) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }
    // dimension of the vectors, and be conservative assume no overlap.
    int d = static_cast<int>(leftMap.size() + rightMap.size());
    // Make a vector to store values of left and right maps
    std::vector<float> x(d * 2, 0);
    size_t i = 0;
    // TODO: optimize this and avoid copying.
    for (const auto& [key, value] : leftMap) {
      auto it = rightMap.find(key);
      double right = (it != rightMap.end()) ? it->second : 0;
      if (i < d) { // Check buffer bounds
        x[i] = static_cast<float>(value);
      }
      if (static_cast<size_t>(d + i) < x.size()) { // Check buffer bounds
        x[d + i] = static_cast<float>(right);
      }
      i++;
    }
    // For the remaining keys in rightMap that does not appear in leftMap
    for (const auto& [key, value] : rightMap) {
      if (leftMap.find(key) == leftMap.end()) {
        x[d + i++] = static_cast<float>(value);
      }
    }
    // Compute the L2 norms by calling faiss
    float norms[2] = {0};
    faiss::fvec_norms_L2(norms, /*x=*/x.data(), /*d=*/d, /*nx=*/2);
    float product = faiss::fvec_inner_product(x.data(), /*y=*/x.data() + d, d);
    if (norms[0] == 0 || norms[1] == 0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }
    // Cosine similarity
    result = static_cast<double>(product / (norms[0] * norms[1]));
  }
};

template <typename T>
struct CosineSimilarityFunctionArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void callNullFree(
      out_type<double>& result,
      const null_free_arg_type<Array<double>>& leftArray,
      const null_free_arg_type<Array<double>>& rightArray) {
    VELOX_USER_CHECK(
        leftArray.size() == rightArray.size(),
        "Both arrays need to have identical size");

    size_t d = leftArray.size();
    std::vector<float> x(static_cast<std::vector<float>::size_type>(d * 2), 0);
    for (size_t i = 0; i < leftArray.size(); i++) {
      if (i < x.size()) { // Check buffer bounds
        x[i] = static_cast<float>(leftArray[i]);
      }
    }
    for (size_t i = 0; i < rightArray.size(); i++) {
      if (i + d < x.size()) { // Check buffer bounds
        x[i + d] = static_cast<float>(rightArray[i]);
      }
    }

    float norms[2] = {0};
    faiss::fvec_norms_L2(norms, /*x=*/x.data(), /*d=*/d, /*nx=*/2);
    if (norms[0] == 0 || norms[1] == 0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }
    float product = faiss::fvec_inner_product(x.data(), /*y=*/x.data() + d, d);

    result = static_cast<double>(product / (norms[0] * norms[1]));
  }
};

template <typename T>
const T* getArrayDataOrCopy(
    const facebook::velox::exec::ArrayView<false, T>& array,
    std::vector<T>& buffer) {
  if (array.isFlatElements()) {
    auto flatVec = dynamic_cast<const facebook::velox::FlatVector<T>*>(
        array.elementsVectorBase());
    if (flatVec) {
      return flatVec->template rawValues<T>() + array.offset();
    }
  }
  // Not flat: must copy
  buffer.resize(array.size());
  for (size_t i = 0; i < array.size(); ++i) {
    buffer[i] = array[i];
  }
  return buffer.data();
}

template <typename T>
struct CosineSimilarityFunctionFloatArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void callNullFree(
      out_type<float>& result,
      const facebook::velox::exec::ArrayView<false, float>& leftArray,
      const facebook::velox::exec::ArrayView<false, float>& rightArray) {
    VELOX_USER_CHECK(
        leftArray.size() == rightArray.size(),
        "Both arrays need to have identical size");
    size_t d = leftArray.size();
    if (d == 0) {
      result = std::numeric_limits<float>::quiet_NaN();
      return;
    }

    std::vector<float> leftBuffer, rightBuffer;
    const float* x = getArrayDataOrCopy(leftArray, leftBuffer);
    const float* y = getArrayDataOrCopy(rightArray, rightBuffer);

    float normX = 0, normY = 0;
    faiss::fvec_norms_L2(&normX, x, d, 1);
    faiss::fvec_norms_L2(&normY, y, d, 1);
    if (normX == 0 || normY == 0) {
      result = std::numeric_limits<float>::quiet_NaN();
      return;
    }
    float product = faiss::fvec_inner_product(x, y, d);
    result = static_cast<float>(product / (normX * normY));
  }
};

template <typename T>
struct L2SquaredFunctionFloatArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void callNullFree(
      out_type<float>& result,
      const facebook::velox::exec::ArrayView<false, float>& leftArray,
      const facebook::velox::exec::ArrayView<false, float>& rightArray) {
    VELOX_USER_CHECK(
        leftArray.size() == rightArray.size(),
        "Both arrays need to have identical size");
    size_t d = leftArray.size();
    if (d == 0) {
      result = std::numeric_limits<float>::quiet_NaN();
      return;
    }

    std::vector<float> leftBuffer, rightBuffer;
    const float* x = getArrayDataOrCopy(leftArray, leftBuffer);
    const float* y = getArrayDataOrCopy(rightArray, rightBuffer);
    result = faiss::fvec_L2sqr(x, y, d);
  }
};

template <typename T>
struct L2SquaredFunctionDoubleArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void callNullFree(
      out_type<double>& result,
      const facebook::velox::exec::ArrayView<false, double>& leftArray,
      const facebook::velox::exec::ArrayView<false, double>& rightArray) {
    VELOX_USER_CHECK(
        leftArray.size() == rightArray.size(),
        "Both arrays need to have identical size");
    size_t d = leftArray.size();
    if (d == 0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    std::vector<float> x(static_cast<std::vector<float>::size_type>(d), 0);
    std::vector<float> y(static_cast<std::vector<float>::size_type>(d), 0);

    for (size_t i = 0; i < leftArray.size(); i++) {
      if (i < x.size()) {
        x[i] = static_cast<float>(leftArray[i]);
      }
    }
    for (size_t i = 0; i < rightArray.size(); i++) {
      if (i < x.size()) {
        y[i] = static_cast<float>(rightArray[i]);
      }
    }

    float l2_sqr = faiss::fvec_L2sqr(x.data(), y.data(), d);
    result = static_cast<double>(l2_sqr);
  }
};

#else // VELOX_ENABLE_FAISS

template <typename T>
struct CosineSimilarityFunctionMap {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  double normalize(const null_free_arg_type<Map<Varchar, double>>& map) {
    double norm = 0.0;
    for (const auto& [key, value] : map) {
      norm += (value * value);
    }
    return std::sqrt(norm);
  }

  double dotProduct(
      const null_free_arg_type<Map<Varchar, double>>& leftMap,
      const null_free_arg_type<Map<Varchar, double>>& rightMap) {
    double result = 0.0;
    for (const auto& [key, value] : leftMap) {
      auto it = rightMap.find(key);
      if (it != rightMap.end()) {
        result += value * it->second;
      }
    }
    return result;
  }

  void callNullFree(
      out_type<double>& result,
      const null_free_arg_type<Map<Varchar, double>>& leftMap,
      const null_free_arg_type<Map<Varchar, double>>& rightMap) {
    if (leftMap.empty() || rightMap.empty()) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double normLeftMap = normalize(leftMap);
    if (normLeftMap == 0.0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double normRightMap = normalize(rightMap);
    if (normRightMap == 0.0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double product = dotProduct(leftMap, rightMap);
    result = product / (normLeftMap * normRightMap);
  }
};

template <typename T>
struct CosineSimilarityFunctionArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  double normalize(const null_free_arg_type<Array<double>>& map) {
    double norm = 0.0;
    for (const auto value : map) {
      norm += (value * value);
    }
    return std::sqrt(norm);
  }

  double dotProduct(
      const null_free_arg_type<Array<double>>& leftArray,
      const null_free_arg_type<Array<double>>& rightArray) {
    double result = 0.0;
    for (size_t i = 0; i < leftArray.size(); i++) {
      result += leftArray[i] * rightArray[i];
    }
    return result;
  }

  void callNullFree(
      out_type<double>& result,
      const null_free_arg_type<Array<double>>& leftArray,
      const null_free_arg_type<Array<double>>& rightArray) {
    VELOX_USER_CHECK(
        leftArray.size() == rightArray.size(),
        "Both arrays need to have identical size");

    double normLeftArray = normalize(leftArray);
    if (normLeftArray == 0.0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double normRightArray = normalize(rightArray);
    if (normRightArray == 0.0) {
      result = std::numeric_limits<double>::quiet_NaN();
      return;
    }

    double product = dotProduct(leftArray, rightArray);
    result = product / (normLeftArray * normRightArray);
  }
};
#endif // VELOX_ENABLE_FAISS

} // namespace
} // namespace facebook::velox::functions
