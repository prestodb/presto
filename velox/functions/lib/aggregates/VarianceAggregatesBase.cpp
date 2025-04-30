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

#include "velox/functions/lib/aggregates/VarianceAggregatesBase.h"

namespace facebook::velox::functions::aggregate {

void VarianceAccumulator::update(double value) {
  count_ += 1;
  double delta = value - mean();
  mean_ += delta / count();
  m2_ += delta * (value - mean());
}

void VarianceAccumulator::merge(
    int64_t countOther,
    double meanOther,
    double m2Other) {
  if (countOther == 0) {
    return;
  }
  if (count_ == 0) {
    count_ = countOther;
    mean_ = meanOther;
    m2_ = m2Other;
    return;
  }
  int64_t newCount = countOther + count();
  double delta = meanOther - mean();
  double newMean = mean() + delta / newCount * countOther;
  m2_ += m2Other +
      delta * delta * countOther * count() / static_cast<double>(newCount);
  count_ = newCount;
  mean_ = newMean;
}

void checkSumCountRowType(
    const TypePtr& type,
    const std::string& errorMessage) {
  VELOX_CHECK_EQ(type->kind(), TypeKind::ROW, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kCountIdx)->kind(), TypeKind::BIGINT, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kMeanIdx)->kind(), TypeKind::DOUBLE, "{}", errorMessage);
  VELOX_CHECK_EQ(
      type->childAt(kM2Idx)->kind(), TypeKind::DOUBLE, "{}", errorMessage);
}

} // namespace facebook::velox::functions::aggregate
