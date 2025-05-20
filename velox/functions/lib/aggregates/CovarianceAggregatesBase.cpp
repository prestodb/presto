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

#include "velox/functions/lib/aggregates/CovarianceAggregatesBase.h"

namespace facebook::velox::functions::aggregate {

int64_t CovarAccumulator::count() const {
  return count_;
}

double CovarAccumulator::c2() const {
  return c2_;
}

double CovarAccumulator::meanX() const {
  return meanX_;
}

double CovarAccumulator::meanY() const {
  return meanY_;
}

void CovarAccumulator::update(double x, double y) {
  count_ += 1;
  double deltaX = x - meanX();
  meanX_ += deltaX / count();
  double deltaY = y - meanY();
  meanY_ += deltaY / count();
  c2_ += deltaX * (y - meanY());
}

void CovarAccumulator::merge(
    int64_t countOther,
    double meanXOther,
    double meanYOther,
    double c2Other) {
  if (countOther == 0) {
    return;
  }
  if (count_ == 0) {
    count_ = countOther;
    meanX_ = meanXOther;
    meanY_ = meanYOther;
    c2_ = c2Other;
    return;
  }

  int64_t newCount = countOther + count();
  double deltaMeanX = meanXOther - meanX();
  double deltaMeanY = meanYOther - meanY();
  c2_ += c2Other +
      deltaMeanX * deltaMeanY * count() * countOther /
          static_cast<double>(newCount);
  meanX_ += deltaMeanX * countOther / static_cast<double>(newCount);
  meanY_ += deltaMeanY * countOther / static_cast<double>(newCount);
  count_ = newCount;
}

double RegrAccumulator::m2X() const {
  return m2X_;
}

void RegrAccumulator::update(double x, double y) {
  double oldMeanX = meanX();
  CovarAccumulator::update(x, y);

  m2X_ += (x - oldMeanX) * (x - meanX());
}

void RegrAccumulator::merge(
    int64_t countOther,
    double meanXOther,
    double meanYOther,
    double c2Other,
    double m2XOther) {
  if (countOther == 0) {
    return;
  }
  if (count() == 0) {
    m2X_ = m2XOther;
  } else {
    m2X_ += m2XOther +
        1.0 * count() / (count() + countOther) * countOther *
            std::pow(meanX() - meanXOther, 2);
  }
  CovarAccumulator::merge(countOther, meanXOther, meanYOther, c2Other);
}

double ExtendedRegrAccumulator::m2Y() const {
  return m2Y_;
}

void ExtendedRegrAccumulator::update(double x, double y) {
  double oldMeanY = meanY();
  RegrAccumulator::update(x, y);
  m2Y_ += (y - oldMeanY) * (y - meanY());
}

void ExtendedRegrAccumulator::merge(
    int64_t countOther,
    double meanXOther,
    double meanYOther,
    double c2Other,
    double m2XOther,
    double m2YOther) {
  if (countOther == 0) {
    return;
  }
  if (count() == 0) {
    m2X_ = m2XOther;
    m2Y_ = m2YOther;
  } else {
    m2X_ += m2XOther +
        1.0 * count() / (count() + countOther) * countOther *
            std::pow(meanX() - meanXOther, 2);

    m2Y_ += m2YOther +
        1.0 * count() / (count() + countOther) * countOther *
            std::pow(meanY() - meanYOther, 2);
  }
  CovarAccumulator::merge(countOther, meanXOther, meanYOther, c2Other);
}

void CovarIntermediateInput::mergeInto(
    CovarAccumulator& accumulator,
    vector_size_t row) {
  accumulator.merge(
      count_->valueAt(row),
      meanX_->valueAt(row),
      meanY_->valueAt(row),
      c2_->valueAt(row));
}

void CovarIntermediateResult::set(
    vector_size_t row,
    const CovarAccumulator& accumulator) {
  count_[row] = accumulator.count();
  meanX_[row] = accumulator.meanX();
  meanY_[row] = accumulator.meanY();
  c2_[row] = accumulator.c2();
}

void CorrAccumulator::update(double x, double y) {
  double oldMeanX = meanX();
  double oldMeanY = meanY();
  CovarAccumulator::update(x, y);

  m2X_ += (x - oldMeanX) * (x - meanX());
  m2Y_ += (y - oldMeanY) * (y - meanY());
}

void CorrAccumulator::merge(
    int64_t countOther,
    double meanXOther,
    double meanYOther,
    double c2Other,
    double m2XOther,
    double m2YOther) {
  if (countOther == 0) {
    return;
  }

  if (count() == 0) {
    m2X_ = m2XOther;
    m2Y_ = m2YOther;
  } else {
    auto k = 1.0 * count() / (count() + countOther) * countOther;
    m2X_ += m2XOther + k * std::pow(meanX() - meanXOther, 2);
    m2Y_ += m2YOther + k * std::pow(meanY() - meanYOther, 2);
  }

  CovarAccumulator::merge(countOther, meanXOther, meanYOther, c2Other);
}

void CorrIntermediateInput::mergeInto(
    CorrAccumulator& accumulator,
    vector_size_t row) {
  accumulator.merge(
      count_->valueAt(row),
      meanX_->valueAt(row),
      meanY_->valueAt(row),
      c2_->valueAt(row),
      m2X_->valueAt(row),
      m2Y_->valueAt(row));
}

void CorrIntermediateResult::set(
    vector_size_t row,
    const CorrAccumulator& accumulator) {
  CovarIntermediateResult::set(row, accumulator);
  m2X_[row] = accumulator.m2X();
  m2Y_[row] = accumulator.m2Y();
}

} // namespace facebook::velox::functions::aggregate
