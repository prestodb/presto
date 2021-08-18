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

#include "velox/dwio/dwrf/writer/RatioTracker.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwrf {

RatioTracker::RatioTracker(float initialGuess)
    : workingAverage_{initialGuess} {}

float RatioTracker::getEstimatedRatio() const {
  return workingAverage_;
}

bool RatioTracker::isEmptyInput(size_t denominator, size_t numerator) {
  return denominator == 0;
}

void RatioTracker::takeSample(size_t denominator, size_t numerator) {
  // Discard empty input.
  if (isEmptyInput(denominator, numerator)) {
    return;
  }

  numeratorTotal_ += numerator;
  denominatorTotal_ += denominator;
  DWIO_ENSURE_GT(denominatorTotal_, 0);
  // Don't care about underflow.
  workingAverage_ = numeratorTotal_ * 1.0f / denominatorTotal_;
  ++sampleSize_;
}

size_t RatioTracker::getSampleSize() const {
  return sampleSize_;
}

CompressionRatioTracker::CompressionRatioTracker(float initialGuess)
    : RatioTracker{initialGuess} {}

bool CompressionRatioTracker::isEmptyInput(
    size_t denominator,
    size_t numerator) {
  return numerator == 0 || denominator == 0;
}

FlushOverheadRatioTracker::FlushOverheadRatioTracker(float initialGuess)
    : RatioTracker{initialGuess} {}

AverageRowSizeTracker::AverageRowSizeTracker(float initialGuess)
    : RatioTracker{initialGuess} {}

} // namespace facebook::velox::dwrf
