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

#include <glob.h>

constexpr float kCompressionRatioInitialGuess = 0.3f;
constexpr float kFlushOverheadRatioInitialGuess = 0.1f;
// TODO(T79660637): make write cost estimate more granular.
constexpr float kAverageRowSizeInitialGuess = 0.0f;

namespace facebook::velox::dwrf {

class RatioTracker {
 public:
  explicit RatioTracker(float initialGuess);
  virtual ~RatioTracker() = default;

  float getEstimatedRatio() const;
  virtual bool isEmptyInput(size_t denominator, size_t numerator);
  void takeSample(size_t denominator, size_t numerator);
  size_t getSampleSize() const;

 private:
  float workingAverage_;
  size_t sampleSize_{0};
  size_t numeratorTotal_{0};
  size_t denominatorTotal_{0};
};

class CompressionRatioTracker final : public RatioTracker {
 public:
  explicit CompressionRatioTracker(
      float initialGuess = kCompressionRatioInitialGuess);

  bool isEmptyInput(size_t denominator, size_t numerator) override;
};

class FlushOverheadRatioTracker final : public RatioTracker {
 public:
  explicit FlushOverheadRatioTracker(
      float initialGuess = kFlushOverheadRatioInitialGuess);
};

class AverageRowSizeTracker final : public RatioTracker {
 public:
  explicit AverageRowSizeTracker(
      float initialGuess = kAverageRowSizeInitialGuess);
};

} // namespace facebook::velox::dwrf
