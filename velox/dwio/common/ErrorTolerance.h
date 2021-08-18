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

#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace dwio {
namespace common {

/**
 * Error tolerance level for readers
 * Errors include schema mismatch(not able to parse data as expected type)
 */
enum class ErrorToleranceLevel {
  TOLERANCE_IGNORE, // Ignore any column parsing errors.
  TOLERANCE_STRICT, // Raise exception immediately
  TOLERANCE_PERMISSIVE // Allow user specified number of conversion errors.
                       // bad values are converted to nulls
};

struct ErrorTolerance {
 private:
  ErrorToleranceLevel toleranceLevel_ =
      ErrorToleranceLevel::TOLERANCE_PERMISSIVE;
  float maxAllowableErrorsRatio_ = 1;

 public:
  ErrorTolerance() = default;

  void setTolerance(
      ErrorToleranceLevel toleranceLevel,
      float maxAllowableErrorsRatio = 1) {
    toleranceLevel_ = toleranceLevel;
    maxAllowableErrorsRatio_ = maxAllowableErrorsRatio;
    DWIO_ENSURE(
        ErrorTolerance::maxAllowableErrorsRatio_ > 0 &&
            ErrorTolerance::maxAllowableErrorsRatio_ <= 1,
        "invalid Error tolerance: ",
        maxAllowableErrorsRatio);
  }

  ErrorToleranceLevel toleranceLevel() {
    return toleranceLevel_;
  }
  float maxAllowableErrorsRatio() {
    return maxAllowableErrorsRatio_;
  }
};

} // namespace common
} // namespace dwio
} // namespace facebook
