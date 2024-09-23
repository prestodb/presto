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

namespace facebook::velox::test {

struct FloatConstants {
  static constexpr auto kNaND = std::numeric_limits<double>::quiet_NaN();
  static constexpr auto kNaNF = std::numeric_limits<float>::quiet_NaN();

  static constexpr auto kMaxD = std::numeric_limits<double>::max();
  static constexpr auto kMaxF = std::numeric_limits<float>::max();
};
} // namespace facebook::velox::test
