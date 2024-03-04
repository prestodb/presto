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

#include "folly/Random.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions {

template <typename T>
struct RandFunction {
  static constexpr bool is_deterministic = false;
  static constexpr auto canonical_name = exec::FunctionCanonicalName::kRand;

  FOLLY_ALWAYS_INLINE void call(double& result) {
    result = folly::Random::randDouble01();
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& out, const int64_t input) {
    checkBound(input);
    out = folly::Random::rand64(input);
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& out, const int32_t input) {
    checkBound(input);
    out = folly::Random::rand32(input);
  }

  FOLLY_ALWAYS_INLINE void call(int16_t& out, const int16_t input) {
    checkBound(input);
    out = int16_t(folly::Random::rand32(input));
  }

  FOLLY_ALWAYS_INLINE void call(int8_t& out, const int8_t input) {
    checkBound(input);
    out = int8_t(folly::Random::rand32(input));
  }

  template <typename InputType>
  FOLLY_ALWAYS_INLINE
      typename std::enable_if<std::is_integral<InputType>::value, void>::type
      checkBound(InputType input) {
    VELOX_USER_CHECK_GT(input, 0, "bound must be positive");
  }
};

} // namespace facebook::velox::functions
