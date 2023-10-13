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

#include "velox/common/base/CheckedArithmetic.h"

// Forwarding the definitions here so that codegen can still use functions in
// this namespace.
namespace facebook::velox::functions {

template <typename T>
T checkedPlus(const T& a, const T& b) {
  return facebook::velox::checkedPlus(a, b);
}

template <typename T>
T checkedMinus(const T& a, const T& b) {
  return facebook::velox::checkedMinus(a, b);
}

template <typename T>
T checkedMultiply(const T& a, const T& b) {
  return facebook::velox::checkedMultiply(a, b);
}

template <typename T>
T checkedDivide(const T& a, const T& b) {
  return facebook::velox::checkedDivide(a, b);
}

template <typename T>
T checkedModulus(const T& a, const T& b) {
  return facebook::velox::checkedModulus(a, b);
}

template <typename T>
T checkedNegate(const T& a) {
  return facebook::velox::checkedNegate(a);
}

} // namespace facebook::velox::functions
