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

#include "velox/common/base/Exceptions.h"

#include <folly/Demangle.h>

namespace facebook::velox {

namespace detail {

template <typename From, typename To>
void ensureCastSucceeded(To* casted, From* original) {
  // Either casted or original will be nullptr. Otherwise it's a bad usage.
  if (casted == nullptr) {
    VELOX_CHECK_NOT_NULL(
        original, "If casted is nullptr, original must not be.");
    VELOX_FAIL(
        "Failed to cast from '{}' to '{}'. Object is of type '{}'.",
        folly::demangle(typeid(From).name()),
        folly::demangle(typeid(To).name()),
        folly::demangle(typeid(*original).name()));
  }
}

} // namespace detail

// `checked_pointer_cast` is a dynamic casting tool to throw a Velox exception
// when the casting failed. Use this instead of `std::dynamic_pointer_cast`
// when:
//   1) Casting must happen
//   2) We want a stack trace if it failed.
template <typename To, typename From>
std::shared_ptr<To> checked_pointer_cast(const std::shared_ptr<From>& input) {
  VELOX_CHECK_NOT_NULL(input.get());
  auto casted = std::dynamic_pointer_cast<To>(input);
  detail::ensureCastSucceeded(casted.get(), input.get());
  return casted;
}

template <typename To, typename From>
std::unique_ptr<To> checked_pointer_cast(std::unique_ptr<From> input) {
  VELOX_CHECK_NOT_NULL(input.get());
  auto* released = input.release();
  To* casted{nullptr};
  try {
    casted = dynamic_cast<To*>(released);
    detail::ensureCastSucceeded(casted, released);
  } catch (...) {
    input.reset(released);
    throw;
  }
  return std::unique_ptr<To>(casted);
}

template <typename To, typename From>
To* checked_pointer_cast(From* input) {
  VELOX_CHECK_NOT_NULL(input);
  auto* casted = dynamic_cast<To*>(input);
  detail::ensureCastSucceeded(casted, input);
  return casted;
}

template <typename To, typename From>
std::unique_ptr<To> static_unique_pointer_cast(std::unique_ptr<From> input) {
  VELOX_CHECK_NOT_NULL(input.get());
  auto* released = input.release();
  auto* casted = static_cast<To*>(released);
  return std::unique_ptr<To>(casted);
}

template <typename To, typename From>
bool is_instance_of(const std::shared_ptr<From>& input) {
  VELOX_CHECK_NOT_NULL(input.get());
  auto* casted = dynamic_cast<const To*>(input.get());
  return casted != nullptr;
}

template <typename To, typename From>
bool is_instance_of(const std::unique_ptr<From>& input) {
  VELOX_CHECK_NOT_NULL(input.get());
  auto* casted = dynamic_cast<const To*>(input.get());
  return casted != nullptr;
}

template <typename To, typename From>
bool is_instance_of(const From* input) {
  VELOX_CHECK_NOT_NULL(input);
  auto* casted = dynamic_cast<const To*>(input);
  return casted != nullptr;
}

} // namespace facebook::velox
