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

#include "velox/functions/lib/SubscriptUtil.h"

namespace facebook::velox::functions {

namespace {
std::exception_ptr makeZeroSubscriptError() {
  try {
    VELOX_USER_FAIL("SQL array indices start at 1");
  } catch (const std::exception& e) {
    return std::current_exception();
  }
}

std::exception_ptr makeBadSubscriptError() {
  try {
    VELOX_USER_FAIL("Array subscript out of bounds.");
  } catch (const std::exception& e) {
    return std::current_exception();
  }
}

std::exception_ptr makeNegativeSubscriptError() {
  try {
    VELOX_USER_FAIL("Array subscript is negative.");
  } catch (const std::exception& e) {
    return std::current_exception();
  }
}
} // namespace

const std::exception_ptr& zeroSubscriptError() {
  static std::exception_ptr error = makeZeroSubscriptError();
  return error;
}

const std::exception_ptr& badSubscriptError() {
  static std::exception_ptr error = makeBadSubscriptError();
  return error;
}

const std::exception_ptr& negativeSubscriptError() {
  static std::exception_ptr error = makeNegativeSubscriptError();
  return error;
}

} // namespace facebook::velox::functions
