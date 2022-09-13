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

#include <cstdint>

namespace facebook::velox::exec::test {

struct NonPODInt64 {
  static int constructed;
  static int destructed;

  static void clearStats() {
    constructed = 0;
    destructed = 0;
  }

  int64_t value;

  NonPODInt64(int64_t value_ = 0) : value(value_) {
    ++constructed;
  }

  ~NonPODInt64() {
    value = -1;
    ++destructed;
  }

  // No move/copy constructor and assignment operator are used in this case.
  NonPODInt64(const NonPODInt64& other) = delete;
  NonPODInt64(NonPODInt64&& other) = delete;
  NonPODInt64& operator=(const NonPODInt64&) = delete;
  NonPODInt64& operator=(NonPODInt64&&) = delete;
};
} // namespace facebook::velox::exec::test
