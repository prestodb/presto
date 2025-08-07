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

namespace facebook::velox::functions {
/// Murmur3 aligns with Austin Appleby
/// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
///
/// Signed integer types have been remapped to unsigned types (as in the
/// original) to avoid undefined signed integer overflow and sign extension.
class Murmur3Hash32Base {
 protected:
  /// Hash the lower int, then combine with higher int, is a fast path of
  /// hashBytes.
  static uint32_t hashInt64(uint64_t input, uint32_t seed);

  static uint32_t mixK1(uint32_t k1);

  static uint32_t mixH1(uint32_t h1, uint32_t k1);

  // Finalization mix - force all bits of a hash block to avalanche.
  static uint32_t fmix(uint32_t h1, uint32_t length);
};
} // namespace facebook::velox::functions
