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

// Adapted from Apache Arrow.

#include "XxHasher.h"

#define XXH_INLINE_ALL
#include <external/xxhash/xxhash.h>

namespace facebook::velox::parquet {

namespace {
template <typename T>
uint64_t XxHashHelper(T value, uint32_t seed) {
  return XXH64(reinterpret_cast<const void*>(&value), sizeof(T), seed);
}

template <typename T>
void XxHashesHelper(
    const T* values,
    uint32_t seed,
    int numValues,
    uint64_t* results) {
  for (int i = 0; i < numValues; ++i) {
    results[i] = XxHashHelper(values[i], seed);
  }
}

} // namespace

uint64_t XxHasher::hash(int32_t value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::hash(int64_t value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::hash(float value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::hash(double value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::hash(const ByteArray* value) const {
  return XXH64(
      reinterpret_cast<const void*>(value->ptr),
      value->len,
      kParquetBloomXxHashSeed);
}

void XxHasher::hashes(const int32_t* values, int numValues, uint64_t* hashes)
    const {
  XxHashesHelper(values, kParquetBloomXxHashSeed, numValues, hashes);
}

void XxHasher::hashes(const int64_t* values, int numValues, uint64_t* hashes)
    const {
  XxHashesHelper(values, kParquetBloomXxHashSeed, numValues, hashes);
}

void XxHasher::hashes(const float* values, int numValues, uint64_t* hashes)
    const {
  XxHashesHelper(values, kParquetBloomXxHashSeed, numValues, hashes);
}

void XxHasher::hashes(const double* values, int numValues, uint64_t* hashes)
    const {
  XxHashesHelper(values, kParquetBloomXxHashSeed, numValues, hashes);
}

void XxHasher::hashes(const ByteArray* values, int numValues, uint64_t* hashes)
    const {
  for (int i = 0; i < numValues; ++i) {
    hashes[i] = XXH64(
        reinterpret_cast<const void*>(values[i].ptr),
        values[i].len,
        kParquetBloomXxHashSeed);
  }
}

} // namespace facebook::velox::parquet
