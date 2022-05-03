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

#include "velox/common/base/SimdUtil.h"
#include <folly/Preprocessor.h>

namespace facebook::velox::simd {

namespace detail {

alignas(kPadding) int32_t byteSetBits[256][8];
alignas(kPadding) int32_t permute4x64Indices[16][8];

} // namespace detail

namespace {

void initByteSetBits() {
  for (int32_t i = 0; i < 256; ++i) {
    int32_t* entry = detail::byteSetBits[i];
    int32_t fill = 0;
    for (auto b = 0; b < 8; ++b) {
      if (i & (1 << b)) {
        entry[fill++] = b;
      }
    }
    for (; fill < 8; ++fill) {
      entry[fill] = fill;
    }
  }
}

void initPermute4x64Indices() {
  for (int i = 0; i < 16; ++i) {
    int32_t* result = detail::permute4x64Indices[i];
    int32_t fill = 0;
    for (int bit = 0; bit < 4; ++bit) {
      if (i & (1 << bit)) {
        result[fill++] = bit * 2;
        result[fill++] = bit * 2 + 1;
      }
    }
    for (; fill < 8; ++fill) {
      result[fill] = fill;
    }
  }
}

} // namespace

bool initializeSimdUtil() {
  static bool inited = false;
  if (inited) {
    return true;
  }
  initByteSetBits();
  initPermute4x64Indices();
  inited = true;
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_simdConstants) = initializeSimdUtil();

} // namespace facebook::velox::simd
