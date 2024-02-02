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

#include "velox/exec/prefixsort/tests/utils/EncoderTestUtils.h"

namespace facebook::velox::exec::prefixsort::test {

namespace {

/// The decode method will only be used in testing scenarios. In production
/// code, we have no scenarios that require decoding a normalized key.
void decodeNoNulls(char* encoded, int64_t& value) {
  value = *reinterpret_cast<uint64_t*>(encoded);
  value = __builtin_bswap64(value ^ 128);
}

} // namespace

void encodeInPlace(std::vector<int64_t>& data) {
  const static auto encoder = PrefixSortEncoder(true, true);
  for (auto i = 0; i < data.size(); i++) {
    encoder.encodeNoNulls(data[i], (char*)data.data() + i * sizeof(int64_t));
  }
}

void decodeInPlace(std::vector<int64_t>& data) {
  for (auto i = 0; i < data.size(); i++) {
    decodeNoNulls((char*)data.data() + i * sizeof(int64_t), data[i]);
  }
}

} // namespace facebook::velox::exec::prefixsort::test
