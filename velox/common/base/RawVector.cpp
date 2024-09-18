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

#include "velox/common/base/RawVector.h"
#include <folly/Preprocessor.h>
#include <numeric>

namespace facebook::velox {

namespace {
std::vector<int32_t> iotaData;

bool initializeIota() {
  iotaData.resize(10000);
  std::iota(iotaData.begin(), iotaData.end(), 0);
  return true;
}
} // namespace

const int32_t*
iota(int32_t size, raw_vector<int32_t>& storage, int32_t offset) {
  if (iotaData.size() < offset + size) {
    storage.resize(size);
    std::iota(storage.begin(), storage.end(), offset);
    return storage.data();
  }
  return iotaData.data() + offset;
}

static bool FB_ANONYMOUS_VARIABLE(g_iotaConstants) = initializeIota();

} // namespace facebook::velox
