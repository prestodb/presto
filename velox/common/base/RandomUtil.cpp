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

#include "velox/common/base/RandomUtil.h"

#include <optional>

#include <folly/Random.h>

namespace facebook::velox::random {

namespace {

std::optional<uint32_t> customSeed;

}

void setSeed(uint32_t value) {
  customSeed = value;
}

uint32_t getSeed() {
  return customSeed ? *customSeed : folly::Random::rand32();
}

} // namespace facebook::velox::random
