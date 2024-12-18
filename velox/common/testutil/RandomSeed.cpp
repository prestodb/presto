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

#include "velox/common/testutil/RandomSeed.h"

#include <folly/Conv.h>
#include <folly/Random.h>
#include <glog/logging.h>

#include <cstdlib>

namespace facebook::velox::common::testutil {

bool useRandomSeed() {
  const char* env = getenv("VELOX_TEST_USE_RANDOM_SEED");
  return env && folly::to<bool>(env);
}

unsigned getRandomSeed(unsigned fixedValue) {
  if (!useRandomSeed()) {
    return fixedValue;
  }
  auto seed = folly::Random::secureRand32();
  LOG(INFO) << "Random seed: " << seed;
  return seed;
}

} // namespace facebook::velox::common::testutil
