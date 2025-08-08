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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/experimental/wave/dwio/nimble/fuzzer/NimbleReaderFuzzer.h"

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DECLARE_int32(steps);
DECLARE_int32(duration_sec);

DEFINE_int64(allocator_capacity, 8L << 30, "Allocator capacity in bytes.");

DEFINE_int64(arbitrator_capacity, 6L << 30, "Arbitrator capacity in bytes.");

using namespace facebook::velox::exec;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  test::setupMemory(FLAGS_allocator_capacity, FLAGS_arbitrator_capacity);
  const size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  facebook::velox::wave::NimbleReaderFuzzerOptions options;
  facebook::velox::wave::nimbleReaderFuzzer(initialSeed, options, nullptr);
}
