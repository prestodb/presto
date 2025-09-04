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

#include <cuda_runtime.h> // @manual

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/experimental/wave/dwio/nimble/fuzzer/NimbleReaderFuzzer.h"

DECLARE_int32(steps);
DECLARE_int32(duration_sec);

namespace facebook::velox::wave {
struct NimbleReaderFuzzerParam {
  int64_t seed{0};
  NimbleReaderFuzzerOptions options{};
};
std::vector<NimbleReaderFuzzerParam> NimbleReaderFuzzerParams() {
  return {
      {.seed = 42, .options = {}},
      {.seed = 56, .options = {}},
      {.seed = 189, .options = {}},
      {.seed = 331, .options = {}},
  };
}
class NimbleReaderFuzzerTest
    : public testing::Test,
      public testing::WithParamInterface<NimbleReaderFuzzerParam> {
 protected:
  static void SetUpTestCase() {
    memory::initializeMemoryManager(velox::memory::MemoryManager::Options{});
  }
  void SetUp() override {
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
    auto param = GetParam();
    seed_ = param.seed;
    opts_ = param.options;
  }

  int64_t seed_{0};
  NimbleReaderFuzzerOptions opts_{};
};

TEST_P(NimbleReaderFuzzerTest, basic) {
  facebook::velox::wave::nimbleReaderFuzzer(seed_, opts_, nullptr);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    NimbleReaderFuzzerTests,
    NimbleReaderFuzzerTest,
    testing::ValuesIn(NimbleReaderFuzzerParams()));
} // namespace facebook::velox::wave
