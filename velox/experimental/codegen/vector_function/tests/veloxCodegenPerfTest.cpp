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

#include <gtest/gtest.h>
#include "velox/experimental/codegen/vector_function/Perf.h"

using namespace ::testing;

namespace facebook::velox::codegen {
/*@mode/dev-sand
branch instructions: 3012
branch misses: 7
cpu clock: 6862
task clock: 4513
@mode/opt
branch instructions: 6
branch misses: 2
cpu clock: 4307
task clock: 1956
*/
TEST(PerfTest, TestLoop) {
  Perf p;
  for (int i = 0; i < 1000; i++) {
    /// do nothing, without optimization, each loop contains 3 branches
    /// (according to godbolt.org with x86-clang++ 12.0.1), with optimization
    /// this whole thing disappears
  }
}
} // namespace facebook::velox::codegen
