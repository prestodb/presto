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

namespace facebook::velox::common::testutil {

/// Get a truely random seed and log it for future reproducing if
/// VELOX_TEST_USE_RANDOM_SEED is set.  Otherwise return a fixed value so test
/// runs are deterministic.  We use environment variable because `buck test`
/// does not allow pass in command line arguments.
unsigned getRandomSeed(unsigned fixedValue);

bool useRandomSeed();

} // namespace facebook::velox::common::testutil
