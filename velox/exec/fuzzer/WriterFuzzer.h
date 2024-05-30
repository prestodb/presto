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

#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_bool(enable_window_reference_verification);

namespace facebook::velox::exec::test {
/// Runs the writer fuzzer.
/// @param seed Random seed - Pass the same seed for reproducibility.
/// @param referenceQueryRunner Reference query runner for results
/// verification.
void writerFuzzer(
    size_t seed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

/// Returns all the folders in a local fs path recursively.
std::vector<std::string> listFolders(std::string_view path);

} // namespace facebook::velox::exec::test
