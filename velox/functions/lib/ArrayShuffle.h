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

#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {

/// Function metadata with 'deterministic' as false, it is used for both
/// 'makeArrayShuffle' and 'makeArrayShuffleWithCustomSeed'.
/// To ensure 'makeArrayShuffleWithCustomSeed' generates different sequences
/// for constant inputs, We need to set 'deterministic' to false to prevent
/// constant folding.
exec::VectorFunctionMetadata getMetadataForArrayShuffle();

std::vector<std::shared_ptr<exec::FunctionSignature>> arrayShuffleSignatures();

/// Shuffle with rand seed.
std::shared_ptr<exec::VectorFunction> makeArrayShuffle(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>>
arrayShuffleWithCustomSeedSignatures();

/// Shuffle with custom seed (Spark's behavior).
std::shared_ptr<exec::VectorFunction> makeArrayShuffleWithCustomSeed(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

} // namespace facebook::velox::functions
