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

#include "velox/expression/fuzzer/ArgGenerator.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::velox::fuzzer::test {

/// Assert the equivalence between the given return type and the actual type
/// resolved from generated argument types.
void assertReturnType(
    const std::shared_ptr<ArgGenerator>& generator,
    const exec::FunctionSignature& signature,
    const TypePtr& returnType);

// Assert that no argument types can be generated for the given return type.
void assertEmptyArgs(
    std::shared_ptr<ArgGenerator> generator,
    const exec::FunctionSignature& signature,
    const TypePtr& returnType);

} // namespace facebook::velox::fuzzer::test
