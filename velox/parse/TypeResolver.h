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
#include "velox/type/Type.h"

namespace facebook::velox::parse {

// Registers type resolver with the parser to resolve return types of special
// forms (if, switch, and, or, etc.) and vector functions.
void registerTypeResolver();

// Given scalar function name and input types, resolves the return type. Throws
// an exception if return type cannot be resolved. Returns null if return type
// cannot be resolved and 'nullOnFailure' is true.
TypePtr resolveScalarFunctionType(
    const std::string& name,
    const std::vector<TypePtr>& args,
    bool nullOnFailure = false);

} // namespace facebook::velox::parse
