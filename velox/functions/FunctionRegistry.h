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

#include <string>
#include <vector>

#include "velox/expression/FunctionSignature.h"
#include "velox/type/Type.h"

namespace facebook::velox {

using FunctionSignatureMap = std::
    unordered_map<std::string, std::vector<const exec::FunctionSignature*>>;

/// Returns a mapping of all Simple and Vector functions registered in Velox
/// The mapping is function name -> list of function signatures
FunctionSignatureMap getFunctionSignatures();

/// Given a function name and argument types, returns
/// the return type if function exists otherwise returns nullptr
std::shared_ptr<const Type> resolveFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Given name of simple function and argument types, returns
/// the return type if function exists otherwise returns nullptr
std::shared_ptr<const Type> resolveSimpleFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Given name of vector function and argument types, returns
/// the return type if function exists otherwise returns nullptr
std::shared_ptr<const Type> resolveVectorFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

} // namespace facebook::velox
