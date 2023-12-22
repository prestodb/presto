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
#include "velox/expression/TypeSignature.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

/// Parses a string into TypeSignature. The format of the string is type name,
/// optionally followed by type parameters enclosed in parenthesis.
///
/// Examples:
///     - bigint
///     - double
///     - array(T)
///     - map(K,V)
///     - row(named bigint,array(tinyint),T)
///     - function(S,T,R)
///
/// Row fields are allowed to be named or anonymous, e.g. "row(foo bigint)" or
/// "row(bigint)"
TypeSignature parseTypeSignature(const std::string& signature);

} // namespace facebook::velox::exec
