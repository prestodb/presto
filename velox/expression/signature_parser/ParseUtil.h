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

namespace facebook::velox::exec {

/// Convert words with spaces to a Velox Type Signature.
/// First check if all the words are a Velox type.
/// If cannotHaveFieldName = false, check if the first word is a field name and
/// the remaining words are a Velox type.
TypeSignaturePtr inferTypeWithSpaces(
    const std::vector<std::string>& words,
    bool canHaveFieldName = false);

} // namespace facebook::velox::exec
