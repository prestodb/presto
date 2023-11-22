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
#include "velox/type/Type.h"

namespace facebook::velox {

/// Normalize Presto types such as INT and DOUBLE PRECISION and convert to Velox
/// type.
TypePtr typeFromString(
    const std::string& type,
    bool failIfNotRegistered = true);

/// Convert words with spaces to a Velox type.
/// First check if all the words are a Velox type.
/// Then check if the first word is a field name and the remaining words are a
/// Velox type. If cannotHaveFieldName = true, then all words must be a Velox
/// type.
std::pair<std::string, TypePtr> inferTypeWithSpaces(
    std::vector<std::string>& words,
    bool cannotHaveFieldName = false);

} // namespace facebook::velox
