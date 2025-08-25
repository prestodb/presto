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

namespace facebook::velox::functions::prestosql {

/// Normalize Presto types such as INT and DOUBLE PRECISION and convert to Velox
/// type.
TypePtr typeFromString(
    const std::string& type,
    bool failIfNotRegistered = true);

TypePtr customTypeWithChildren(
    const std::string& name,
    const std::vector<TypePtr>& children);

/// Creates a LongEnumParameter with the enumName and valuesMap and passes it in
/// as a parameter to BIGINT_ENUM type to return an enum Type.
/// The valuesMap is assumed to have a format of "[["CURIOUS",-2], ["HAPPY",0]]"
/// so that folly::parseJson can be used to parse and throw on duplicate keys
/// and/or values.
TypePtr getEnumType(
    const std::string& enumType,
    const std::string& enumName,
    const std::string& valuesMap);

/// Convert words with spaces to a Velox type.
/// First check if all the words are a Velox type.
/// Then check if the first word is a field name and the remaining words are a
/// Velox type. If cannotHaveFieldName = true, then all words must be a Velox
/// type.
std::pair<std::string, TypePtr> inferTypeWithSpaces(
    std::vector<std::string>& words,
    bool cannotHaveFieldName = false);

} // namespace facebook::velox::functions::prestosql
