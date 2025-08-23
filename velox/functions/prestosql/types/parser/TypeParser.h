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

/// Parses a type string in Presto format to Velox type.
/// Example type strings:
///    row(col0 bigint, varchar)
///    array(bigint)
///    map(bigint, array(bigint))
///    function(bigint,bigint,bigint)
/// The parsing is case-insensitive. i.e. 'Row' and 'row' are equal.
/// Field names for rows are optional.
/// Quoted field names are supported.
/// All custom types need to be registered. An error is thrown otherwise.
/// Uses the Type::getType API to convert a string to Velox type.
TypePtr parseType(const std::string& typeText);

} // namespace facebook::velox::functions::prestosql
