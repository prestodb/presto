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

#include "velox/expression/StringWriter.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions::sparksql {

/// Trims trailing ASCII space characters (0x20) from `'abc'`
/// to ensure its length does not exceed the specified Unicode string length
/// `limit` (must be greater than 0) in characters. Throws an exception if the
/// string still exceeds `limit` after trimming.
void trimTrailingSpaces(
    exec::StringWriter& output,
    StringView inputStr,
    int32_t numChars,
    uint32_t limit);

} // namespace facebook::velox::functions::sparksql
