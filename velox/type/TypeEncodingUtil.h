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

namespace facebook::velox {
/// Return the approximate encoding width of the given type. The width of a type
/// is the number of streams involved when reading or writing data of this type.
///
/// NOTE: this is an approximate encoding width for a given data type with
/// direct file encoding or flat in-memory encoding. But for dictionary or
/// flatmap encoding, there might be more streams encoded. Also this doesn't
/// count the encoded null stream.
size_t approximateTypeEncodingwidth(const TypePtr& type);
} // namespace facebook::velox
