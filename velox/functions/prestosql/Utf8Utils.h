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

#include <cstdint>

namespace facebook::velox::functions {

/// This function is not part of the original utf8proc.
/// Tries to get the length of UTF-8 encoded code point.  A
/// positive return value means the UTF-8 sequence is valid, and
/// the result is the length of the code point.  A negative return value means
/// the UTF-8 sequence at the position is invalid, and the length of the invalid
/// sequence is the absolute value of the result.
///
/// @param input Pointer to the first byte of the code point. Must not be null.
/// @param size Number of available bytes. Must be greater than zero.
/// @return the length of the code point or negative the number of bytes in the
/// invalid UTF-8 sequence.
///
/// Adapted from tryGetCodePointAt in
/// https://github.com/airlift/slice/blob/master/src/main/java/io/airlift/slice/SliceUtf8.java
int32_t tryGetCharLength(const char* input, int64_t size);

} // namespace facebook::velox::functions
