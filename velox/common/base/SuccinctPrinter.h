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

namespace facebook::velox {

/// Match the input duration in nanoseconds to the most appropriate unit and
/// return a string value. Possible units are nanoseconds(ns), microseconds(us),
/// milliseconds(ms), seconds(s), minutes(m), hours(h), days(d).
/// The default precision is 2 decimal digits.
std::string succinctNanos(uint64_t duration, int precision = 2);

/// Match the input duration in milliseconds to the most appropriate unit and
/// return a string value. Possible units are milliseconds(ms), seconds(s),
/// minutes(m), hours(h), days(d). The default precision is 2 decimal digits.
std::string succinctMillis(uint64_t duration, int precision = 2);

/// Match the input bytes to the most appropriate unit and return a
/// string value. Possible units are bytes(B), kilobytes(KB),
/// megabytes(MB), gigabyte(GB), terabytes(TB).
/// The default precision is 2 decimal digits.
std::string succinctBytes(uint64_t bytes, int precision = 2);

} // namespace facebook::velox
