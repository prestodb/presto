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

namespace facebook::velox::util {

// Returns the timezone name associated with timeZoneID.
std::string getTimeZoneName(int64_t timeZoneID);

// Returns the timeZoneID for the timezone name.
// If failOnError = true, throws an exception for unrecognized timezone.
// Otherwise, returns -1.
int16_t getTimeZoneID(std::string_view timeZone, bool failOnError = true);

} // namespace facebook::velox::util
