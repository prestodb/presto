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

#include "velox/functions/lib/TimeUtils.h"

namespace facebook::velox::functions {

const folly::F14FastMap<std::string, int8_t> kDayOfWeekNames{
    {"th", 0},       {"fr", 1},     {"sa", 2},       {"su", 3},
    {"mo", 4},       {"tu", 5},     {"we", 6},       {"thu", 0},
    {"fri", 1},      {"sat", 2},    {"sun", 3},      {"mon", 4},
    {"tue", 5},      {"wed", 6},    {"thursday", 0}, {"friday", 1},
    {"saturday", 2}, {"sunday", 3}, {"monday", 4},   {"tuesday", 5},
    {"wednesday", 6}};
} // namespace facebook::velox::functions
