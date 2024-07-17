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

// Macros to disable deprecation warnings
#ifdef __clang__
#define VELOX_SUPPRESS_STRINGOP_OVERFLOW_WARNING
#define VELOX_UNSUPPRESS_STRINGOP_OVERFLOW_WARNING
#else
#define VELOX_SUPPRESS_STRINGOP_OVERFLOW_WARNING \
  _Pragma("GCC diagnostic push");                \
  _Pragma("GCC diagnostic ignored \"-Wstringop-overflow\"")
#define VELOX_UNSUPPRESS_STRINGOP_OVERFLOW_WARNING \
  _Pragma("GCC diagnostic pop");
#endif

#define VELOX_CONCAT(x, y) x##y
// Need this extra layer to expand __COUNTER__.
#define VELOX_VARNAME_IMPL(x, y) VELOX_CONCAT(x, y)
#define VELOX_VARNAME(x) VELOX_VARNAME_IMPL(x, __COUNTER__)
