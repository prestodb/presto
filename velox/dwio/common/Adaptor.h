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

#define HAS_PRE_1970

#include <string>

#ifdef __clang__
#define DIAGNOSTIC_PUSH _Pragma("clang diagnostic push")
#define DIAGNOSTIC_POP _Pragma("clang diagnostic pop")
#elif defined(__GNUC__)
#define DIAGNOSTIC_PUSH _Pragma("GCC diagnostic push")
#define DIAGNOSTIC_POP _Pragma("GCC diagnostic pop")
#else
#error("Unknown compiler")
#endif

#define PRAGMA(TXT) _Pragma(#TXT)

#ifdef __clang__
#define DIAGNOSTIC_IGNORE(XXX) PRAGMA(clang diagnostic ignored XXX)
#elif defined(__GNUC__)
#define DIAGNOSTIC_IGNORE(XXX) PRAGMA(GCC diagnostic ignored XXX)
#else
#define DIAGNOSTIC_IGNORE(XXX)
#endif

#ifndef UINT32_MAX
#define UINT32_MAX 0xffffffff
#endif

#ifndef INT64_MAX
#define INT64_MAX 0x7fffffffffffffff
#endif

#ifndef INT64_MIN
#define INT64_MIN (-0x7fffffffffffffff - 1)
#endif
