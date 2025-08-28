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
/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#ifndef CONFIG_H
#define CONFIG_H

// #ifdef MACOS
#define SUPPORT_64BITS
#define HUGE_TYPE int64_t
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_STDLIB_H
#define USE_LIMITS_H
#include <limits.h>
#define MAXINT INT_MAX

// #define FLEX
// #endif /* MACOS */

#ifdef NCR
#define STDLIB_HAS_GETOPT
#define USE_STRING_H
#define USE_VALUES_H
#ifdef SQLSERVER
#define WIN32
#else
/* the 64 bit defines are for the Metaware compiler */
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_COUNT 1
#define HUGE_FORMAT "%LLd"
#define int32_t int
#endif /* SQLSERVER or MP/RAS */
#endif /* NCR */

#ifdef AIX
#define _ALL_SOURCE
#define USE_STRING_H
#define USE_LIMITS_H
/*
 * if the C compiler is 3.1 or later, then uncomment the
 * lines for 64 bit seed generation
 */
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_COUNT 1
#define HUGE_FORMAT "%lld"
#define STDLIB_HAS_GETOPT
#define USE_STDLIB_H
#define FLEX
#endif /* AIX */

#ifdef CYGWIN
#define USE_STRING_H
#define PATH_SEP '\\'
#define SUPPORT_64BITS
#define HUGE_TYPE __int64
#define HUGE_COUNT 1
#define HUGE_FORMAT "%I64d"
#endif /* WIN32 */

#ifdef HPUX
#define SUPPORT_64BITS
#define HUGE_TYPE long long int
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#define FLEX
#endif /* HPUX */

#ifdef INTERIX
#define USE_LIMITS_H
#define SUPPORT_64BITS
#define HUGE_TYPE long long int
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#endif /* INTERIX */

#ifdef LINUX
#define SUPPORT_64BITS
#define HUGE_TYPE int64_t
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#define FLEX
#endif /* LINUX */

#ifdef SOLARIS
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#endif /* SOLARIS */

#ifdef SOL86
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#endif /* SOLARIS */

#ifdef WIN32
#define USE_STRING_H
#define USE_LIMITS_H
#define PATH_SEP '\\'
#define SUPPORT_64BITS
#define HUGE_TYPE __int64
#define HUGE_COUNT 1
#define HUGE_FORMAT "%I64d"
#endif /* WIN32 */

/* preliminary defines for 64-bit windows compile */
#ifdef WIN64
#define USE_STRING_H
#define PATH_SEP '\\'
#define SUPPORT_64BITS
#define HUGE_TYPE __int64
#define HUGE_COUNT 1
#define HUGE_FORMAT "%I64d"
#endif /* WIN32 */

#ifndef PATH_SEP
#define PATH_SEP '/'
#endif /* PATH_SEP */

#ifndef HUGE_TYPE
#error The code now requires 64b support
#endif

/***
 ** DATABASE DEFINES
 ***/
#ifdef _MYSQL
#define STR_QUOTES
#endif

#endif /* CONFIG_H */
