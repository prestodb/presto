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

#ifndef PORTING_H
#define PORTING_H

#include <string.h>
// #ifdef USE_STRING_H
// #include <string.h>
// #else
// #include <strings.h>
// #endif

#ifdef USE_VALUES_H
#include <values.h>
#endif

#ifdef USE_LIMITS_H
#include <limits.h>
#endif

#ifdef USE_STDLIB_H
#include <stdlib.h>
#endif

#ifndef WIN32
#include <sys/types.h>
#else
#define int32_t __int32
#define int64_t __int64
#endif

#ifdef WIN32
#include <sys/timeb.h>
#define timeb _timeb
#define ftime _ftime
#else
#include <sys/time.h>
#endif

#include "velox/tpcds/gen/dsdgen/include/config.h"

typedef HUGE_TYPE ds_key_t;

/*
 * add some functions that are not strictly ANSI standard
 */
#ifndef strdup
char* strdup(const char*);
#endif

#ifdef WIN32
#include <io.h> // @manual
#include <winbase.h> // @manual
#include <windows.h> // @manual
#include <winsock2.h> // @manual
#define random rand
#define strncasecmp _strnicmp
#define strcasecmp _stricmp
#define strdup _strdup
#define access _access
#define isatty _isatty
#define fileno _fileno
#define F_OK 0
#define MAXINT INT_MAX
#define THREAD __declspec(thread)
#define MIN_MULTI_NODE_ROWS 100000
#define MIN_MULTI_THREAD_ROWS 5000
#define THREAD __declspec(thread)
/* Lines added by Chuck McDevitt for WIN32 support */
#ifndef _POSIX_
#ifndef S_ISREG
#define S_ISREG(m) (((m) & _S_IFMT) == _S_IFREG)
#define S_ISFIFO(m) (((m) & _S_IFMT) == _S_IFIFO)
#endif
#endif
#endif /* WIN32 */

#ifdef _MSC_VER
#pragma comment(lib, "ws2_32.lib")
#endif

#ifdef INTERIX
#include <limits.h>
#define MAXINT INT_MAX
#endif /* INTERIX */

#ifdef AIX
#define MAXINT INT_MAX
#endif

#ifdef MACOS
#include <limits.h>
#define MAXINT INT_MAX
#endif /* MACOS */

#define INTERNAL(m)                                                            \
  {                                                                            \
    auto result = fprintf(                                                     \
        stderr, "ERROR: %s\n\tFile: %s\n\tLine: %d\n", m, __FILE__, __LINE__); \
    if (result < 0)                                                            \
      perror("sprintf failed");                                                \
  }

#define OPEN_CHECK(var, path)                                               \
  if ((var) == NULL) {                                                      \
    fprintf(                                                                \
        stderr, "Open failed for %s at %s:%d\n", path, __FILE__, __LINE__); \
    exit(1);                                                                \
  }

#ifdef MEM_TEST
#define MALLOC_CHECK(v)                                                 \
  if (v == NULL) {                                                      \
    fprintf(stderr, "Malloc Failed at %d in %s\n", __LINE__, __FILE__); \
    exit(1);                                                            \
  } else {                                                              \
    fprintf(                                                            \
        stderr,                                                         \
        "Add (%x) %d at %d in %s\n",                                    \
        sizeof(*v),                                                     \
        v,                                                              \
        __LINE__,                                                       \
        __FILE__);                                                      \
  }
#else
#define MALLOC_CHECK(v)                                                     \
  if (v == nullptr) {                                                       \
    int result =                                                            \
        fprintf(stderr, "Malloc Failed at %d in %s\n", __LINE__, __FILE__); \
    if (result < 0)                                                         \
      perror("fprintf failed");                                             \
    exit(1);                                                                \
  }
#endif /* MEM_TEST */
#endif
