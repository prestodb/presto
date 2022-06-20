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
#define VELOX_SUPPRESS_DEPRECATION_WARNING \
  _Pragma("clang diagnostic push");        \
  _Pragma("clang diagnostic ignored \"-Wdeprecated-declarations\"")
#define VELOX_UNSUPPRESS_DEPRECATION_WARNING _Pragma("clang diagnostic pop");
#define VELOX_SUPPRESS_RETURN_LOCAL_ADDR_WARNING
#define VELOX_UNSUPPRESS_RETURN_LOCAL_ADDR_WARNING
#else
#define VELOX_SUPPRESS_DEPRECATION_WARNING
#define VELOX_UNSUPPRESS_DEPRECATION_WARNING
#define VELOX_SUPPRESS_RETURN_LOCAL_ADDR_WARNING \
  _Pragma("GCC diagnostic push");                \
  _Pragma("GCC diagnostic ignored \"-Wreturn-local-addr\"")
#define VELOX_UNSUPPRESS_RETURN_LOCAL_ADDR_WARNING \
  _Pragma("GCC diagnostic pop");
#endif
