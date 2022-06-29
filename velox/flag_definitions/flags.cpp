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

#include <gflags/gflags.h>

// Used in velox/builder/SimpleVectorBuilder.cpp

DEFINE_int32(
    max_block_value_set_length,
    5,
    "Max entries per column that the block meta-record stores for pre-flight "
    "filtering optimization");

// Used in velox/common/memory/Memory.cpp

DEFINE_int32(
    memory_usage_aggregation_interval_millis,
    2,
    "Interval to compute aggregate memory usage for all nodes");

// Used in velox/common/memory/MappedMemory.cpp

DEFINE_int32(
    velox_memory_pool_mb,
    4 * 1024,
    "Size of file cache/operator working memory in MB");

DEFINE_bool(
    velox_use_malloc,
    true,
    "Use malloc for file cache and large operator allocations");

// Used in common/base/VeloxException.cpp

/// TODO: deprecate 'FLAGS_deprecate velox_exception_stacktrace' flag once after
/// 'FLAGS_velox_exception_user_stacktrace_enabled' and
/// 'FLAGS_velox_exception_system_stacktrace_enabled' have been rolled out in
/// production.
DEFINE_bool(
    velox_exception_stacktrace,
    true,
    "Enable the stacktrace for VeloxException");

DEFINE_bool(
    velox_exception_user_stacktrace_enabled,
    false,
    "Enable the stacktrace for user type of VeloxException");

DEFINE_bool(
    velox_exception_system_stacktrace_enabled,
    true,
    "Enable the stacktrace for system type of VeloxException");

/// TODO: deprecate 'FLAGS_velox_exception_stacktrace_rate_limit_ms' flag once
/// after 'FLAGS_velox_exception_user_stacktrace_rate_limit_ms' and
/// 'FLAGS_velox_exception_system_stacktrace_rate_limit_ms' have been rolled out
/// in production.
DEFINE_int32(
    velox_exception_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " VeloxException; off when set to 0 (the default)");

DEFINE_int32(
    velox_exception_user_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " user type of VeloxException; off when set to 0 (the default)");

DEFINE_int32(
    velox_exception_system_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " system type of VeloxException; off when set to 0 (the default)");

// Used in common/base/ProcessBase.cpp

DEFINE_bool(avx2, true, "Enables use of AVX2 when available");

DEFINE_bool(bmi2, true, "Enables use of BMI2 when available");
