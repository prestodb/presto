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

DEFINE_bool(
    velox_time_allocations,
    true,
    "Record time and volume for large allocation/free");

// Used in common/base/VeloxException.cpp
DEFINE_bool(
    velox_exception_user_stacktrace_enabled,
    false,
    "Enable the stacktrace for user type of VeloxException");

DEFINE_bool(
    velox_exception_system_stacktrace_enabled,
    true,
    "Enable the stacktrace for system type of VeloxException");

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

// Used in exec/Expr.cpp

DEFINE_string(
    velox_save_input_on_expression_any_failure_path,
    "",
    "Enable saving input vector and expression SQL on any failure during "
    "expression evaluation. Specifies the directory to use for storing the "
    "vectors and expression SQL strings.");

DEFINE_string(
    velox_save_input_on_expression_system_failure_path,
    "",
    "Enable saving input vector and expression SQL on system failure during "
    "expression evaluation. Specifies the directory to use for storing the "
    "vectors and expression SQL strings. This flag is ignored if "
    "velox_save_input_on_expression_any_failure_path is set.");
