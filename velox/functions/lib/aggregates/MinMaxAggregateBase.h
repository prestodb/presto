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

#include "velox/exec/Aggregate.h"

namespace facebook::velox::functions::aggregate {

/// Min and max functions in Presto and Spark have different semantics:
/// 1. Nested NULLs are compared as values in Spark and as "unknown value" in
/// Presto.
/// 2. The timestamp type represents a time instant in microsecond precision in
/// Spark, but millisecond precision in Presto.
/// Parameters 'nullHandlingMode' and 'precision' allow to register min and max
/// functions with different behaviors.
exec::AggregateFunctionFactory getMinFunctionFactory(
    const std::string& name,
    CompareFlags::NullHandlingMode nullHandlingMode,
    TimestampPrecision precision);

exec::AggregateFunctionFactory getMaxFunctionFactory(
    const std::string& name,
    CompareFlags::NullHandlingMode nullHandlingMode,
    TimestampPrecision precision);
} // namespace facebook::velox::functions::aggregate
