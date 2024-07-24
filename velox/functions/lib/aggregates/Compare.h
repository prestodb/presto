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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::functions::aggregate {

/// Compare the new value of the DecodedVector at the given index with the value
/// stored in the SingleValueAccumulator. Returns 0 if stored and new values are
/// equal; <0 if stored value is less then new value; >0 if stored value is
/// greater than new value.
///
/// If nullHandlingMode is NullAsValue, nested nulls are handled as value. If
/// nullHandlingMode is StopAtNull, it will throw an exception when complex
/// type values contain nulls.
/// Note, The default nullHandlingMode in Presto is StopAtNull while the
/// default nullHandlingMode is NullAsValue in Spark.
int32_t compare(
    const velox::functions::aggregate::SingleValueAccumulator* accumulator,
    const DecodedVector& decoded,
    vector_size_t index,
    CompareFlags::NullHandlingMode nullHandlingMode);
} // namespace facebook::velox::functions::aggregate
