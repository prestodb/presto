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

#include "velox/common/memory/MemoryPool.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"

namespace facebook::velox::core::test {
/// Converts a Filter object to a TypedExpr object that can be used in Velox's
/// expression evaluation system.
///
/// This function takes a filter that applies to a specific subfield and
/// converts it to an equivalent expression tree.
///
/// @param subfield The subfield to which the filter applies. This is used to
///                 create the base expression that the filter condition will
///                 be applied to.
/// @param filter The filter to convert. This can be any subclass of Filter,
///               such as AlwaysTrue, IsNull, BigintRange, BytesValues, etc.
/// @param rowType The row type that contains the subfield. This is used to
///                resolve the subfield path and determine its type.
/// @param pool Memory pool to use for allocations.
/// @return A TypedExpr object representing the filter condition applied to
///         the subfield.
core::TypedExprPtr filterToExpr(
    const common::Subfield& subfield,
    const common::Filter* filter,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool);
} // namespace facebook::velox::core::test
