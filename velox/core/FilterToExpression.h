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

namespace facebook::velox::core {
core::TypedExprPtr filterToExpr(
    const common::Subfield& subfield,
    const common::Filter* filter,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool);
} // namespace facebook::velox::core
