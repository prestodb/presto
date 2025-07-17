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

#include "velox/vector/BaseVector.h"

namespace facebook::velox {

// Converts Variant `value` into a Velox vector using specified type.
//
// Supports all primitive types and complex types that do not contain DECIMAL
// types.
//
// @returns ConstantVector of size 1.
//
// TODO Fold into BaseVector::createConstant API.
VectorPtr variantToVector(
    const TypePtr& type,
    const Variant& value,
    memory::MemoryPool* pool);

// Convers a value at 'index' of 'vector' into a Variant.
Variant vectorToVariant(const VectorPtr& vector, vector_size_t index);

} // namespace facebook::velox
