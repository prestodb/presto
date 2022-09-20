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

/// Serializes the type into binary format and writes it to the provided
/// output stream. Used for testing.
void saveType(const TypePtr& type, std::ostream& out);

/// Deserializes a type serialized by 'saveType' from the provided input stream.
/// Used for testing.
TypePtr restoreType(std::istream& in);

/// Serializes the vector into binary format and writes it to the provided
/// output stream. The serialiation preserved encoding.
void saveVector(const BaseVector& vector, std::ostream& out);

/// Deserializes a vector serialized by 'save' from the provided input stream.
VectorPtr restoreVector(std::istream& in, memory::MemoryPool* pool);
} // namespace facebook::velox
