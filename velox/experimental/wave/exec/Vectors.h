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

#include "velox/experimental/wave/exec/OperandSet.h"
#include "velox/experimental/wave/exec/Wave.h"
#include "velox/experimental/wave/vector/WaveVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::wave {

void vectorsToDevice(
    folly::Range<const BaseVector**> source,
    const OperandSet& ids,
    WaveStream& stream);

WaveVectorPtr allocateWaveVector(const BaseVector* source, GpuArena& arena);

void ensureWaveVector(
    WaveVectorPtr& waveVector,
    const BaseVector* vector,
    GpuArena& arena);

/// Allocates or resizes WaveVectors / Operands given types, size and
/// nullability.
void ensureVectors(
    folly::Range<vector_size_t*> sizes,
    const std::vector<TypePtr>& types,
    folly::Range<bool*> nullable,
    std::vector<WaveVectorPtr> vectors,
    folly::Range<Operand*> operands,
    GpuArena& arena);

void ensureWaveVector(
    WaveVectorPtr& waveVector,
    const TypePtr& type,
    vector_size_t size,
    bool nullable,
    GpuArena& arena);

// Patches the position at 'offset' in 'code' to be a new uninitialized device
// array of int32_t of at least 'size' elements.
void allocateIndirection(
    GpuArena& arena,
    vector_size_t size,
    const WaveBufferPtr& code,
    int32_t offset);

} // namespace facebook::velox::wave
