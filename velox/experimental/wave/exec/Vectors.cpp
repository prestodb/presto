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

#include "velox/experimental/wave/exec/Vectors.h"

namespace facebook::velox::wave {

WaveVectorPtr allocateWaveVector(const BaseVector* source, GpuArena& arena) {
  auto result = WaveVector::create(source->type(), arena);
  result->resize(source->size(), source->mayHaveNulls());
  return result;
}

void ensureWaveVector(
    WaveVectorPtr& waveVector,
    const BaseVector* vector,
    GpuArena& arena) {
  if (!waveVector) {
    waveVector = allocateWaveVector(vector, arena);
  } else {
    waveVector->resize(vector->size(), vector->mayHaveNulls());
  }
}

void ensureWaveVector(
    WaveVectorPtr& waveVector,
    const TypePtr& type,
    vector_size_t size,
    bool nullable,
    GpuArena& arena) {
  if (!waveVector) {
    waveVector = WaveVector::create(type, arena);
  }
  waveVector->resize(size, nullable);
}

void ensureVectors(
    folly::Range<vector_size_t*> sizes,
    const std::vector<TypePtr>& types,
    folly::Range<bool*> nullable,
    std::vector<WaveVectorPtr> vectors,
    folly::Range<Operand*> operands,
    GpuArena& arena) {
  if (vectors.size() < operands.size()) {
    vectors.resize(operands.size());
  }
  for (auto i = 0; i < operands.size(); ++i) {
    vector_size_t size = sizes.size() > i ? sizes[i] : sizes.back();
    bool isNullable = nullable.empty() ? true
        : nullable.size() > i          ? nullable[i]
                                       : nullable.back();
    ensureWaveVector(vectors[i], types[i], size, isNullable, arena);
    vectors[i]->toOperand(&operands[i]);
  }
}

void transferVector(
    const BaseVector* source,
    int32_t index,
    std::vector<Transfer>& transfers,
    std::vector<WaveVectorPtr>& waveVectors,
    std::vector<Operand>& operands,
    GpuArena& arena,
    int64_t& totalBytes) {
  if (waveVectors.size() <= index) {
    waveVectors.resize(index + 1);
  }
  ensureWaveVector(waveVectors[index], source, arena);
  if (operands.size() <= index) {
    operands.resize(index + 1);
  }
  waveVectors[index]->toOperand(&operands[index]);
  auto values = source->values();
  auto rawNulls = source->rawNulls();
  if (values) {
    auto rawValues = values->as<char>();
    if (source->typeKind() == TypeKind::BOOLEAN) {
      auto bytes = bits::nbytes(source->size());
      transfers.emplace_back(
          rawValues, waveVectors[index]->values<char>(), bytes);
      totalBytes += bytes;
    } else {
      auto bytes = source->size() * source->type()->cppSizeInBytes();
      transfers.emplace_back(
          rawValues, waveVectors[index]->values<char>(), bytes);
      totalBytes += bytes;
    }
    if (rawNulls) {
      auto bytes = bits::nbytes(source->size());
      transfers.emplace_back(rawNulls, waveVectors[index]->nulls(), bytes);
      totalBytes += bytes;
    }
  }
}

void vectorsToDevice(
    folly::Range<const BaseVector**> source,
    const OperandSet& ids,
    WaveStream& stream) {
  std::vector<Transfer> transfers;
  int64_t bytes = 0;
  std::vector<Operand> operandVector;
  std::vector<WaveVectorPtr> waveVectors;
  auto& arena = stream.arena();
  for (auto i = 0; i < source.size(); ++i) {
    transferVector(
        source[i], i, transfers, waveVectors, operandVector, arena, bytes);
  }
  Executable::startTransfer(
      ids, std::move(waveVectors), std::move(transfers), stream);
}

// Patches the position 'ofet' in 'code' to be a new uninitialized device
// array of int32_t of at least 'size' elements.
void allocateIndirection(
    GpuArena& arena,
    vector_size_t size,
    const WaveBufferPtr& code,
    int32_t offset);

} // namespace facebook::velox::wave
