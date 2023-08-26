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

#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/Type.h"
#include "velox/experimental/wave/exec/AggregateFunction.h"
#include "velox/experimental/wave/exec/ErrorCode.h"

namespace facebook::velox::wave::aggregation {

struct Group {
  int initialized;
  void** keys;
  void** accumulators;
};

struct GroupsContainer {
  int32_t numKeys;
  int32_t numAggregates;
  int32_t numGroups;
  int32_t actualNumGroups;
  PhysicalType* keyTypes;
  void** idMaps;
  Group* groups;
  bool useThreadLocalAccumulator;
};

struct NormalizeKeys {
  GroupsContainer* container;
  Operand* inputs;
  Operand* result;
};

struct Aggregate {
  GroupsContainer* container;
  Operand* normalizedKey;
  AggregateFunction* function;
  int32_t numInputs;
  Operand* inputs;
  int32_t accumulatorIndex;
};

struct ExtractKeys {
  GroupsContainer* container;
  int32_t keyIndex;
  Operand* result;
  static int sharedSize();
};

struct ExtractValues {
  GroupsContainer* container;
  AggregateFunction* function;
  int32_t accumulatorIndex;
  Operand* result;
  static int sharedSize();
};

enum class OpCode {
  kNormalizeKeys,
  kAggregate,
  kExtractKeys,
  kExtractValues,
};

struct Instruction {
  OpCode opCode;
  union {
    NormalizeKeys normalizeKeys;
    Aggregate aggregate;
    ExtractKeys extractKeys;
    ExtractValues extractValues;
  } _;
};

struct ThreadBlockProgram {
  int32_t numInstructions;
  Instruction* instructions;
};

void call(
    Stream& stream,
    int numBlocks,
    ThreadBlockProgram* programs,
    int32_t* baseIndices,
    BlockStatus* status,
    int sharedSize);

} // namespace facebook::velox::wave::aggregation
