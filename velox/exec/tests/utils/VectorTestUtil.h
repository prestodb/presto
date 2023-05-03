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

#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

// Shuffle batches.
void shuffleBatches(std::vector<RowVectorPtr>& batches);

// Make batches with random data.
//
// NOTE: if 'batchSize' is 0, then 'numBatches' is ignored and the function
// returns a single empty batch.
std::vector<velox::RowVectorPtr> makeBatches(
    int32_t batchSize,
    int32_t numBatches,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool,
    double nullRatio = 0.1,
    bool shuffle = true);

std::vector<RowVectorPtr> makeBatches(
    vector_size_t numBatches,
    std::function<RowVectorPtr(int32_t)> makeVector,
    bool shuffle = true);

std::vector<RowVectorPtr> mergeBatches(
    std::vector<RowVectorPtr>&& lhs,
    std::vector<RowVectorPtr>&& rhs,
    bool shuffle = false);

std::vector<std::string> concat(
    const std::vector<std::string>& a,
    const std::vector<std::string>& b);

std::vector<RowVectorPtr> makeCopies(
    const std::vector<RowVectorPtr>& source,
    int32_t numCopies);

} // namespace facebook::velox::exec::test
