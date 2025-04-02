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

#include <folly/Random.h>

#include <boost/random/uniform_01.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/random/uniform_real_distribution.hpp>

#include "velox/expression/ComplexWriterTypes.h"

namespace facebook::velox {
namespace generator_spec_utils {

vector_size_t getRandomIndex(FuzzerGenerator& rng, vector_size_t maxIndex);

BufferPtr generateNullsBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t vectorSize,
    double nullProbability);

BufferPtr generateIndicesBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t bufferSize,
    vector_size_t baseVectorSize);
} // namespace generator_spec_utils

namespace fuzzer {
/// Used to write variants to a GenericWriter, recursively calls itself for
/// complex types.
template <TypeKind KIND>
void writeOne(const variant& v, exec::GenericWriter& writer) {
  using T = typename TypeTraits<KIND>::NativeType;
  writer.template castTo<T>() = v.value<KIND>();
}

template <>
void writeOne<TypeKind::VARCHAR>(const variant& v, exec::GenericWriter& writer);

template <>
void writeOne<TypeKind::VARBINARY>(
    const variant& v,
    exec::GenericWriter& writer);

template <>
void writeOne<TypeKind::ARRAY>(const variant& v, exec::GenericWriter& writer);

template <>
void writeOne<TypeKind::MAP>(const variant& v, exec::GenericWriter& writer);

template <>
void writeOne<TypeKind::ROW>(const variant& v, exec::GenericWriter& writer);
} // namespace fuzzer
} // namespace facebook::velox
