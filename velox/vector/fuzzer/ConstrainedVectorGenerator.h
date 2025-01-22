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

#include "velox/common/fuzzer/ConstrainedGenerators.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::fuzzer {

class ConstrainedVectorGenerator {
 public:
  ConstrainedVectorGenerator() = delete;

  static VectorPtr generateConstant(
      const std::shared_ptr<AbstractInputGenerator>& customGenerator,
      vector_size_t size,
      memory::MemoryPool* pool);

  static VectorPtr generateFlat(
      const std::shared_ptr<AbstractInputGenerator>& customGenerator,
      vector_size_t size,
      memory::MemoryPool* pool);
};

} // namespace facebook::velox::fuzzer
