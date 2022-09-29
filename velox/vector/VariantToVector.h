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

namespace facebook::velox::core {

// Converts a sequence of values from a variant array to an ArrayVector. The
// output ArrayVector contains one single row, which contains the elements
// extracted from the input variant vector.
ArrayVectorPtr variantArrayToVector(
    const std::vector<variant>& variantArray,
    velox::memory::MemoryPool* pool);

} // namespace facebook::velox::core
