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

#include <cstdint>

namespace facebook::velox {
class Type;
}

namespace facebook::velox::wave {

struct PhysicalType {
  enum Kind {
    kInt8,
    kInt16,
    kInt32,
    kInt64,
    kInt128,
    kFloat32,
    kFloat64,
    kString,
    kArray,
    kMap,
    kRow,
  } kind;
  int32_t numChildren;
  PhysicalType** children;
};

PhysicalType fromCpuType(const Type&);

} // namespace facebook::velox::wave
