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

#include "velox/functions/prestosql/types/fuzzer_utils/SfmSketchInputGenerator.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/types/SfmSketchType.h"

namespace facebook::velox::fuzzer {

SfmSketchInputGenerator::SfmSketchInputGenerator(
    const size_t seed,
    const double nullRatio,
    memory::MemoryPool* pool)
    : AbstractInputGenerator(seed, SFMSKETCH(), nullptr, nullRatio),
      pool_(pool) {
  // Initialize supported input types for SfmSketch.
  static const std::vector<TypePtr> kBaseTypes{
      BIGINT(), VARCHAR(), DOUBLE(), UNKNOWN()};
  baseType_ = kBaseTypes[rand<size_t>(rng_, 0, kBaseTypes.size() - 1)];
}

variant SfmSketchInputGenerator::generate() {
  if (coinToss(rng_, nullRatio_)) {
    return variant::null(TypeKind::VARBINARY);
  }

  // Generate based on the type
  switch (baseType_->kind()) {
    case TypeKind::BIGINT:
      return generateTyped<int64_t>();
    case TypeKind::DOUBLE:
      return generateTyped<double>();
    case TypeKind::VARCHAR:
      return generateTyped<std::string>();
    case TypeKind::UNKNOWN:
      return generateTyped<UnknownValue>();
    default:
      VELOX_UNREACHABLE("Unsupported type for SfmSketch generation");
  }
}

} // namespace facebook::velox::fuzzer
