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

#include "velox/functions/prestosql/types/fuzzer_utils/HyperLogLogInputGenerator.h"

#include "velox/common/fuzzer/Utils.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/type/Variant.h"

namespace facebook::velox::fuzzer {

HyperLogLogInputGenerator::HyperLogLogInputGenerator(
    const size_t seed,
    const double nullRatio,
    memory::MemoryPool* pool,
    int32_t minNumValues)
    : AbstractInputGenerator{seed, HYPERLOGLOG(), nullptr, nullRatio},
      minNumValues_{minNumValues},
      pool_{pool} {
  static const std::vector<TypePtr> kBaseTypes{
      BIGINT(), VARCHAR(), DOUBLE(), UNKNOWN()};
  baseType_ = kBaseTypes[rand<int32_t>(rng_, 0, kBaseTypes.size() - 1)];
  error_ = rand<double>(rng_, 0.0040625, 0.26000);
}

variant HyperLogLogInputGenerator::generate() {
  if (coinToss(rng_, nullRatio_)) {
    return variant::null(type_->kind());
  }

  if (baseType_->isBigint()) {
    return generateTyped<int64_t>();
  } else if (baseType_->isVarchar()) {
    return generateTyped<std::string>();
  } else if (baseType_->isDouble()) {
    return generateTyped<double>();
  } else {
    return generateTyped<UnknownValue>();
  }
}

} // namespace facebook::velox::fuzzer
