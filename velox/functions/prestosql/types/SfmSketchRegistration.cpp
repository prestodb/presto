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

#include "velox/functions/prestosql/types/SfmSketchRegistration.h"
#include "velox/functions/prestosql/types/SfmSketchType.h"
#include "velox/functions/prestosql/types/fuzzer_utils/SfmSketchInputGenerator.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {
class SfmSketchTypeFactory : public CustomTypeFactory {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_CHECK(parameters.empty());
    return SFMSKETCH();
  }

  // SfmSketch supports casting and should be treated as Varbinary during type
  // casting.
  exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& config) const override {
    return std::make_shared<fuzzer::SfmSketchInputGenerator>(
        config.seed_, config.nullRatio_, config.pool_);
  }
};
} // namespace
void registerSfmSketchType() {
  registerCustomType(
      "sfmsketch", std::make_unique<const SfmSketchTypeFactory>());
}
} // namespace facebook::velox
