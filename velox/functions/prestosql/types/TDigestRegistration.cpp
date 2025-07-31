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

#include "velox/functions/prestosql/types/TDigestRegistration.h"
#include "velox/common/fuzzer/ConstrainedGenerators.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {
class TDigestTypeFactory : public CustomTypeFactory {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_CHECK_EQ(parameters.size(), 1);
    VELOX_CHECK(parameters[0].kind == TypeParameterKind::kType);
    return TDIGEST(parameters[0].type);
  }

  // TDigest should be treated as Varbinary during type castings.
  exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& config) const override {
    fuzzer::FuzzerGenerator rng(config.seed_);
    return std::make_shared<fuzzer::TDigestInputGenerator>(
        config.seed_, TDIGEST(DOUBLE()), config.nullRatio_);
  }
};
} // namespace

void registerTDigestType() {
  registerCustomType("tdigest", std::make_unique<const TDigestTypeFactory>());
}
} // namespace facebook::velox
