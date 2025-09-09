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

#include "velox/functions/prestosql/types/P4HyperLogLogRegistration.h"

#include "velox/functions/prestosql/types/P4HyperLogLogType.h"
#include "velox/functions/prestosql/types/fuzzer_utils/P4HyperLogLogInputGenerator.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {
class P4HyperLogLogTypeFactory : public CustomTypeFactory {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_CHECK(parameters.empty());
    return P4HYPERLOGLOG();
  }

  // P4HyperLogLog should be treated as Varbinary during type castings.
  exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& config) const override {
    return std::make_shared<fuzzer::P4HyperLogLogInputGenerator>(
        config.seed_, config.nullRatio_, config.pool_);
  }
};
} // namespace
void registerP4HyperLogLogType() {
  registerCustomType(
      "p4hyperloglog", std::make_unique<const P4HyperLogLogTypeFactory>());
}
} // namespace facebook::velox
