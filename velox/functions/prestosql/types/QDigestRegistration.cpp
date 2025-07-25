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

#include "velox/functions/prestosql/types/QDigestRegistration.h"

#include "velox/common/fuzzer/ConstrainedGenerators.h"
#include "velox/functions/prestosql/types/QDigestType.h"

namespace facebook::velox {
namespace {
class QDigestTypeFactories : public CustomTypeFactories {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_CHECK_EQ(parameters.size(), 1);
    VELOX_CHECK(parameters[0].kind == TypeParameterKind::kType);
    return QDIGEST(parameters[0].type);
  }

  // QDigest should be treated as Varbinary during type castings.
  exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& config) const override {
    VELOX_USER_CHECK_NOT_NULL(config.type_, "QDigest Type must be provided");
    const auto& parameters = config.type_->parameters();

    VELOX_USER_CHECK(
        parameters.size() == 1 &&
            parameters[0].kind == TypeParameterKind::kType,
        "QDigest must have exactly one parameter");

    const auto paramType = parameters[0].type;
    const auto kind = paramType->kind();
    VELOX_USER_CHECK(
        // QDigest supports three types: BIGINT, REAL, and DOUBLE
        kind == TypeKind::BIGINT || kind == TypeKind::REAL ||
            kind == TypeKind::DOUBLE,
        "QDigest only supports BIGINT, REAL, and DOUBLE parameter types, got: {}",
        paramType->toString());

    return std::make_shared<fuzzer::QDigestInputGenerator>(
        config.seed_, QDIGEST(paramType), config.nullRatio_, paramType);
  }
};
} // namespace

void registerQDigestType() {
  registerCustomType("qdigest", std::make_unique<const QDigestTypeFactories>());
}
} // namespace facebook::velox
