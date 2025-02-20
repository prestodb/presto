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

#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

class TDigestType : public VarbinaryType {
 public:
  static const std::shared_ptr<const TDigestType>& get(
      const TypePtr& dataType) {
    // Only TDIGEST(DOUBLE) exists in Presto so we use a singleton to improve
    // performance.
    VELOX_CHECK(dataType->isDouble());
    static const auto instance =
        std::shared_ptr<const TDigestType>(new TDigestType(DOUBLE()));
    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "TDIGEST";
  }

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

  std::string toString() const override {
    return fmt::format("TDIGEST({})", parameters_[0].type->toString());
  }

  folly::dynamic serialize() const override;

 private:
  explicit TDigestType(const TypePtr& dataType)
      : parameters_({TypeParameter(dataType)}) {}

  const std::vector<TypeParameter> parameters_;
};

inline std::shared_ptr<const TDigestType> TDIGEST(const TypePtr& dataType) {
  return TDigestType::get(dataType);
}

// Type to use for inputs and outputs of simple functions, e.g.
// arg_type<TDigest> and out_type<TDigest>.
struct TDigestT {
  using type = Varbinary;
  static constexpr const char* typeName = "tdigest";
};

using TDigest = CustomType<TDigestT>;

class TDigestTypeFactories : public CustomTypeFactories {
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
      const InputGeneratorConfig& /*config*/) const override {
    return nullptr;
  }
};

void registerTDigestType();

} // namespace facebook::velox
