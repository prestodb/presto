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
  TDigestType() = default;

 public:
  static const std::shared_ptr<const TDigestType>& get() {
    static const std::shared_ptr<const TDigestType> instance =
        std::shared_ptr<TDigestType>(new TDigestType());

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "TDIGEST";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }
};

inline bool isTDigestType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return TDigestType::get() == type;
}

inline std::shared_ptr<const TDigestType> TDIGEST() {
  return TDigestType::get();
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
  TypePtr getType() const override {
    return TDIGEST();
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
