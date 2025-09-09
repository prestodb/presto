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

namespace facebook::velox {

class P4HyperLogLogType final : public VarbinaryType {
  P4HyperLogLogType() = default;

 public:
  static std::shared_ptr<const P4HyperLogLogType> get() {
    VELOX_CONSTEXPR_SINGLETON P4HyperLogLogType kInstance;
    return {std::shared_ptr<const P4HyperLogLogType>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "P4HYPERLOGLOG";
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

  bool isOrderable() const override {
    return false;
  }
};

inline bool isP4HyperLogLogType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return P4HyperLogLogType::get() == type;
}

inline std::shared_ptr<const P4HyperLogLogType> P4HYPERLOGLOG() {
  return P4HyperLogLogType::get();
}

// Type to use for inputs and outputs of simple functions, e.g.
// arg_type<P4HyperLogLog> and out_type<P4HyperLogLog>.
struct P4HyperLogLogT {
  using type = Varbinary;
  static constexpr const char* typeName = "p4hyperloglog";
};

using P4HyperLogLog = CustomType<P4HyperLogLogT>;

} // namespace facebook::velox
