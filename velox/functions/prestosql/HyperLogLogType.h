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

#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/type/Type.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

class HyperLogLogType : public VarbinaryType {
  HyperLogLogType() = default;

 public:
  static const std::shared_ptr<const HyperLogLogType>& get() {
    static const std::shared_ptr<const HyperLogLogType> instance =
        std::shared_ptr<HyperLogLogType>(new HyperLogLogType());

    return instance;
  }

  std::string toString() const override {
    return "HYPERLOGLOG";
  }
};

inline bool isHyperLogLogType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return HyperLogLogType::get() == type;
}

inline std::shared_ptr<const HyperLogLogType> HYPERLOGLOG() {
  return HyperLogLogType::get();
}

// Type used for function registration.
struct HyperLogLog {
 private:
  HyperLogLog() {}
};

// Template specializations for the type defined.
template <>
struct CppToType<HyperLogLog> {
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = false;
  static constexpr TypeKind typeKind = TypeKind::VARBINARY;

  static auto create() {
    return HyperLogLogType::get();
  }
};

// What kind of vector should be used underneath to store this type.
template <>
struct TypeToFlatVector<HyperLogLog> {
  using type = FlatVector<StringView>;
};

namespace exec {
namespace detail {

// Input and output arg mapping for simple functions.
template <>
struct resolver<HyperLogLog> {
  using in_type = resolver<Varbinary>::in_type;
  using out_type = resolver<Varbinary>::out_type;
};

} // namespace detail
} // namespace exec
} // namespace facebook::velox
