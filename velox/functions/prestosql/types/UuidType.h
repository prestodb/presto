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

#include "velox/expression/CastExpr.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {

/// Represents a UUID (Universally Unique IDentifier), also known as a
/// GUID (Globally Unique IDentifier), using the format defined in :rfc:`4122`.
///
/// Example: UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'
class UuidType : public HugeintType {
  UuidType() = default;

 public:
  static const std::shared_ptr<const UuidType>& get() {
    static const std::shared_ptr<const UuidType> instance{new UuidType()};

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "UUID";
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

FOLLY_ALWAYS_INLINE bool isUuidType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return UuidType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const UuidType> UUID() {
  return UuidType::get();
}

// Type used for function registration.
struct UuidT {
  using type = int128_t;
  static constexpr const char* typeName = "uuid";
};

using Uuid = CustomType<UuidT>;

void registerUuidType();

} // namespace facebook::velox
