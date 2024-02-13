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

#include "velox/type/Type.h"

namespace facebook::velox {

namespace exec {
class CastOperator;
}

// Given a class T, create a custom type that can be used to refer to it, with
// an underlying opaque vector type.
// Note that name must be stored in a variable and passed and cant be inlined.
// OpaqueCustomTypeRegister<T, "type"> wont compile.
// but static constexpr char* type = "type", OpaqueCustomTypeRegister<T, type>
// works.
template <typename T, const char* customTypeName>
class OpaqueCustomTypeRegister {
 public:
  static bool registerType() {
    return facebook::velox::registerCustomType(
        customTypeName, std::make_unique<const TypeFactory>());
  }

  // Type used in the simple function interface as CustomType<TypeT>.
  struct TypeT {
    using type = std::shared_ptr<T>;
    static constexpr const char* typeName = customTypeName;
  };

  using SimpleType = CustomType<TypeT>;

  class VeloxType : public OpaqueType {
   public:
    VeloxType() : OpaqueType(std::type_index(typeid(T))) {}

    static const TypePtr& get() {
      static const TypePtr instance{new VeloxType()};
      return instance;
    }

    static const std::shared_ptr<const exec::CastOperator>& getCastOperator() {
      VELOX_UNSUPPORTED();
    }

    bool equivalent(const velox::Type& other) const override {
      // Pointer comparison works since this type is a singleton.
      return this == &other;
    }

    const char* name() const override {
      return customTypeName;
    }

    std::string toString() const override {
      return customTypeName;
    }
  };

  static const TypePtr& singletonTypePtr() {
    return VeloxType::get();
  }

 private:
  class TypeFactory : public CustomTypeFactories {
   public:
    TypeFactory() = default;

    TypePtr getType() const override {
      return singletonTypePtr();
    }

    exec::CastOperatorPtr getCastOperator() const override {
      VELOX_UNSUPPORTED();
    }
  };
};
} // namespace facebook::velox
