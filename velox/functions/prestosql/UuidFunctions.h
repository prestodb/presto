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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/types/UuidRegistration.h"
#include "velox/functions/prestosql/types/UuidType.h"

namespace facebook::velox::functions {

#define VELOX_GEN_BINARY_EXPR_UUID(Name, uuidCompExpr)                         \
  template <typename T>                                                        \
  struct Name##Uuid {                                                          \
    VELOX_DEFINE_FUNCTION_TYPES(T);                                            \
                                                                               \
    FOLLY_ALWAYS_INLINE void                                                   \
    call(bool& result, const arg_type<Uuid>& lhs, const arg_type<Uuid>& rhs) { \
      result = (uuidCompExpr);                                                 \
    }                                                                          \
  };

VELOX_GEN_BINARY_EXPR_UUID(LtFunction, (uint128_t)lhs < (uint128_t)rhs);
VELOX_GEN_BINARY_EXPR_UUID(GtFunction, (uint128_t)lhs > (uint128_t)rhs);
VELOX_GEN_BINARY_EXPR_UUID(LteFunction, (uint128_t)lhs <= (uint128_t)rhs);
VELOX_GEN_BINARY_EXPR_UUID(GteFunction, (uint128_t)lhs >= (uint128_t)rhs);

#undef VELOX_GEN_BINARY_EXPR_UUID

template <typename T>
struct BetweenFunctionUuid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      bool& result,
      const arg_type<Uuid>& value,
      const arg_type<Uuid>& low,
      const arg_type<Uuid>& high) {
    auto castValue = static_cast<uint128_t>(value);
    result = castValue >= static_cast<uint128_t>(low) &&
        castValue <= static_cast<uint128_t>(high);
  }
};

template <typename T>
struct UuidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(int128_t& result) {
    boost::uuids::uuid uuid = generator_();
    memcpy(&result, uuid.data, 16);
  }

 private:
  boost::uuids::random_generator generator_;
};

inline void registerUuidFunctions(const std::string& prefix) {
  registerUuidType();
  registerFunction<UuidFunction, Uuid>({prefix + "uuid"});
  registerFunction<LtFunctionUuid, bool, Uuid, Uuid>({prefix + "lt"});
  registerFunction<GtFunctionUuid, bool, Uuid, Uuid>({prefix + "gt"});
  registerFunction<LteFunctionUuid, bool, Uuid, Uuid>({prefix + "lte"});
  registerFunction<GteFunctionUuid, bool, Uuid, Uuid>({prefix + "gte"});
  registerFunction<BetweenFunctionUuid, bool, Uuid, Uuid, Uuid>(
      {prefix + "between"});
}

} // namespace facebook::velox::functions
