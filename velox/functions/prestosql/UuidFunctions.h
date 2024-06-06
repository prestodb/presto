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
#include "velox/functions/prestosql/types/UuidType.h"

namespace facebook::velox::functions {

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
}

} // namespace facebook::velox::functions
