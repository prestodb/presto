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

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/EnumFunctions.h"
#include "velox/functions/prestosql/types/BigintEnumRegistration.h"
#include "velox/functions/prestosql/types/VarcharEnumRegistration.h"
#include "velox/type/SimpleFunctionApi.h"

namespace facebook::velox::functions {

void registerEnumFunctions(const std::string& prefix) {
  registerBigintEnumType();
  registerVarcharEnumType();

  registerFunction<
      ParameterBinder<EnumKeyFunction, BigintEnumTypePtr>,
      Varchar,
      BigintEnum<E1>>({prefix + "enum_key"});
  registerFunction<
      ParameterBinder<EnumKeyFunction, VarcharEnumTypePtr>,
      Varchar,
      VarcharEnum<E1>>({prefix + "enum_key"});
}
} // namespace facebook::velox::functions
