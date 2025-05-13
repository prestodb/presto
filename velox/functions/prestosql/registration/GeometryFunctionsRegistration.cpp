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

#include <string>
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/GeometryFunctions.h"
#include "velox/functions/prestosql/types/GeometryRegistration.h"

namespace facebook::velox::functions {
void registerGeometryFunctions(const std::string& prefix) {
  registerGeometryType();
  registerFunction<StGeometryFromTextFunction, Geometry, Varchar>(
      {{prefix + "ST_GeometryFromText"}});

  registerFunction<StGeomFromBinaryFunction, Geometry, Varbinary>(
      {{prefix + "ST_GeomFromBinary"}});

  registerFunction<StAsTextFunction, Varchar, Geometry>(
      {{prefix + "ST_AsText"}});

  registerFunction<StAsBinaryFunction, Varbinary, Geometry>(
      {{prefix + "ST_AsBinary"}});
}

} // namespace facebook::velox::functions
