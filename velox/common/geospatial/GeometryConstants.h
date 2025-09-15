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

#include <cstdint>

// This file contains constats for working with geospatial queries.
// They _must not_ require the GEOS library (or any 3p library).

namespace facebook::velox::common::geospatial {

enum class GeometrySerializationType : uint8_t {
  POINT = 0,
  MULTI_POINT = 1,
  LINE_STRING = 2,
  MULTI_LINE_STRING = 3,
  POLYGON = 4,
  MULTI_POLYGON = 5,
  GEOMETRY_COLLECTION = 6,
  ENVELOPE = 7
};

enum class EsriShapeType : uint32_t {
  POINT = 1,
  POLYLINE = 3,
  POLYGON = 5,
  MULTI_POINT = 8
};

} // namespace facebook::velox::common::geospatial
