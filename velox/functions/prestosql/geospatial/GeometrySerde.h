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

#include <geos/geom/Geometry.h>
#include "velox/type/StringView.h"

namespace facebook::velox::functions::geospatial {

/// Deserialize Velox's internal format to a geometry.  Do not call this within
/// GEOS_TRY macro: it will catch the exceptions that need to bubble up.
std::unique_ptr<geos::geom::Geometry> deserializeGeometry(
    const StringView& geometryString);

/// Serialize geometry into Velox's internal format.  Do not call this within
/// GEOS_TRY macro: it will catch the exceptions that need to bubble up.
std::string serializeGeometry(const geos::geom::Geometry& geosGeometry);

/// Deserialize Velox's internal format to a geometry and get the Envelope.
const std::unique_ptr<geos::geom::Envelope> getEnvelopeFromGeometry(
    const StringView& geometry);

} // namespace facebook::velox::functions::geospatial
