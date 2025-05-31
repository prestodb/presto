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
#include <geos/util/AssertionFailedException.h>
#include <geos/util/UnsupportedOperationException.h>
#include <optional>

#include "velox/common/base/Status.h"

namespace facebook::velox::functions::geospatial {

/// Utility macro used to wrap GEOS library calls in a try-catch block,
/// returning a velox::Status with error message if an exception is caught.
#define GEOS_TRY(func, user_error_message)                       \
  try {                                                          \
    func                                                         \
  } catch (const geos::util::UnsupportedOperationException& e) { \
    return Status::UnknownError(                                 \
        fmt::format("Internal geometry error: {}", e.what()));   \
  } catch (const geos::util::AssertionFailedException& e) {      \
    return Status::UnknownError(                                 \
        fmt::format("Internal geometry error: {}", e.what()));   \
  } catch (const geos::util::GEOSException& e) {                 \
    return Status::UserError(                                    \
        fmt::format("{}: {}", user_error_message, e.what()));    \
  }

std::optional<std::string> geometryInvalidReason(
    const geos::geom::Geometry* geometry);

} // namespace facebook::velox::functions::geospatial
