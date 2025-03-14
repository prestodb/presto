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

#include "velox/common/base/Status.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/types/BingTileType.h"

namespace facebook::velox::functions {

template <typename T>
struct BingTileFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<BingTile>& result,
      const arg_type<int32_t>& x,
      const arg_type<int32_t>& y,
      const arg_type<int8_t>& zoom) {
    uint64_t tile = BingTileType::bingTileCoordsToInt(
        static_cast<uint32_t>(x),
        static_cast<uint32_t>(y),
        static_cast<uint8_t>(zoom));
    if (FOLLY_UNLIKELY(!BingTileType::isBingTileIntValid(tile))) {
      std::optional<std::string> reason =
          BingTileType::bingTileInvalidReason(tile);
      if (reason.has_value()) {
        return Status::UserError(reason.value());
      } else {
        return Status::UnknownError(fmt::format(
            "Velox Error constructing BingTile from x {} y {} zoom {}; please report this.",
            x,
            y,
            zoom));
      }
    }
    result = tile;
    return Status::OK();
  }
};

template <typename T>
struct BingTileZoomLevelFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<int8_t>& result,
      const arg_type<BingTile>& tile) {
    uint64_t tileInt = tile;
    result = BingTileType::bingTileZoom(tileInt);
  }
};

template <typename T>
struct BingTileCoordinatesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t>>& result,
      const arg_type<BingTile>& tile) {
    uint64_t tileInt = tile;
    result = std::make_tuple(
        static_cast<int32_t>(BingTileType::bingTileX(tileInt)),
        static_cast<int32_t>(BingTileType::bingTileY(tileInt)));
  }
};

} // namespace facebook::velox::functions
