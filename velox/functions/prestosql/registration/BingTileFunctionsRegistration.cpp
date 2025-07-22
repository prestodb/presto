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
#include "velox/functions/prestosql/BingTileFunctions.h"
#include "velox/functions/prestosql/types/BingTileRegistration.h"
#include "velox/functions/prestosql/types/BingTileType.h"
#include "velox/type/SimpleFunctionApi.h"

namespace facebook::velox::functions {

namespace {

void registerSimpleBingTileFunctions(const std::string& prefix) {
  // BingTile constructors
  registerFunction<BingTileFunction, BingTile, int32_t, int32_t, int32_t>(
      {prefix + "bing_tile"});
  registerFunction<BingTileFunction, BingTile, Varchar>({prefix + "bing_tile"});

  // BingTile accessors
  registerFunction<BingTileZoomLevelFunction, int8_t, BingTile>(
      {prefix + "bing_tile_zoom_level"});
  registerFunction<
      BingTileCoordinatesFunction,
      Row<int32_t, int32_t>,
      BingTile>({prefix + "bing_tile_coordinates"});

  // Parent/child tiles
  registerFunction<BingTileParentFunction, BingTile, BingTile>(
      {prefix + "bing_tile_parent"});
  registerFunction<BingTileParentFunction, BingTile, BingTile, int8_t>(
      {prefix + "bing_tile_parent"});
  registerFunction<BingTileParentFunction, BingTile, BingTile, int32_t>(
      {prefix + "bing_tile_parent"});
  registerFunction<BingTileChildrenFunction, Array<BingTile>, BingTile>(
      {prefix + "bing_tile_children"});
  registerFunction<
      BingTileChildrenFunction,
      Array<BingTile>,
      BingTile,
      int32_t>({prefix + "bing_tile_children"});
  registerFunction<BingTileToQuadKeyFunction, Varchar, BingTile>(
      {prefix + "bing_tile_quadkey"});
  registerFunction<BingTileAtFunction, BingTile, double, double, int32_t>(
      {prefix + "bing_tile_at"});
  registerFunction<
      BingTilesAroundFunction,
      Array<BingTile>,
      double,
      double,
      int32_t>({prefix + "bing_tiles_around"});
  registerFunction<
      BingTilesAroundFunction,
      Array<BingTile>,
      double,
      double,
      int32_t,
      double>({prefix + "bing_tiles_around"});
}

} // namespace

void registerBingTileFunctions(const std::string& prefix) {
  registerBingTileType();

  registerSimpleBingTileFunctions(prefix);
}

} // namespace facebook::velox::functions
