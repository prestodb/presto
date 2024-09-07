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

#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/dwio/dwrf/common/ByteRLE.h"

namespace facebook::velox::dwrf {

/// Struct containing context for flatmap encoding.
/// Default initialization is non-flatmap context.
struct FlatMapContext {
 public:
  uint32_t sequence{0};

  // Kept alive by key nodes
  BooleanRleDecoder* inMapDecoder{nullptr};

  std::function<void(
      facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats)>
      keySelectionCallback{nullptr};
};

} // namespace facebook::velox::dwrf
