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

#include <lz4.h>
#include <lz4frame.h>
#include <lz4hc.h>
#include <memory>
#include "velox/common/compression/Compression.h"
#include "velox/common/compression/HadoopCompressionFormat.h"

namespace facebook::velox::common {

struct Lz4CodecOptions : CodecOptions {
  enum Lz4Type { kLz4Frame, kLz4Raw, kLz4Hadoop };

  Lz4CodecOptions(
      Lz4Type lz4Type,
      int32_t compressionLevel = kDefaultCompressionLevel)
      : CodecOptions(compressionLevel), lz4Type(lz4Type) {}

  Lz4Type lz4Type;
};

// Lz4 frame format codec.
std::unique_ptr<Codec> makeLz4FrameCodec(
    int32_t compressionLevel = kDefaultCompressionLevel);

// Lz4 "raw" format codec.
std::unique_ptr<Codec> makeLz4RawCodec(
    int32_t compressionLevel = kDefaultCompressionLevel);

// Lz4 "Hadoop" format codec (Lz4 raw codec prefixed with lengths header).
std::unique_ptr<Codec> makeLz4HadoopCodec();
} // namespace facebook::velox::common
