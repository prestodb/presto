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

#include <folly/Expected.h>
#include <cstdint>
#include "velox/common/base/Status.h"

namespace facebook::velox::common {

/// Parquet files written with the Hadoop compression codecs use their own
/// framing.
/// The input buffer can contain an arbitrary number of "frames", each
/// with the following structure:
/// - bytes 0..3: big-endian uint32_t representing the frame decompressed
/// size
/// - bytes 4..7: big-endian uint32_t representing the frame compressed size
/// - bytes 8...: frame compressed data
class HadoopCompressionFormat {
 public:
  virtual ~HadoopCompressionFormat() {}

 protected:
  /// Try to decompress input data in Hadoop's compression format.
  /// Returns true if decompression is successful, false otherwise.
  bool tryDecompressHadoop(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength,
      uint64_t& actualDecompressedSize);

  /// Called by tryDecompressHadoop to decompress a single frame and
  /// should be implemented based on the specific compression format.
  /// E.g. Lz4HadoopCodec uses Lz4RawCodec::decompress to decompress a frame.
  virtual Expected<uint64_t> decompressInternal(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) = 0;

  // Offset starting at which page data can be read/written.
  static constexpr uint64_t kPrefixLength = sizeof(uint32_t) * 2;
};
} // namespace facebook::velox::common
