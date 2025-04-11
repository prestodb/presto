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

#include "velox/common/compression/HadoopCompressionFormat.h"
#include "velox/common/base/Exceptions.h"

#include <folly/lang/Bits.h>

namespace facebook::velox::common {

bool HadoopCompressionFormat::tryDecompressHadoop(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength,
    uint64_t& actualDecompressedSize) {
  uint64_t totalDecompressedSize = 0;

  while (inputLength >= kPrefixLength) {
    const uint32_t expectedDecompressedSize =
        folly::Endian::big(folly::loadUnaligned<uint32_t>(input));
    const uint32_t expectedCompressedSize = folly::Endian::big(
        folly::loadUnaligned<uint32_t>(input + sizeof(uint32_t)));
    input += kPrefixLength;
    inputLength -= kPrefixLength;

    if (inputLength < expectedCompressedSize) {
      // Not enough bytes for Hadoop "frame".
      return false;
    }
    if (outputLength < expectedDecompressedSize) {
      // Not enough bytes to hold advertised output => probably not Hadoop.
      return false;
    }
    // Try decompressing and compare with expected decompressed length.
    auto maybeDecompressedSize =
        decompressInternal(input, expectedCompressedSize, output, outputLength);
    if (maybeDecompressedSize.hasError() ||
        maybeDecompressedSize.value() != expectedDecompressedSize) {
      return false;
    }

    input += expectedCompressedSize;
    inputLength -= expectedCompressedSize;
    output += expectedDecompressedSize;
    outputLength -= expectedDecompressedSize;
    totalDecompressedSize += expectedDecompressedSize;
  }

  if (inputLength == 0) {
    actualDecompressedSize = totalDecompressedSize;
    return true;
  }
  return false;
}
} // namespace facebook::velox::common
