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

#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/common/IntDecoder.h"
#include "velox/dwio/dwrf/common/RLEv1.h"
#include "velox/dwio/dwrf/common/RLEv2.h"

namespace facebook::velox::dwrf {
/**
 * Create an RLE decoder.
 * @param input the input stream to read from
 * @param version version of RLE decoding to do
 * @param pool memory pool to use for allocation
 */
template <bool isSigned>
std::unique_ptr<dwio::common::IntDecoder<isSigned>> createRleDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    RleVersion version,
    memory::MemoryPool& pool,
    bool useVInts,
    uint32_t numBytes) {
  switch (static_cast<int64_t>(version)) {
    case RleVersion_1:
      return std::make_unique<RleDecoderV1<isSigned>>(
          std::move(input), useVInts, numBytes);
    case RleVersion_2:
      return std::make_unique<RleDecoderV2<isSigned>>(std::move(input), pool);
    default:
      DWIO_ENSURE(false, "not supported");
      return {};
  }
}

/**
 * Create a direct decoder
 */
template <bool isSigned>
std::unique_ptr<dwio::common::IntDecoder<isSigned>> createDirectDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    bool useVInts,
    uint32_t numBytes) {
  return std::make_unique<dwio::common::DirectDecoder<isSigned>>(
      std::move(input), useVInts, numBytes);
}

} // namespace facebook::velox::dwrf
