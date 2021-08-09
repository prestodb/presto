/*
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

#include "velox/dwio/dwrf/common/IntEncoder.h"
#include "velox/dwio/dwrf/common/RLEv1.h"

namespace facebook::velox::dwrf {

template <bool isSigned>
uint64_t IntEncoder<isSigned>::flush() {
  output_->BackUp(bufferLength_ - bufferPosition_);
  auto dataSize = output_->flush();
  bufferLength_ = bufferPosition_ = 0;
  return dataSize;
}

template uint64_t IntEncoder<true>::flush();
template uint64_t IntEncoder<false>::flush();

template <bool isSigned>
std::unique_ptr<IntEncoder<isSigned>> IntEncoder<isSigned>::createRle(
    RleVersion version,
    std::unique_ptr<BufferedOutputStream> output,
    bool useVInts,
    uint32_t numBytes) {
  switch (static_cast<int64_t>(version)) {
    case RleVersion_1:
      return std::make_unique<RleEncoderV1<isSigned>>(
          std::move(output), useVInts, numBytes);
    case RleVersion_2:
    default:
      DWIO_ENSURE(false, "not supported");
      return {};
  }
}

template std::unique_ptr<IntEncoder<true>> IntEncoder<true>::createRle(
    RleVersion version,
    std::unique_ptr<BufferedOutputStream> output,
    bool useVInts,
    uint32_t numBytes);
template std::unique_ptr<IntEncoder<false>> IntEncoder<false>::createRle(
    RleVersion version,
    std::unique_ptr<BufferedOutputStream> output,
    bool useVInts,
    uint32_t numBytes);

template <bool isSigned>
std::unique_ptr<IntEncoder<isSigned>> IntEncoder<isSigned>::createDirect(
    std::unique_ptr<BufferedOutputStream> output,
    bool useVInts,
    uint32_t numBytes) {
  return std::make_unique<IntEncoder<isSigned>>(
      std::move(output), useVInts, numBytes);
}

template std::unique_ptr<IntEncoder<true>> IntEncoder<true>::createDirect(
    std::unique_ptr<BufferedOutputStream> output,
    bool useVInts,
    uint32_t numBytes);
template std::unique_ptr<IntEncoder<false>> IntEncoder<false>::createDirect(
    std::unique_ptr<BufferedOutputStream> output,
    bool useVInts,
    uint32_t numBytes);

} // namespace facebook::velox::dwrf
