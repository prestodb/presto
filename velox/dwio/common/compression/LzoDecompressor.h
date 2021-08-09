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

#pragma once

namespace facebook {
namespace dwio {
namespace common {
namespace compression {

/**
 * Decompress the bytes in to the output buffer.
 * @param inputAddress the start of the input
 * @param inputLimit one past the last byte of the input
 * @param outputAddress the start of the output buffer
 * @param outputLimit one past the last byte of the output buffer
 * @result the number of bytes decompressed
 */
uint64_t lzoDecompress(
    const char* inputAddress,
    const char* inputLimit,
    char* outputAddress,
    char* outputLimit);

} // namespace compression
} // namespace common
} // namespace dwio
} // namespace facebook
