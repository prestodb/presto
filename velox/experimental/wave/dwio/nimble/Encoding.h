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

namespace facebook::wave::nimble {
class Encoding {
 public:
  // The binary layout for each Encoding begins with the same prefix:
  // 1 byte: EncodingType
  // 1 byte: DataType
  // 4 bytes: uint32_t num rows
  static constexpr int kEncodingTypeOffset = 0;
  static constexpr int kDataTypeOffset = 1;
  static constexpr int kRowCountOffset = 2;
  static constexpr int kPrefixSize = 6;
};
} // namespace facebook::wave::nimble
