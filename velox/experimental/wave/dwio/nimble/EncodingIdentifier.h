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

#include <cstdint>

namespace facebook::wave::nimble {

// When encoding contains nested encodings, each nested encoding has its
// own identifier.
using NestedEncodingIdentifier = uint8_t;

struct EncodingIdentifiers {
  struct Dictionary {
    static constexpr NestedEncodingIdentifier Alphabet = 0;
    static constexpr NestedEncodingIdentifier Indices = 1;
  };

  struct MainlyConstant {
    static constexpr NestedEncodingIdentifier IsCommon = 0;
    static constexpr NestedEncodingIdentifier OtherValues = 1;
  };

  struct Nullable {
    static constexpr NestedEncodingIdentifier Data = 0;
    static constexpr NestedEncodingIdentifier Nulls = 1;
  };

  struct RunLength {
    static constexpr NestedEncodingIdentifier RunLengths = 0;
    static constexpr NestedEncodingIdentifier RunValues = 1;
  };

  struct SparseBool {
    static constexpr NestedEncodingIdentifier Indices = 0;
  };

  struct Trivial {
    static constexpr NestedEncodingIdentifier Lengths = 0;
  };
};

} // namespace facebook::wave::nimble
