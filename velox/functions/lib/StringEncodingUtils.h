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

#include "velox/functions/lib/string/StringCore.h"

namespace facebook::velox::functions {

using namespace stringCore;

/// Return the string encoding of a vector, if not set UTF8 is returned
static StringEncodingMode getStringEncodingOrUTF8(
    BaseVector* vector,
    const SelectivityVector& rows) {
  if (auto simpleVector = vector->template as<SimpleVector<StringView>>()) {
    auto ascii = simpleVector->isAscii(rows);
    return ascii && ascii.value() ? StringEncodingMode::ASCII
                                  : StringEncodingMode::UTF8;
  }
  VELOX_UNREACHABLE();
  return StringEncodingMode::UTF8;
}

/// Wrap an input function with the appropriate string encoding instantiation.
/// Func is a struct templated on StringEncodingMode with a static function
/// apply. The wrapper will call Func::apply<EncodingMode> with the correct
/// StringEncodingMode instantiation.
template <template <StringEncodingMode> typename Func>
struct StringEncodingTemplateWrapper {
  template <typename... Params>
  static void apply(const StringEncodingMode mode, Params... args) {
    switch (mode) {
      case StringEncodingMode::UTF8:
        Func<StringEncodingMode::UTF8>::apply(args...);
        return;

      case StringEncodingMode::ASCII:
        Func<StringEncodingMode::ASCII>::apply(args...);
        return;

      case StringEncodingMode::MOSTLY_ASCII:
        Func<StringEncodingMode::MOSTLY_ASCII>::apply(args...);
        return;
    }
  }
};

} // namespace facebook::velox::functions
