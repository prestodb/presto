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
#include "velox/functions/lib/string/StringCore.h"

namespace facebook::velox::functions {

namespace {

/// Given the input vectors determines ascii mode to be used. Returns
/// ASSUME_TRUE if all input vectors are ascii otherwise returns MAYBE
template <typename BaseVectorType>
AsciiMode getAsciiMode(const BaseVectorType* vector) {
  if (auto simpleVector = vector->template as<SimpleVector<StringView>>()) {
    if (simpleVector->isAscii()) {
      return AsciiMode::ASSUME_TRUE;
    }
  }
  return AsciiMode::MAYBE;
}

template <typename BaseVectorType, typename... REST>
AsciiMode getAsciiMode(const BaseVectorType* vector, REST... rest) {
  if (auto simpleVector = vector->template as<SimpleVector<StringView>>()) {
    if (simpleVector->isAscii()) {
      return getAsciiMode(rest...);
    }
  }
  return AsciiMode::MAYBE;
}

/// Wrap an input function with the appropriate AsciiMode instantiation.
/// Func is a struct templated on AsciiMode with a static function apply. It
/// will call Func::apply<AsciiMode> with the correct AsciiMode instantiation.
template <template <AsciiMode> typename Func>
struct AsciiModeTemplateWrapper {
  template <typename... Params>
  static void apply(const AsciiMode mode, Params... args) {
    switch (mode) {
      case AsciiMode::ASSUME_FALSE:
        Func<AsciiMode::ASSUME_FALSE>::apply(args...);
        return;

      case AsciiMode::ASSUME_TRUE:
        Func<AsciiMode::ASSUME_TRUE>::apply(args...);
        return;

      case AsciiMode::MAYBE:
        Func<AsciiMode::MAYBE>::apply(args...);
        return;
    }
  }
};

} // namespace
} // namespace facebook::velox::functions
