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

#include "velox/expression/VectorFunction.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::functions {

/// Helper function that prepares a string result vector and initializes it.
/// It will use the input argToReuse vector instead of creating new one when
/// possible. Returns true if argToReuse vector was moved to results
bool prepareFlatResultsVector(
    VectorPtr& result,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    VectorPtr& argToReuse);

/// Return the string encoding of a vector, if not set UTF8 is returned
static bool isAscii(BaseVector* vector, const SelectivityVector& rows) {
  if (auto simpleVector = vector->template as<SimpleVector<StringView>>()) {
    auto ascii = simpleVector->isAscii(rows);
    return ascii.has_value() && ascii.value();
  }
  VELOX_UNREACHABLE();
  return false;
};

/// Wrap an input function with the appropriate ascii instantiation.
/// Func is a struct templated on boolean with a static function
/// apply. The wrapper will call Func::apply<bool> based on truth value of the
/// boolean.
template <template <bool> typename Func>
struct StringEncodingTemplateWrapper {
  template <typename... Params>
  static void apply(const bool isAscii, Params... args) {
    if (isAscii) {
      Func<true>::apply(args...);
    } else {
      Func<false>::apply(args...);
    }
  }
};

} // namespace facebook::velox::functions
