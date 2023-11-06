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

#include "velox/experimental/codegen/vector_function/StringTypes.h"
#include "velox/type/Conversions.h"
#include "velox/type/Type.h"

namespace facebook::velox::codegen {

template <TypeKind kind, bool castByTruncate>
struct CodegenConversionStub {
  template <typename T>
  using ReturnType = typename TypeTraits<kind>::DeepCopiedType;

  template <typename T>
  static ReturnType<T> cast(const T& arg) {
    if constexpr (std::is_same_v<codegen::TempString<TempsAllocator>, T>) {
      return util::Converter<kind, void, castByTruncate>::cast(
          folly::StringPiece(arg.data(), arg.size()));
    } else {
      return util::Converter<kind, void, castByTruncate>::cast(arg);
    }
  }
};

}; // namespace facebook::velox::codegen
