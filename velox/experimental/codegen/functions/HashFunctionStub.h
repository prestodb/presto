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

#include <iostream>
#include "velox/experimental/codegen/vector_function/StringTypes.h"
#include "velox/functions/prestosql/HashImpl.h"
#include "velox/type/Conversions.h"
#include "velox/type/Type.h"

#ifndef FOLLY_HAS_STRING_VIEW
namespace folly {
template <>
struct hasher<std::string_view> {
  using folly_is_avalanching = std::true_type;

  size_t operator()(const std::string_view& key) const {
    return static_cast<size_t>(
        hash::SpookyHashV2::Hash64(key.data(), key.size(), 0));
  }
};
} // namespace folly
#endif

namespace facebook::velox::codegen {

template <typename T>
FOLLY_ALWAYS_INLINE int64_t computeHashStub(const T& arg) {
  if constexpr (std::is_same_v<codegen::TempString<TempsAllocator>, T>) {
    //   std::cerr << std::string_view(arg.data(), arg.size());
    return functions::computeHash(std::string_view(arg.data(), arg.size()));
  } else {
    return functions::computeHash(arg);
  }
}

}; // namespace facebook::velox::codegen
