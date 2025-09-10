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

#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {

inline std::string_view trimToken(std::string_view token) {
  VELOX_DCHECK(!stringImpl::isAsciiWhiteSpace(token[0]));
  auto size = token.size();
  while (stringImpl::isAsciiWhiteSpace(token[size - 1])) {
    --size;
  }
  return std::string_view(token.data(), size);
}

} // namespace facebook::velox::functions
