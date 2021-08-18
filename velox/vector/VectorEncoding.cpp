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

#include "velox/vector/VectorEncoding.h"

#include <stdexcept>

#include <fmt/core.h>
#include <folly/container/F14Map.h>

namespace facebook::velox::VectorEncoding {

Simple mapNameToSimple(const std::string& name) {
  static folly::F14FastMap<std::string, Simple> vecNameMap{
      {"BIASED", Simple::BIASED},
      {"CONSTANT", Simple::CONSTANT},
      {"DICTIONARY", Simple::DICTIONARY},
      {"FLAT", Simple::FLAT},
      {"SEQUENCE", Simple::SEQUENCE},
      {"ROW", Simple::ROW},
      {"MAP", Simple::MAP},
      {"ARRAY", Simple::ARRAY}};

  if (vecNameMap.find(name) == vecNameMap.end()) {
    throw std::invalid_argument(
        fmt::format("Specified vector encoding is not found : {}", name));
  }
  return vecNameMap[name];
}
} // namespace facebook::velox::VectorEncoding
