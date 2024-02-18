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
#include "velox/type/Subfield.h"
#include "velox/type/Tokenizer.h"

namespace facebook::velox::common {

Subfield::Subfield(
    const std::string& path,
    const std::shared_ptr<Separators>& separators) {
  Tokenizer tokenizer(path, separators);
  VELOX_CHECK(tokenizer.hasNext(), "Column name is missing: {}", path);

  auto firstElement = tokenizer.next();
  VELOX_CHECK(
      firstElement->kind() == kNestedField,
      "Subfield path must start with a name: {}",
      path);
  std::vector<std::unique_ptr<PathElement>> pathElements;
  pathElements.push_back(std::move(firstElement));
  while (tokenizer.hasNext()) {
    pathElements.push_back(tokenizer.next());
  }
  path_ = std::move(pathElements);
}

Subfield::Subfield(std::vector<std::unique_ptr<Subfield::PathElement>>&& path)
    : path_(std::move(path)) {
  VELOX_CHECK_GE(path_.size(), 1);
}

Subfield Subfield::clone() const {
  Subfield subfield;
  subfield.path_.reserve(path_.size());
  for (auto& element : path_) {
    subfield.path_.push_back(element->clone());
  }
  return subfield;
}

} // namespace facebook::velox::common
