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
#include <boost/algorithm/string/replace.hpp>
#include <folly/container/F14Map.h>
#include "velox/type/Tokenizer.h"

namespace facebook::velox::common {

namespace {
const auto& subfieldKindNames() {
  static const folly::F14FastMap<SubfieldKind, std::string_view> kNames = {
      {SubfieldKind::kAllSubscripts, "AllSubscripts"},
      {SubfieldKind::kNestedField, "NestedField"},
      {SubfieldKind::kStringSubscript, "StringSubscript"},
      {SubfieldKind::kLongSubscript, "LongSubscript"},
  };

  return kNames;
}
} // namespace

VELOX_DEFINE_ENUM_NAME(SubfieldKind, subfieldKindNames)

Subfield::Subfield(
    const std::string& path,
    std::shared_ptr<const Separators> separators) {
  Tokenizer tokenizer{path, std::move(separators)};
  VELOX_CHECK(tokenizer.hasNext(), "Column name is missing: {}", path);

  auto firstElement = tokenizer.next();
  VELOX_CHECK_EQ(
      firstElement->kind(),
      SubfieldKind::kNestedField,
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
  VELOX_CHECK_EQ(
      path_[0]->kind(),
      SubfieldKind::kNestedField,
      "Subfield path must start with a name");
}

Subfield Subfield::clone() const {
  Subfield subfield;
  subfield.path_.reserve(path_.size());
  for (auto& element : path_) {
    subfield.path_.push_back(element->clone());
  }
  return subfield;
}

bool Subfield::isPrefix(const Subfield& other) const {
  if (path_.size() < other.path_.size()) {
    for (size_t i = 0; i < path_.size(); ++i) {
      if (!(*path_[i].get() == *other.path_[i].get())) {
        return false;
      }
    }
    return true;
  }
  return false;
}

std::string Subfield::toString() const {
  if (!valid()) {
    return "";
  }
  std::ostringstream out;
  out << static_cast<const NestedField*>(path_[0].get())->name();
  for (size_t i = 1; i < path_.size(); i++) {
    out << path_[i]->toString();
  }
  return out.str();
}

bool Subfield::operator==(const Subfield& other) const {
  if (this == &other) {
    return true;
  }

  if (path_.size() != other.path_.size()) {
    return false;
  }
  for (size_t i = 0; i < path_.size(); ++i) {
    if (!(*path_[i].get() == *other.path_[i].get())) {
      return false;
    }
  }
  return true;
}

size_t Subfield::hash() const {
  size_t result = 1;
  for (size_t i = 0; i < path_.size(); ++i) {
    result = result * 31 + path_[i]->hash();
  }
  return result;
}

std::string Subfield::StringSubscript::toString() const {
  return "[\"" + boost::replace_all_copy(index_, "\"", "\\\"") + "\"]";
}

} // namespace facebook::velox::common
