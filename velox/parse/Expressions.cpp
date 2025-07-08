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
#include "velox/parse/Expressions.h"

namespace facebook::velox::core {

namespace {

std::string escapeName(const std::string& name) {
  return folly::cEscape<std::string>(name);
}
} // namespace

std::string FieldAccessExpr::toString() const {
  if (isRootColumn()) {
    return appendAliasIfExists(fmt::format("\"{}\"", escapeName(name_)));
  }

  return appendAliasIfExists(
      fmt::format("dot({},\"{}\")", input()->toString(), escapeName(name_)));
}

std::string CallExpr::toString() const {
  std::string buf{name_ + "("};
  bool first = true;
  for (auto& f : inputs()) {
    if (!first) {
      buf += ",";
    }
    buf += f->toString();
    first = false;
  }
  buf += ")";
  return appendAliasIfExists(buf);
}

std::string CastExpr::toString() const {
  return appendAliasIfExists(
      "cast(" + input()->toString() + ", " + type_->toString() + ")");
}

std::string LambdaExpr::toString() const {
  std::ostringstream out;

  if (arguments_.size() > 1) {
    out << "(" << folly::join(", ", arguments_) << ")";
  } else {
    out << arguments_[0];
  }
  out << " -> " << body_->toString();
  return out.str();
}

} // namespace facebook::velox::core
