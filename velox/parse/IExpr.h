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

#include "velox/common/base/ClassName.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace facebook {
namespace velox {
namespace core {

/* an implicitly-typed expression, such as function call, literal, etc... */
class IExpr {
 public:
  explicit IExpr(std::optional<std::string> alias = std::nullopt)
      : alias_{std::move(alias)} {}

  virtual const std::vector<std::shared_ptr<const IExpr>>& getInputs()
      const = 0;

  std::shared_ptr<const IExpr> getInput() const {
    return getInputs().size() == 1 ? getInputs().at(0) : nullptr;
  }

  virtual ~IExpr() = default;

  virtual std::string toString() const = 0;

  const std::optional<std::string>& alias() const {
    return alias_;
  }

 protected:
  static const std::vector<std::shared_ptr<const IExpr>>& EMPTY() {
    static const std::vector<std::shared_ptr<const IExpr>> empty{};
    return empty;
  }

  std::string appendAliasIfExists(std::string s) const {
    if (!alias_.has_value()) {
      return s;
    }

    return fmt::format("{} AS {}", std::move(s), alias_.value());
  }

  std::optional<std::string> alias_;
};

} // namespace core
} // namespace velox
} // namespace facebook
