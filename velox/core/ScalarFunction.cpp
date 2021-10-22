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
#include "velox/core/ScalarFunction.h"

namespace facebook {
namespace velox {
namespace core {

FunctionKey IScalarFunction::key() const {
  return FunctionKey{getName(), argTypes()};
}

std::string IScalarFunction::signature(const std::string& name) const {
  std::string s{name};
  s.append("(");
  bool first = true;
  for (auto& arg : argTypes()) {
    if (!first) {
      s.append(", ");
    }
    first = false;
    s.append(arg->toString());
  }
  s.append(")");
  return s;
}

} // namespace core
} // namespace velox
} // namespace facebook
