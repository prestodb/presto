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

#include "velox/expression/TypeSignature.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec {
void toAppend(
    const facebook::velox::exec::TypeSignature& signature,
    std::string* result) {
  result->append(signature.toString());
}

std::string TypeSignature::toString() const {
  std::ostringstream out;
  if (rowFieldName_.has_value()) {
    out << *rowFieldName_ << " ";
  }
  out << baseName_;
  if (!parameters_.empty()) {
    out << "(" << folly::join(",", parameters_) << ")";
  }
  return out.str();
}

} // namespace facebook::velox::exec
