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

#include "velox/python/vector/PyVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorPrinter.h"

namespace facebook::velox::py {

std::string PyVector::summarizeToText() const {
  return velox::VectorPrinter::summarizeToText(*vector_);
}

std::string PyVector::printDetailed() const {
  return velox::printVector(*vector_);
}

PyVector PyVector::childAt(vector_size_t idx) const {
  if (auto rowVector = std::dynamic_pointer_cast<RowVector>(vector_)) {
    return PyVector{rowVector->childAt(idx), pool_};
  }
  throw std::runtime_error(fmt::format(
      "Can only call child_at() on RowVector, but got '{}'", toString()));
}

} // namespace facebook::velox::py
