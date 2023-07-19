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

#include "velox/dwio/common/Reader.h"

namespace facebook::velox::dwio::common {

VectorPtr RowReader::projectColumns(
    const VectorPtr& input,
    const velox::common::ScanSpec& spec) {
  auto* inputRow = input->as<RowVector>();
  VELOX_CHECK_NOT_NULL(inputRow);
  auto& inputRowType = input->type()->asRow();
  column_index_t numColumns = 0;
  for (auto& childSpec : spec.children()) {
    numColumns = std::max(numColumns, childSpec->channel() + 1);
  }
  std::vector<std::string> names(numColumns);
  std::vector<TypePtr> types(numColumns);
  std::vector<VectorPtr> children(numColumns);
  for (auto& childSpec : spec.children()) {
    if (!childSpec->projectOut()) {
      continue;
    }
    auto i = childSpec->channel();
    names[i] = childSpec->fieldName();
    if (childSpec->isConstant()) {
      children[i] = BaseVector::wrapInConstant(
          input->size(), 0, childSpec->constantValue());
    } else {
      children[i] =
          inputRow->childAt(inputRowType.getChildIdx(childSpec->fieldName()));
    }
    types[i] = children[i]->type();
  }
  return std::make_shared<RowVector>(
      input->pool(),
      ROW(std::move(names), std::move(types)),
      nullptr,
      input->size(),
      std::move(children));
}

} // namespace facebook::velox::dwio::common
