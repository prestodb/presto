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

using namespace velox::common;

namespace {

template <TypeKind kKind>
bool filterSimpleVectorRow(
    const BaseVector& vector,
    Filter& filter,
    vector_size_t index) {
  using T = typename TypeTraits<kKind>::NativeType;
  auto* simpleVector = vector.asUnchecked<SimpleVector<T>>();
  return applyFilter(filter, simpleVector->valueAt(index));
}

bool filterRow(const BaseVector& vector, Filter& filter, vector_size_t index) {
  if (vector.isNullAt(index)) {
    return filter.testNull();
  }
  switch (vector.typeKind()) {
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      VELOX_USER_CHECK(
          filter.kind() == FilterKind::kIsNull ||
              filter.kind() == FilterKind::kIsNotNull,
          "Complex type can only take null filter, got {}",
          filter.toString());
      return filter.testNonNull();
    default:
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          filterSimpleVectorRow, vector.typeKind(), vector, filter, index);
  }
}

void applyFilter(
    const BaseVector& vector,
    const ScanSpec& spec,
    uint64_t* result) {
  if (spec.filter()) {
    bits::forEachSetBit(result, 0, vector.size(), [&](auto i) {
      if (!filterRow(vector, *spec.filter(), i)) {
        bits::clearBit(result, i);
      }
    });
  }
  if (!vector.type()->isRow()) {
    // Filter on MAP or ARRAY children are pruning, and won't affect correctness
    // of the result.
    return;
  }
  auto& rowType = vector.type()->asRow();
  auto* rowVector = vector.as<RowVector>();
  // Should not have any lazy from non-selective reader.
  VELOX_CHECK_NOT_NULL(rowVector);
  for (auto& childSpec : spec.children()) {
    auto child =
        rowVector->childAt(rowType.getChildIdx(childSpec->fieldName()));
    applyFilter(*child, *childSpec, result);
  }
}

} // namespace

VectorPtr RowReader::projectColumns(
    const VectorPtr& input,
    const ScanSpec& spec,
    const Mutation* mutation) {
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
  std::vector<uint64_t> passed(bits::nwords(input->size()), -1);
  if (mutation) {
    if (mutation->deletedRows) {
      bits::andWithNegatedBits(
          passed.data(), mutation->deletedRows, 0, input->size());
    }
    if (mutation->randomSkip) {
      bits::forEachSetBit(passed.data(), 0, input->size(), [&](auto i) {
        if (!mutation->randomSkip->testOne()) {
          bits::clearBit(passed.data(), i);
        }
      });
    }
  }
  for (auto& childSpec : spec.children()) {
    VectorPtr child;
    if (childSpec->isConstant()) {
      child = BaseVector::wrapInConstant(
          input->size(), 0, childSpec->constantValue());
    } else {
      child =
          inputRow->childAt(inputRowType.getChildIdx(childSpec->fieldName()));
      applyFilter(*child, *childSpec, passed.data());
    }
    if (!childSpec->projectOut()) {
      continue;
    }
    auto i = childSpec->channel();
    names[i] = childSpec->fieldName();
    types[i] = child->type();
    children[i] = std::move(child);
  }
  auto rowType = ROW(std::move(names), std::move(types));
  auto size = bits::countBits(passed.data(), 0, input->size());
  if (size == 0) {
    return RowVector::createEmpty(rowType, input->pool());
  }
  if (size < input->size()) {
    auto indices = allocateIndices(size, input->pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    vector_size_t j = 0;
    bits::forEachSetBit(
        passed.data(), 0, input->size(), [&](auto i) { rawIndices[j++] = i; });
    for (auto& child : children) {
      child->disableMemo();
      child = BaseVector::wrapInDictionary(
          nullptr, indices, size, std::move(child));
    }
  }
  return std::make_shared<RowVector>(
      input->pool(), rowType, nullptr, size, std::move(children));
}

} // namespace facebook::velox::dwio::common
