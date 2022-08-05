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

#include "velox/exec/OperatorUtils.h"
#include "velox/exec/VectorHasher.h"
#include "velox/expression/EvalCtx.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

void deselectRowsWithNulls(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    SelectivityVector& rows) {
  bool anyChange = false;
  for (int32_t i = 0; i < hashers.size(); ++i) {
    auto& decoded = hashers[i]->decodedVector();
    if (decoded.mayHaveNulls()) {
      anyChange = true;
      const auto* nulls = hashers[i]->decodedVector().nulls();
      bits::andBits(rows.asMutableRange().bits(), nulls, 0, rows.end());
    }
  }

  if (anyChange) {
    rows.updateBounds();
  }
}

uint64_t* FilterEvalCtx::getRawSelectedBits(
    vector_size_t size,
    memory::MemoryPool* pool) {
  uint64_t* rawBits;
  BaseVector::ensureBuffer<bool, uint64_t>(size, pool, &selectedBits, &rawBits);
  return rawBits;
}

vector_size_t* FilterEvalCtx::getRawSelectedIndices(
    vector_size_t size,
    memory::MemoryPool* pool) {
  vector_size_t* rawSelected;
  BaseVector::ensureBuffer<vector_size_t>(
      size, pool, &selectedIndices, &rawSelected);
  return rawSelected;
}

namespace {
vector_size_t processConstantFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows) {
  auto constant = filterResult->as<ConstantVector<bool>>();
  if (constant->isNullAt(0) || constant->valueAt(0) == false) {
    return 0;
  }
  return rows.size();
}

vector_size_t processFlatFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool) {
  auto size = rows.size();

  auto selectedBits = filterEvalCtx.getRawSelectedBits(size, pool);
  auto nonNullBits =
      filterResult->as<FlatVector<bool>>()->rawValues<uint64_t>();
  if (filterResult->mayHaveNulls()) {
    bits::andBits(selectedBits, nonNullBits, filterResult->rawNulls(), 0, size);
  } else {
    memcpy(selectedBits, nonNullBits, bits::nbytes(size));
  }

  vector_size_t passed = 0;
  auto* rawSelected = filterEvalCtx.getRawSelectedIndices(size, pool);
  bits::forEachSetBit(
      selectedBits, 0, size, [&rawSelected, &passed](vector_size_t row) {
        rawSelected[passed++] = row;
      });
  return passed;
}

vector_size_t processEncodedFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool) {
  auto size = rows.size();

  DecodedVector& decoded = filterEvalCtx.decodedResult;
  decoded.decode(*filterResult.get(), rows);
  auto values = decoded.data<uint64_t>();
  auto nulls = decoded.nulls();
  auto indices = decoded.indices();

  vector_size_t passed = 0;
  auto* rawSelected = filterEvalCtx.getRawSelectedIndices(size, pool);
  auto* rawSelectedBits = filterEvalCtx.getRawSelectedBits(size, pool);
  memset(rawSelectedBits, 0, bits::nbytes(size));
  for (int32_t i = 0; i < size; ++i) {
    auto index = indices[i];
    if ((!nulls || !bits::isBitNull(nulls, i)) &&
        bits::isBitSet(values, index)) {
      rawSelected[passed++] = i;
      bits::setBit(rawSelectedBits, i);
    }
  }
  return passed;
}
} // namespace

vector_size_t processFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool) {
  switch (filterResult->encoding()) {
    case VectorEncoding::Simple::CONSTANT:
      return processConstantFilterResults(filterResult, rows);
    case VectorEncoding::Simple::FLAT:
      return processFlatFilterResults(filterResult, rows, filterEvalCtx, pool);
    default:
      return processEncodedFilterResults(
          filterResult, rows, filterEvalCtx, pool);
  }
}

VectorPtr wrapChild(
    vector_size_t size,
    BufferPtr mapping,
    const VectorPtr& child,
    BufferPtr nulls) {
  if (!mapping) {
    return child;
  }

  return BaseVector::wrapInDictionary(nulls, mapping, size, child);
}

RowVectorPtr
wrap(vector_size_t size, BufferPtr mapping, const RowVectorPtr& vector) {
  if (!mapping) {
    return vector;
  }

  std::vector<VectorPtr> wrappedChildren;
  wrappedChildren.reserve(vector->childrenSize());
  for (auto& child : vector->children()) {
    wrappedChildren.emplace_back(wrapChild(size, mapping, child));
  }

  return std::make_shared<RowVector>(
      vector->pool(),
      vector->type(),
      BufferPtr(nullptr),
      size,
      wrappedChildren);
}

void loadColumns(const RowVectorPtr& input, core::ExecCtx& execCtx) {
  LocalDecodedVector decodedHolder(execCtx);
  LocalSelectivityVector baseRowsHolder(&execCtx);
  LocalSelectivityVector rowsHolder(&execCtx);
  SelectivityVector* rows = nullptr;
  for (auto& child : input->children()) {
    if (isLazyNotLoaded(*child)) {
      if (!rows) {
        rows = rowsHolder.get(input->size());
        rows->setAll();
      }
      LazyVector::ensureLoadedRows(
          child,
          *rows,
          *decodedHolder.get(),
          *baseRowsHolder.get(input->size()));
    }
  }
}

} // namespace facebook::velox::exec
