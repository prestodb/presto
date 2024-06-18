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

namespace {

template <TypeKind kind>
void scalarGatherCopy(
    BaseVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    column_index_t sourceColumnChannel) {
  VELOX_DCHECK(target->isFlatEncoding());

  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = target->template asUnchecked<FlatVector<T>>();
  uint64_t* rawNulls = nullptr;
  if (std::is_same_v<T, StringView>) {
    for (int i = 0; i < count; ++i) {
      VELOX_DCHECK(!sources[i]->mayHaveNulls());
      if (sources[i]
              ->childAt(sourceColumnChannel)
              ->isNullAt(sourceIndices[i])) {
        if (FOLLY_UNLIKELY(rawNulls == nullptr)) {
          rawNulls = target->mutableRawNulls();
        }
        bits::setNull(rawNulls, targetIndex + i, true);
        continue;
      }
      auto* source = sources[i]->childAt(sourceColumnChannel).get();
      flatVector->setNoCopy(
          targetIndex + i,
          source->template asUnchecked<FlatVector<T>>()->valueAt(
              sourceIndices[i]));
      flatVector->acquireSharedStringBuffers(source);
    }
  } else {
    for (int i = 0; i < count; ++i) {
      VELOX_DCHECK(!sources[i]->mayHaveNulls());
      if (sources[i]
              ->childAt(sourceColumnChannel)
              ->isNullAt(sourceIndices[i])) {
        if (FOLLY_UNLIKELY(rawNulls == nullptr)) {
          rawNulls = target->mutableRawNulls();
        }
        bits::setNull(rawNulls, targetIndex + i, true);
        continue;
      }
      flatVector->set(
          targetIndex + i,
          sources[i]
              ->childAt(sourceColumnChannel)
              ->template asUnchecked<FlatVector<T>>()
              ->valueAt(sourceIndices[i]));
    }
  }
}

void complexGatherCopy(
    BaseVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    column_index_t sourceChannel) {
  for (int i = 0; i < count; ++i) {
    target->copy(
        sources[i]->childAt(sourceChannel).get(),
        targetIndex + i,
        sourceIndices[i],
        1);
  }
}

void gatherCopy(
    BaseVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    column_index_t sourceChannel) {
  if (target->isScalar()) {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        scalarGatherCopy,
        target->type()->kind(),
        target,
        targetIndex,
        count,
        sources,
        sourceIndices,
        sourceChannel);
  } else {
    complexGatherCopy(
        target, targetIndex, count, sources, sourceIndices, sourceChannel);
  }
}

// We want to aggregate some operator runtime metrics per operator rather than
// per event. This function returns true for such metrics.
bool shouldAggregateRuntimeMetric(const std::string& name) {
  static const folly::F14FastSet<std::string> metricNames{
      "dataSourceAddSplitWallNanos",
      "dataSourceReadWallNanos",
      "dataSourceLazyWallNanos",
      "queuedWallNanos",
      "flushTimes"};
  if (metricNames.contains(name)) {
    return true;
  }

  // 'blocked*WallNanos'
  if (name.size() > 16 and strncmp(name.c_str(), "blocked", 7) == 0) {
    return true;
  }

  return false;
}

} // namespace

void deselectRowsWithNulls(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    SelectivityVector& rows) {
  bool anyChange = false;
  for (int32_t i = 0; i < hashers.size(); ++i) {
    auto& decoded = hashers[i]->decodedVector();
    if (decoded.mayHaveNulls()) {
      anyChange = true;
      const auto* nulls = hashers[i]->decodedVector().nulls(&rows);
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
  return rows.countSelected();
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
  if (!rows.isAllSelected()) {
    bits::andBits(selectedBits, rows.allBits(), 0, size);
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
  auto nulls = decoded.nulls(&rows);
  auto indices = decoded.indices();

  vector_size_t passed = 0;
  auto* rawSelected = filterEvalCtx.getRawSelectedIndices(size, pool);
  auto* rawSelectedBits = filterEvalCtx.getRawSelectedBits(size, pool);
  memset(rawSelectedBits, 0, bits::nbytes(size));
  for (int32_t i = 0; i < size; ++i) {
    auto index = indices[i];
    if ((!nulls || !bits::isBitNull(nulls, i)) &&
        bits::isBitSet(values, index) && rows.isValid(i)) {
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

  return wrap(
      size,
      std::move(mapping),
      asRowType(vector->type()),
      vector->children(),
      vector->pool());
}

RowVectorPtr wrap(
    vector_size_t size,
    BufferPtr mapping,
    const RowTypePtr& rowType,
    const std::vector<VectorPtr>& childVectors,
    memory::MemoryPool* pool) {
  if (mapping == nullptr) {
    return RowVector::createEmpty(rowType, pool);
  }
  std::vector<VectorPtr> wrappedChildren;
  wrappedChildren.reserve(childVectors.size());
  for (auto& child : childVectors) {
    wrappedChildren.emplace_back(wrapChild(size, mapping, child));
  }
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, size, wrappedChildren);
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

void gatherCopy(
    RowVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    const std::vector<IdentityProjection>& columnMap) {
  VELOX_DCHECK_GE(count, 0);
  if (FOLLY_UNLIKELY(count <= 0)) {
    return;
  }
  VELOX_CHECK_LE(count, sources.size());
  VELOX_CHECK_LE(count, sourceIndices.size());
  VELOX_DCHECK_EQ(sources.size(), sourceIndices.size());
  if (!columnMap.empty()) {
    for (const auto& columnProjection : columnMap) {
      gatherCopy(
          target->childAt(columnProjection.outputChannel).get(),
          targetIndex,
          count,
          sources,
          sourceIndices,
          columnProjection.inputChannel);
    }
  } else {
    for (auto i = 0; i < target->type()->size(); ++i) {
      gatherCopy(
          target->childAt(i).get(),
          targetIndex,
          count,
          sources,
          sourceIndices,
          i);
    }
  }
}

std::string makeOperatorSpillPath(
    const std::string& spillDir,
    int pipelineId,
    int driverId,
    int32_t operatorId) {
  VELOX_CHECK(!spillDir.empty());
  return fmt::format("{}/{}_{}_{}", spillDir, pipelineId, driverId, operatorId);
}

void addOperatorRuntimeStats(
    const std::string& name,
    const RuntimeCounter& value,
    std::unordered_map<std::string, RuntimeMetric>& stats) {
  if (UNLIKELY(stats.count(name) == 0)) {
    stats.insert(std::pair(name, RuntimeMetric(value.unit)));
  } else {
    VELOX_CHECK_EQ(stats.at(name).unit, value.unit);
  }
  stats.at(name).addValue(value.value);
}

void aggregateOperatorRuntimeStats(
    std::unordered_map<std::string, RuntimeMetric>& stats) {
  for (auto& runtimeMetric : stats) {
    if (shouldAggregateRuntimeMetric(runtimeMetric.first)) {
      runtimeMetric.second.aggregate();
    }
  }
}

folly::Range<vector_size_t*> initializeRowNumberMapping(
    BufferPtr& mapping,
    vector_size_t size,
    memory::MemoryPool* pool) {
  if (!mapping || !mapping->unique() ||
      mapping->size() < sizeof(vector_size_t) * size) {
    mapping = allocateIndices(size, pool);
  }
  return folly::Range(mapping->asMutable<vector_size_t>(), size);
}

void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const RowVectorPtr& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping) {
  projectChildren(
      projectedChildren, src->children(), projections, size, mapping);
}

void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const std::vector<VectorPtr>& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping) {
  for (auto [inputChannel, outputChannel] : projections) {
    if (outputChannel >= projectedChildren.size()) {
      projectedChildren.resize(outputChannel + 1);
    }
    projectedChildren[outputChannel] =
        wrapChild(size, mapping, src[inputChannel]);
  }
}
} // namespace facebook::velox::exec
