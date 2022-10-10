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

#include "velox/vector/LazyVector.h"
#include <folly/ThreadLocal.h>
#include "velox/common/base/RawVector.h"
#include "velox/common/time/Timer.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox {

// Thread local stat writer, if set (not null) are used here to record how much
// time was spent on IO in lazy vectors.
static folly::ThreadLocalPtr<BaseRuntimeStatWriter> sRunTimeStatWriters;

void setRunTimeStatWriter(std::unique_ptr<BaseRuntimeStatWriter>&& ptr) {
  sRunTimeStatWriters.reset(std::move(ptr));
}

static void writeIOWallTimeStat(size_t ioTimeStartMicros) {
  if (BaseRuntimeStatWriter* pWriter = sRunTimeStatWriters.get()) {
    pWriter->addRuntimeStat(
        "dataSourceLazyWallNanos",
        RuntimeCounter(
            (getCurrentTimeMicro() - ioTimeStartMicros) * 1'000,
            RuntimeCounter::Unit::kNanos));
  }
}

void VectorLoader::load(RowSet rows, ValueHook* hook, VectorPtr* result) {
  const auto ioTimeStartMicros = getCurrentTimeMicro();
  loadInternal(rows, hook, result);
  writeIOWallTimeStat(ioTimeStartMicros);

  if (hook) {
    // Record number of rows loaded directly into ValueHook bypassing
    // materialization into vector. This counter can be used to understand
    // whether aggregation pushdown is happening or not.
    if (auto* pWriter = sRunTimeStatWriters.get()) {
      pWriter->addRuntimeStat("loadedToValueHook", RuntimeCounter(rows.size()));
    }
  }
}

void VectorLoader::load(
    const SelectivityVector& rows,
    ValueHook* hook,
    VectorPtr* result) {
  const auto ioTimeStartMicros = getCurrentTimeMicro();
  loadInternal(rows, hook, result);
  writeIOWallTimeStat(ioTimeStartMicros);
}

void VectorLoader::loadInternal(
    const SelectivityVector& rows,
    ValueHook* hook,
    VectorPtr* result) {
  if (rows.isAllSelected()) {
    const auto& indices = DecodedVector::consecutiveIndices();
    assert(!indices.empty());
    if (rows.end() <= indices.size()) {
      load(
          RowSet(&indices[rows.begin()], rows.end() - rows.begin()),
          hook,
          result);
      return;
    }
  }
  std::vector<vector_size_t> positions(rows.countSelected());
  int index = 0;
  rows.applyToSelected([&](vector_size_t row) { positions[index++] = row; });
  load(positions, hook, result);
}

VectorPtr LazyVector::slice(vector_size_t offset, vector_size_t length) const {
  VELOX_CHECK(isLoaded(), "Cannot take slice on unloaded lazy vector");
  VELOX_DCHECK(vector_);
  return vector_->slice(offset, length);
}

//   static
void LazyVector::ensureLoadedRows(
    VectorPtr& vector,
    const SelectivityVector& rows) {
  if (isLazyNotLoaded(*vector)) {
    DecodedVector decoded;
    SelectivityVector baseRows;
    ensureLoadedRows(vector, rows, decoded, baseRows);
  } else {
    // Even if LazyVectors have been loaded, via some other path, this
    // is needed for initializing wrappers.
    vector->loadedVector();
  }
}

// static
void LazyVector::ensureLoadedRows(
    VectorPtr& vector,
    const SelectivityVector& rows,
    DecodedVector& decoded,
    SelectivityVector& baseRows) {
  decoded.decode(*vector, rows, false);
  if (decoded.base()->encoding() != VectorEncoding::Simple::LAZY) {
    return;
  }
  auto lazyVector = decoded.base()->asUnchecked<LazyVector>();
  if (lazyVector->isLoaded()) {
    vector->loadedVector();
    return;
  }
  raw_vector<vector_size_t> rowNumbers;
  RowSet rowSet;
  if (decoded.isConstantMapping()) {
    rowNumbers.push_back(decoded.index(rows.begin()));
    rowSet = RowSet(rowNumbers);
  } else if (decoded.isIdentityMapping()) {
    if (rows.isAllSelected()) {
      auto iota = velox::iota(rows.end(), rowNumbers);
      rowSet = RowSet(iota, rows.end());
    } else {
      rowNumbers.resize(rows.end());
      rowNumbers.resize(simd::indicesOfSetBits(
          rows.asRange().bits(), 0, rows.end(), rowNumbers.data()));
      rowSet = RowSet(rowNumbers);
    }
  } else {
    baseRows.resize(0);
    baseRows.resize(lazyVector->size(), false);
    rows.applyToSelected([&](auto row) {
      if (!decoded.isNullAt(row)) {
        baseRows.setValid(decoded.index(row), true);
      }
    });
    baseRows.updateBounds();

    rowNumbers.resize(baseRows.end());
    rowNumbers.resize(simd::indicesOfSetBits(
        baseRows.asRange().bits(), 0, baseRows.end(), rowNumbers.data()));

    rowSet = RowSet(rowNumbers);

    lazyVector->load(rowSet, nullptr);

    // The loaded base vector may have fewer rows than the original. Make sure
    // there are no indices referring to rows past the end of the base vector.

    BufferPtr indices = allocateIndices(rows.end(), vector->pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    auto decodedIndices = decoded.indices();
    rows.applyToSelected(
        [&](auto row) { rawIndices[row] = decodedIndices[row]; });

    BufferPtr nulls = nullptr;
    if (decoded.nulls()) {
      if (!baseRows.hasSelections()) {
        // All valid values in 'rows' are nulls. Set the nulls buffer to all
        // nulls to avoid hitting DCHECK when creating a dictionary with a zero
        // sized base vector.
        nulls = allocateNulls(rows.end(), vector->pool(), bits::kNull);
      } else {
        nulls = allocateNulls(rows.end(), vector->pool());
        std::memcpy(
            nulls->asMutable<uint64_t>(),
            decoded.nulls(),
            bits::nbytes(rows.end()));
      }
    }

    vector = BaseVector::wrapInDictionary(
        std::move(nulls),
        std::move(indices),
        rows.end(),
        lazyVector->loadedVectorShared());
    return;
  }
  lazyVector->load(rowSet, nullptr);
  // An explicit call to loadedVector() is necessary to allow for proper
  // initialization of dictionaries, sequences, etc. on top of lazy vector
  // after it has been loaded, even if loaded via some other path.
  vector->loadedVector();
}

} // namespace facebook::velox
