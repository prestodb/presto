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
        (getCurrentTimeMicro() - ioTimeStartMicros) * 1'000);
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
      pWriter->addRuntimeStat("loadedToValueHook", rows.size());
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
    baseRows.resize(lazyVector->size()), false;
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

    // If we have a mapping that is not a single level of dictionary, we
    // collapse this to a single level of dictionary. The reason is
    // that the inner levels of dictionary will reference rows that
    // are not loaded, since the load was done for only the rows that
    // are reachable from 'vector'.
    if (vector->encoding() != VectorEncoding::Simple::DICTIONARY ||
        lazyVector != vector->valueVector().get()) {
      lazyVector->load(rowSet, nullptr);
      BufferPtr indices = allocateIndices(vector->size(), vector->pool());
      std::memcpy(
          indices->asMutable<vector_size_t>(),
          decoded.indices(),
          sizeof(vector_size_t) * vector->size());
      vector = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          std::move(indices),
          vector->size(),
          lazyVector->loadedVectorShared());
      return;
    }
  }
  lazyVector->load(rowSet, nullptr);
  // An explicit call to loadedVector() is necessary to allow for proper
  // initialization of dictionaries, sequences, etc. on top of lazy vector
  // after it has been loaded, even if loaded via some other path.
  vector->loadedVector();
}

} // namespace facebook::velox
