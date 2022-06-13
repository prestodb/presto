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

#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox {
// Corresponds to declaration in LazyVector.h but repeated here to avoid
// dependency.
using RowSet = folly::Range<const int32_t*>;
} // namespace facebook::velox

namespace facebook::velox::dwrf {

static inline void skipBytes(
    uint64_t numBytes,
    dwio::common::SeekableInputStream* input,
    const char*& bufferStart,
    const char*& bufferEnd) {
  // bufferStart and bufferEnd may be null if we haven't started reading yet.
  if (bufferEnd - bufferStart >= numBytes) {
    bufferStart += numBytes;
    return;
  }
  numBytes -= bufferEnd - bufferStart;
  input->Skip(numBytes);
  bufferStart = bufferEnd;
}

static inline void readBytes(
    int64_t numBytes,
    dwio::common::SeekableInputStream* input,
    void* bytes,
    const char*& bufferStart,
    const char*& bufferEnd) {
  char* bytesAsChars = reinterpret_cast<char*>(bytes);
  uint64_t bytesRead = 0;
  auto bytesNeeded = numBytes;
  for (;;) {
    auto copyBytes = std::min<uint64_t>(bytesNeeded, bufferEnd - bufferStart);
    if (copyBytes) {
      memcpy(&bytesAsChars[bytesRead], bufferStart, copyBytes);
      bytesNeeded -= copyBytes;
      bytesRead += copyBytes;
      bufferStart += copyBytes;
    }
    if (!bytesNeeded) {
      break;
    }
    int32_t size;
    const void* bufferPointer;
    if (!input->Next(&bufferPointer, &size)) {
      VELOX_CHECK(false, "Reading past end");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + size;
  }
}

template <typename T>
inline bool isDense(const T* values, int32_t size) {
  return (values[size - 1] - values[0] == size - 1);
}

template <typename Dense, typename Sparse, typename SparseN>
void rowLoop(
    const int32_t* rows,
    int32_t begin,
    int32_t end,
    int32_t step,
    Dense dense,
    Sparse sparse,
    SparseN sparseN) {
  int32_t i = begin;
  auto firstPartial = (end - begin) & ~(step - 1);
  for (; i < firstPartial; i += step) {
    if (isDense(&rows[i], step)) {
      dense(i);
    } else {
      sparse(i);
    }
  }
  if (i != end) {
    sparseN(i, end - i);
  }
}

template <typename T, typename TResult>
inline void readContiguous(
    int32_t size,
    dwio::common::SeekableInputStream& input,
    TResult* output,
    const char*& bufferStart,
    const char*& bufferEnd) {
  if constexpr (sizeof(T) == sizeof(TResult)) {
    readBytes(
        size * sizeof(T),
        &input,
        reinterpret_cast<char*>(output),
        bufferStart,
        bufferEnd);
    return;
  }
  auto toRead = size;
  int32_t numOut = 0;
  while (toRead) {
    auto available = (bufferEnd - bufferStart) / sizeof(T);
    if (available == 0) {
      T temp;
      readBytes(
          sizeof(T),
          &input,
          reinterpret_cast<char*>(&temp),
          bufferStart,
          bufferEnd);
      output[numOut++] = temp;
      --toRead;
      continue;
    }
    auto pos = bufferStart;
    auto numRead = std::min<int32_t>(available, toRead);
    for (auto i = 0; i < numRead; ++i) {
      output[numOut++] = *reinterpret_cast<const T*>(pos);
      pos += sizeof(T);
    }
    bufferStart = pos;
    toRead -= numRead;
  }
}

// Returns the number of elements in rows that are < limit.
inline int32_t numBelow(folly::Range<const int32_t*> rows, int32_t limit) {
  return std::lower_bound(rows.begin(), rows.end(), limit) - rows.begin();
}

template <typename T, typename SingleValue, typename SparseRange>
inline void loopOverBuffers(
    folly::Range<const int32_t*> rows,
    int32_t initialRow,
    dwio::common::SeekableInputStream& input,
    const char*& bufferStart,
    const char*& bufferEnd,
    SingleValue singleValue,
    SparseRange range) {
  int32_t rowOffset = initialRow;
  int32_t rowIndex = 0;
  while (rowIndex < rows.size()) {
    auto available = (bufferEnd - bufferStart) / sizeof(T);
    auto numRowsInBuffer = rows.back() - rowOffset < available
        ? rows.size() - rowIndex
        : numBelow(
              folly::Range<const int32_t*>(
                  &rows[rowIndex], rows.size() - rowIndex),
              rowOffset + available);

    if (!numRowsInBuffer) {
      skipBytes(
          (rows[rowIndex] - rowOffset) * sizeof(T),
          &input,
          bufferStart,
          bufferEnd);
      T temp;
      readBytes(
          sizeof(T),
          &input,
          reinterpret_cast<char*>(&temp),
          bufferStart,
          bufferEnd);
      singleValue(temp, rowIndex);
      rowOffset = rows[rowIndex] + 1;
      ++rowIndex;
      continue;
    }
    range(
        rows.data(),
        rowIndex,
        numRowsInBuffer,
        rowOffset,
        reinterpret_cast<const T*>(bufferStart));
    bufferStart +=
        (rows[rowIndex + numRowsInBuffer - 1] + 1 - rowOffset) * sizeof(T);
    rowOffset = rows[rowIndex + numRowsInBuffer - 1] + 1;
    rowIndex += numRowsInBuffer;
  }
}

template <typename T, typename TResult>
inline void readRows(
    folly::Range<const int32_t*> rows,
    int32_t initialRow,
    dwio::common::SeekableInputStream& input,
    TResult* output,
    const char*& bufferStart,
    const char*& bufferEnd) {
  int32_t numOut = 0;
  loopOverBuffers<T>(
      rows,
      initialRow,
      input,
      bufferStart,
      bufferEnd,
      [&](T value, int32_t /*row*/) { output[numOut++] = value; },
      [&](const int32_t* rows,
          int32_t rowIndex,
          int32_t numRowsInBuffer,
          int32_t rowOffset,
          const T* buffer) {
        for (auto i = 0; i < numRowsInBuffer; ++i) {
          output[numOut++] = buffer[rows[i + rowIndex] - rowOffset];
        }
      });
}

} // namespace facebook::velox::dwrf
