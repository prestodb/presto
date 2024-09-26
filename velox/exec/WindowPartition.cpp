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
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

WindowPartition::WindowPartition(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    bool partial,
    bool complete)
    : partial_(partial),
      data_(data),
      partition_(rows),
      complete_(complete),
      inputMapping_(inputMapping),
      sortKeyInfo_(sortKeyInfo) {
  VELOX_CHECK_NE(partial_, complete_);
  VELOX_CHECK_NE(complete_, partition_.empty());

  for (auto index : inputMapping_) {
    columns_.emplace_back(data_->columnAt(index));
  }
}

WindowPartition::WindowPartition(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo)
    : WindowPartition(data, rows, inputMapping, sortKeyInfo, false, true) {}

WindowPartition::WindowPartition(
    RowContainer* data,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo)
    : WindowPartition(data, {}, inputMapping, sortKeyInfo, true, false) {}

void WindowPartition::addRows(const std::vector<char*>& rows) {
  checkPartial();
  rows_.insert(rows_.end(), rows.begin(), rows.end());
  partition_ = folly::Range(rows_.data(), rows_.size());
}

void WindowPartition::eraseRows(vector_size_t numRows) {
  checkPartial();
  VELOX_CHECK_GE(data_->numRows(), numRows);
  data_->eraseRows(folly::Range<char**>(rows_.data(), numRows));
}

void WindowPartition::removeProcessedRows(vector_size_t numRows) {
  checkPartial();

  VELOX_CHECK_NULL(previousRow_);
  if (complete_ && rows_.size() == numRows) {
    eraseRows(numRows);
  } else {
    eraseRows(numRows - 1);
    previousRow_ = rows_[numRows - 1];
  }

  rows_.erase(rows_.begin(), rows_.begin() + numRows);
  partition_ = folly::Range(rows_.data(), rows_.size());
  startRow_ += numRows;
}

vector_size_t WindowPartition::numRowsForProcessing(
    vector_size_t partitionOffset) const {
  if (partial_) {
    return partition_.size();
  } else {
    return partition_.size() - partitionOffset;
  }
}

void WindowPartition::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  RowContainer::extractColumn(
      partition_.data(),
      rowNumbers,
      columns_[columnIndex],
      data_->columnHasNulls(inputMapping_[columnIndex]),
      resultOffset,
      result);
}

void WindowPartition::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  VELOX_CHECK_GE(partitionOffset, startRow_);
  RowContainer::extractColumn(
      partition_.data() + partitionOffset - startRow_,
      numRows,
      columns_[columnIndex],
      data_->columnHasNulls(inputMapping_[columnIndex]),
      resultOffset,
      result);
}

void WindowPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  RowContainer::extractNulls(
      partition_.data() + partitionOffset,
      numRows,
      columns_[columnIndex],
      nullsBuffer);
}

namespace {

std::pair<vector_size_t, vector_size_t> findMinMaxFrameBounds(
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds) {
  auto rawFrameStarts = frameStarts->as<vector_size_t>();
  auto rawFrameEnds = frameEnds->as<vector_size_t>();

  auto firstValidRow = validRows.begin();
  vector_size_t minFrame = rawFrameStarts[firstValidRow];
  vector_size_t maxFrame = rawFrameEnds[firstValidRow];
  validRows.applyToSelected([&](auto i) {
    minFrame = std::min(minFrame, rawFrameStarts[i]);
    maxFrame = std::max(maxFrame, rawFrameEnds[i]);
  });
  return {minFrame, maxFrame};
}

} // namespace

std::optional<std::pair<vector_size_t, vector_size_t>>
WindowPartition::extractNulls(
    column_index_t col,
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds,
    BufferPtr* nulls) const {
  VELOX_CHECK(validRows.hasSelections(), "Buffer has no active rows");
  auto [minFrame, maxFrame] =
      findMinMaxFrameBounds(validRows, frameStarts, frameEnds);

  // Add 1 since maxFrame is the index of the frame end row.
  auto framesSize = maxFrame - minFrame + 1;
  AlignedBuffer::reallocate<bool>(nulls, framesSize);

  extractNulls(col, minFrame, framesSize, *nulls);
  auto foundNull =
      bits::findFirstBit((*nulls)->as<uint64_t>(), 0, framesSize) >= 0;
  return foundNull ? std::make_optional(std::make_pair(minFrame, framesSize))
                   : std::nullopt;
}

bool WindowPartition::compareRowsWithSortKeys(const char* lhs, const char* rhs)
    const {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : sortKeyInfo_) {
    if (auto result = data_->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return result < 0;
    }
  }
  return false;
}

vector_size_t WindowPartition::findPeerRowEndIndex(
    vector_size_t startRow,
    vector_size_t lastRow,
    const std::function<bool(const char*, const char*)>& peerCompare) {
  auto peerEnd = startRow;
  while (peerEnd <= lastRow) {
    if (peerCompare(
            partition_[startRow - startRow_],
            partition_[peerEnd - startRow_])) {
      break;
    }
    ++peerEnd;
  }
  return peerEnd;
}

void WindowPartition::removePreviousRow() {
  VELOX_CHECK_NOT_NULL(previousRow_);
  data_->eraseRows(folly::Range<char**>(&previousRow_, 1));
  previousRow_ = nullptr;
}

std::pair<vector_size_t, vector_size_t> WindowPartition::computePeerBuffers(
    vector_size_t start,
    vector_size_t end,
    vector_size_t prevPeerStart,
    vector_size_t prevPeerEnd,
    vector_size_t* rawPeerStarts,
    vector_size_t* rawPeerEnds) {
  const auto peerCompare = [&](const char* lhs, const char* rhs) -> bool {
    return compareRowsWithSortKeys(lhs, rhs);
  };

  VELOX_CHECK_LE(end, numRows() + startRow_);

  auto lastPartitionRow = numRows() + startRow_ - 1;
  auto peerStart = prevPeerStart;
  auto peerEnd = prevPeerEnd;

  size_t next = start;
  size_t index{0};
  if (partial_ && start > 0) {
    const auto peerGroup = peerCompare(previousRow_, partition_[0]);

    // The first row is the last row in previous batch so delete it after used
    // for the first peer group detection.
    removePreviousRow();

    if (!peerGroup) {
      peerEnd = findPeerRowEndIndex(start, lastPartitionRow, peerCompare);

      for (; next < std::min(end, peerEnd); ++next, ++index) {
        rawPeerStarts[index] = peerStart;
        rawPeerEnds[index] = peerEnd - 1;
      }
    }
  }

  for (; next < end; ++next, ++index) {
    // When traversing input partition rows, the peers are the rows with the
    // same values for the ORDER BY clause. These rows are equal in some ways
    // and affect the results of ranking functions. This logic exploits the fact
    // that all rows between the peerStart and peerEnd have the same values for
    // rawPeerStarts and rawPeerEnds. So we can compute them just once and reuse
    // across the rows in that peer interval. Note: peerStart and peerEnd can be
    // maintained across getOutput calls. Hence, they are returned to the
    // caller.
    if (next == 0 || next >= peerEnd) {
      // Compute peerStart and peerEnd rows for the first row of the partition
      // or when past the previous peerGroup.
      peerStart = next;
      peerEnd = findPeerRowEndIndex(peerStart, lastPartitionRow, peerCompare);
    }

    rawPeerStarts[index] = peerStart;
    rawPeerEnds[index] = peerEnd - 1;
  }
  VELOX_CHECK_EQ(index, end - start);
  return {peerStart, peerEnd};
}

// Searches for start[frameColumn] in orderByColumn.
// The search could return the first or last row matching start[frameColumn].
// If a matching row is not present, then the index of the first row greater
// than value is returned if the rows are in ascending order. For descending,
// first row smaller than value is returned.
// The search starts with a binary search for a starting point that is the
// largest value < frame bound or (smallest value > frame bound for
// descending). After finding that point it does a sequential search for the
// required value.
vector_size_t WindowPartition::searchFrameValue(
    bool firstMatch,
    vector_size_t start,
    vector_size_t end,
    vector_size_t currentRow,
    column_index_t orderByColumn,
    column_index_t frameColumn,
    const CompareFlags& flags) const {
  auto current = partition_[currentRow];
  vector_size_t begin = start;
  vector_size_t finish = end;
  while (finish - begin >= 2) {
    auto mid = (begin + finish) / 2;
    auto compareResult = data_->compare(
        partition_[mid], current, orderByColumn, frameColumn, flags);

    if (compareResult >= 0) {
      // Search in the first half of the column.
      finish = mid;
    } else {
      // Search in the second half of the column.
      begin = mid;
    }
  }

  return linearSearchFrameValue(
      firstMatch, begin, end, currentRow, orderByColumn, frameColumn, flags);
}

vector_size_t WindowPartition::linearSearchFrameValue(
    bool firstMatch,
    vector_size_t start,
    vector_size_t end,
    vector_size_t currentRow,
    column_index_t orderByColumn,
    column_index_t frameColumn,
    const CompareFlags& flags) const {
  auto current = partition_[currentRow];
  for (vector_size_t i = start; i < end; ++i) {
    auto compareResult = data_->compare(
        partition_[i], current, orderByColumn, frameColumn, flags);

    // The bound value was found. Return if firstMatch required.
    // If the last match is required, then we need to find the first row that
    // crosses the bound and return the prior row.
    if (compareResult == 0) {
      if (firstMatch) {
        return i;
      }
    }

    // Bound is crossed. Last match needs the previous row.
    // But for first row matches, this is the first
    // row that has crossed, but not equals boundary (The equal boundary case
    // is covered by the condition above). So the bound matches this row itself.
    if (compareResult > 0) {
      if (firstMatch) {
        return i;
      } else {
        return i - 1;
      }
    }
  }

  // Return a row beyond the partition boundary. The logic to determine valid
  // frames handles the out of bound and empty frames from this value.
  return end == numRows() ? numRows() + 1 : -1;
}

void WindowPartition::updateKRangeFrameBounds(
    bool firstMatch,
    bool isPreceding,
    const CompareFlags& flags,
    vector_size_t startRow,
    vector_size_t numRows,
    column_index_t frameColumn,
    const vector_size_t* rawPeerBounds,
    vector_size_t* rawFrameBounds) const {
  column_index_t orderByColumn = sortKeyInfo_[0].first;
  column_index_t mappedFrameColumn = inputMapping_[frameColumn];

  vector_size_t start = 0;
  vector_size_t end;
  // frameColumn is a column index into the original input rows, while
  // orderByColumn is a column index into rows in data_ after the columns are
  // reordered as per inputMapping_.
  RowColumn frameRowColumn = columns_[frameColumn];
  RowColumn orderByRowColumn = data_->columnAt(orderByColumn);
  for (auto i = 0; i < numRows; i++) {
    auto currentRow = startRow + i;
    auto* partitionRow = partition_[currentRow];

    // The user is expected to set the frame column equal to NULL when the
    // ORDER BY value is NULL and not in any other case. Validate this
    // assumption.
    VELOX_DCHECK_EQ(
        RowContainer::isNullAt(
            partitionRow, frameRowColumn.nullByte(), frameRowColumn.nullMask()),
        RowContainer::isNullAt(
            partitionRow,
            orderByRowColumn.nullByte(),
            orderByRowColumn.nullMask()));

    // If the frame is NULL or 0 preceding or 0 following then the current row
    // has same values for order by and frame column. In that case
    // the bound matches the peer row for this row.
    if (data_->compare(
            partitionRow,
            partitionRow,
            orderByColumn,
            mappedFrameColumn,
            flags) == 0) {
      rawFrameBounds[i] = rawPeerBounds[i];
    } else {
      // If the search is for a preceding bound then rows between
      // [0, currentRow] are examined. For following bounds, rows between
      // [currentRow, numRows()) are checked.
      if (isPreceding) {
        start = 0;
        end = currentRow + 1;
      } else {
        start = currentRow;
        end = partition_.size();
      }
      rawFrameBounds[i] = searchFrameValue(
          firstMatch,
          start,
          end,
          currentRow,
          orderByColumn,
          mappedFrameColumn,
          flags);
    }
  }
}

void WindowPartition::computeKRangeFrameBounds(
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBuffer,
    vector_size_t* rawFrameBounds) const {
  CompareFlags flags;
  flags.ascending = sortKeyInfo_[0].second.isAscending();
  flags.nullsFirst = sortKeyInfo_[0].second.isNullsFirst();
  // Start bounds require first match. End bounds require last match.
  updateKRangeFrameBounds(
      isStartBound,
      isPreceding,
      flags,
      startRow,
      numRows,
      frameColumn,
      rawPeerBuffer,
      rawFrameBounds);
}

} // namespace facebook::velox::exec
