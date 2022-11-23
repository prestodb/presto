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

#include "velox/dwio/common/SelectiveRepeatedColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetData.h"

namespace facebook::velox::parquet {

/// Comtainer for the lengths of a repeated reader where the lengths are
/// pre-filled from repdefs.
class RepeatedLengths {
 public:
  void setLengths(BufferPtr lengths) {
    lengths_ = std::move(lengths);
    nextLengthIndex_ = 0;
  }

  BufferPtr& lengths() {
    return lengths_;
  }

  void readLengths(int32_t* FOLLY_NONNULL lengths, int32_t numLengths) {
    VELOX_CHECK_LE(
        nextLengthIndex_ + numLengths, lengths_->size() / sizeof(int32_t));
    memcpy(
        lengths,
        lengths_->as<int32_t>() + nextLengthIndex_,
        numLengths * sizeof(int32_t));
    nextLengthIndex_ += numLengths;
  }

 private:
  BufferPtr lengths_;
  int32_t nextLengthIndex_{0};
};

class ListColumnReader : public dwio::common::SelectiveListColumnReader {
 public:
  ListColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> requestedType,
      ParquetParams& params,
      common::ScanSpec& scanSpec);

  void prepareRead(
      vector_size_t offset,
      RowSet rows,
      const uint64_t* FOLLY_NULLABLE incomingNulls) {
    // The prepare is done by the topmost list/struct.
  }

  void seekToRowGroup(uint32_t index) override;

  void enqueueRowGroup(uint32_t index, dwio::common::BufferedInput& input);

  void read(
      vector_size_t offset,
      RowSet rows,
      const uint64_t* FOLLY_NULLABLE /*incomingNulls*/) override;

  void setLengths(BufferPtr lengths) {
    lengths_.setLengths(lengths);
  }
  void readLengths(
      int32_t* FOLLY_NONNULL lengths,
      int32_t numLengths,
      const uint64_t* FOLLY_NULLABLE /*nulls*/) override {
    lengths_.readLengths(lengths, numLengths);
  }

  void setLengthsFromRepDefs(PageReader& leaf);

 private:
  RepeatedLengths lengths_;
  ::parquet::internal::LevelInfo levelInfo_;
};

} // namespace facebook::velox::parquet
