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

#include "velox/dwio/dwrf/reader/DwrfData.h"
#include "velox/dwio/dwrf/reader/SelectiveIntegerColumnReader.h"

namespace facebook::velox::dwrf {

class SelectiveIntegerDictionaryColumnReader
    : public SelectiveIntegerColumnReader {
 public:
  using ValueType = int64_t;

  SelectiveIntegerDictionaryColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      uint32_t numBytes);

  void seekToRowGroup(uint32_t index) override {
    auto positionsProvider = formatData_->seekToRowGroup(index);
    if (inDictionaryReader_) {
      inDictionaryReader_->seekToRowGroup(positionsProvider);
    }

    dataReader_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  template <typename ColumnVisitor>
  void readWithVisitor(RowSet rows, ColumnVisitor visitor);

 private:
  void ensureInitialized();

  std::unique_ptr<ByteRleDecoder> inDictionaryReader_;
  std::unique_ptr<dwio::common::IntDecoder</* isSigned = */ false>> dataReader_;
  std::unique_ptr<dwio::common::IntDecoder</* isSigned = */ true>> dictReader_;
  std::function<BufferPtr()> dictInit_;
  RleVersion rleVersion_;
  bool initialized_{false};
};

template <typename ColumnVisitor>
void SelectiveIntegerDictionaryColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  VELOX_CHECK_EQ(rleVersion_, RleVersion_1);
  auto dictVisitor = visitor.toDictionaryColumnVisitor();
  auto reader = reinterpret_cast<RleDecoderV1<false>*>(dataReader_.get());
  if (nullsInReadRange_) {
    reader->readWithVisitor<true>(
        nullsInReadRange_->as<uint64_t>(), dictVisitor);
  } else {
    reader->readWithVisitor<false>(nullptr, dictVisitor);
  }
  readOffset_ += numRows;
}
} // namespace facebook::velox::dwrf
