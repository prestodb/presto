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

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

class SelectiveStringDictionaryColumnReader
    : public dwio::common::SelectiveColumnReader {
 public:
  using ValueType = int32_t;

  SelectiveStringDictionaryColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  bool hasBulkPath() const override {
    // Only ORC uses RLEv2 encoding. Currently, ORC string data does not
    // support fastpath reads. When reading RLEv2-encoded string data
    // with null, the query will fail.
    return version_ != velox::dwrf::RleVersion_2;
  }

  void seekToRowGroup(uint32_t index) override {
    SelectiveColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->as<DwrfData>().seekToRowGroup(index);
    if (strideDictStream_) {
      strideDictStream_->seekToPosition(positionsProvider);
      strideDictLengthDecoder_->seekToRowGroup(positionsProvider);
      // skip row group dictionary size
      positionsProvider.next();
    }

    dictIndex_->seekToRowGroup(positionsProvider);

    if (inDictionaryReader_) {
      inDictionaryReader_->seekToRowGroup(positionsProvider);
    }

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(
      vector_size_t offset,
      const RowSet& rows,
      const uint64_t* incomingNulls) override;

  void getValues(const RowSet& rows, VectorPtr* result) override;

 private:
  void loadStrideDictionary();
  void makeDictionaryBaseVector();

  template <typename TVisitor>
  void readWithVisitor(TVisitor visitor);

  // Fills 'values' from 'data' and 'lengthDecoder'. The count of
  // values is in 'values.numValues'.
  void loadDictionary(
      dwio::common::SeekableInputStream& data,
      dwio::common::IntDecoder</*isSigned*/ false>& lengthDecoder,
      dwio::common::DictionaryValues& values);
  void ensureInitialized();

  void makeFlat(VectorPtr* result);

  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> dictIndex_;
  std::unique_ptr<ByteRleDecoder> inDictionaryReader_;
  std::unique_ptr<dwio::common::SeekableInputStream> strideDictStream_;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>>
      strideDictLengthDecoder_;

  FlatVectorPtr<StringView> dictionaryValues_;

  int64_t lastStrideIndex_;
  size_t positionOffset_;
  size_t strideDictSizeOffset_;
  RleVersion version_;

  const StrideIndexProvider& provider_;
  dwio::common::ColumnReaderStatistics& statistics_;

  // lazy load the dictionary
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> lengthDecoder_;
  std::unique_ptr<dwio::common::SeekableInputStream> blobStream_;
  bool initialized_{false};
  vector_size_t numRowsScanned_;
};

template <typename TVisitor>
void SelectiveStringDictionaryColumnReader::readWithVisitor(TVisitor visitor) {
  if (version_ == velox::dwrf::RleVersion_1) {
    decodeWithVisitor<velox::dwrf::RleDecoderV1<false>>(
        dictIndex_.get(), visitor);
  } else {
    decodeWithVisitor<velox::dwrf::RleDecoderV2<false>>(
        dictIndex_.get(), visitor);
  }
}

} // namespace facebook::velox::dwrf
