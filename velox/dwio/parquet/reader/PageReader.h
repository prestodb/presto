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

#include <arrow/util/rle_encoding.h>
#include "velox/dwio/common/BitConcatenation.h"
#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/reader/RleDecoder.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::parquet {

/// Manages access to pages inside a ColumnChunk. Interprets page headers and
/// encodings and presents the combination of pages and encoded values as a
/// continuous stream accessible via readWithVisitor().
class PageReader {
 public:
  PageReader(
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      memory::MemoryPool& pool,
      ParquetTypeWithIdPtr nodeType,
      thrift::CompressionCodec::type codec,
      int64_t chunkSize)
      : pool_(pool),
        inputStream_(std::move(stream)),
        type_(std::move(nodeType)),
        maxRepeat_(type_->maxRepeat_),
        maxDefine_(type_->maxDefine_),
        codec_(codec),
        chunkSize_(chunkSize),
        nullConcatenation_(pool_) {}

  /// Advances 'numRows' top level rows.
  void skip(int64_t numRows);

  /// Applies 'visitor' to values in the ColumnChunk of 'this'. The
  /// operation to perform and The operand rows are given by
  /// 'visitor'. The rows are relative to the current position. The
  /// current position after readWithVisitor is immediately after the
  /// last accessed row.
  template <typename Visitor>
  void readWithVisitor(Visitor& visitor);

  // skips 'numValues' top level rows, touching null flags only. Non-null values
  // are not prepared for reading.
  void skipNullsOnly(int64_t numValues);

  /// Reads 'numValues' null flags into 'nulls' and advances the
  /// decoders by as much. The read may span several pages. If there
  /// are no nulls, buffer may be set to nullptr.
  void readNullsOnly(int64_t numValues, BufferPtr& buffer);

 private:
  // If the current page has nulls, returns a nulls bitmap owned by 'this'. This
  // is filled for 'numRows' bits.
  const uint64_t* FOLLY_NULLABLE readNulls(int32_t numRows, BufferPtr& buffer);

  // Skips the define decoder, if any, for 'numValues' top level
  // rows. Returns the number of non-nulls skipped. The range is the
  // current page.
  int32_t skipNulls(int32_t numRows);

  // True if the current page holds dictionary indices.
  bool isDictionary() const {
    return encoding_ == thrift::Encoding::PLAIN_DICTIONARY ||
        encoding_ == thrift::Encoding::RLE_DICTIONARY;
  }

  // Initializes a filter result cache for the dictionary in 'state'.
  void makeFilterCache(dwio::common::ScanState& state);

  // Makes a decoder based on 'encoding_' for bytes from ''pageData_' to
  // 'pageData_' + 'encodedDataSize_'.
  void makedecoder();

  // Reads and skips pages until finding a data page that contains 'row'. Reads
  // and sets 'rowOfPage_' and 'numRowsInPage_' and initializes a decoder for
  // the found page.
  void readNextPage(int64_t row);

  // Parses the PageHeader at 'inputStream_'. Will not read more than
  // 'remainingBytes' since there could be less data left in the
  // ColumnChunk than the full header size.
  thrift::PageHeader readPageHeader(int64_t remainingSize);
  void prepareDataPageV1(const thrift::PageHeader& pageHeader, int64_t row);
  void prepareDataPageV2(const thrift::PageHeader& pageHeader, int64_t row);
  void prepareDictionary(const thrift::PageHeader& pageHeader);
  void makeDecoder();

  // Returns a pointer to contiguous space for the next 'size' bytes
  // from current position. Copies data into 'copy' if the range
  // straddles buffers. Allocates or resizes 'copy' as needed.
  const char* FOLLY_NONNULL readBytes(int32_t size, BufferPtr& copy);

  // Decompresses data starting at 'pageData_', consuming 'compressedsize' and
  // producing up to 'uncompressedSize' bytes. The The start of the decoding
  // result is returned. an intermediate copy may be made in 'uncompresseddata_'
  const char* FOLLY_NONNULL uncompressData(
      const char* FOLLY_NONNULL pageData,
      uint32_t compressedSize,
      uint32_t uncompressedSize);

  template <typename T>
  T readField(const char*& ptr) {
    T data = *reinterpret_cast<const T*>(ptr);
    ptr += sizeof(T);
    return data;
  }

  // Starts iterating over 'rows', which may span multiple pages. 'rows' are
  // relative to current position, with 0 meaning the first
  // unprocessed value in the current page, i.e. the row after the
  // last row touched on a previous call to skip() or
  // readWithVisitor(). This is the first row of the first data page
  // if first call.
  void startVisit(folly::Range<const vector_size_t*> rows);

  // Seeks to the next page in a range given by startVisit().  Returns
  // true if there are unprocessed rows in the set given to
  // startVisit(). Seeks 'this' to the appropriate page and sets
  // 'rowsForPage' to refer to the subset of 'rows' that are on the
  // current page. The numbers in rowsForPage are relative to the
  // first unprocessed value on the page, for a new page 0 means the
  // first value. Reads possible nulls and sets 'reader's
  // nullsInReadRange_' to that or to nullptr if no null flags. Returns the data
  // of nullsInReadRange in 'nulls'.
  bool rowsForPage(
      dwio::common::SelectiveColumnReader& reader,
      folly::Range<const vector_size_t*>& rows,
      const uint64_t* FOLLY_NULLABLE& nulls);

  memory::MemoryPool& pool_;

  std::unique_ptr<dwio::common::SeekableInputStream> inputStream_;
  ParquetTypeWithIdPtr type_;
  const int32_t maxRepeat_;
  const int32_t maxDefine_;
  const thrift::CompressionCodec::type codec_;
  const int64_t chunkSize_;
  const char* bufferStart_{nullptr};
  const char* bufferEnd_{nullptr};

  BufferPtr tempNulls_;
  BufferPtr nullsInReadRange_;
  BufferPtr multiPageNulls_;
  std::unique_ptr<RleDecoder<false>> repeatDecoder_;
  std::unique_ptr<RleDecoder<false>> defineDecoder_;

  // Encoding of current page.
  thrift::Encoding::type encoding_;

  // Row number of first value in current page from start of ColumnChunk.
  int64_t rowOfPage_{0};

  // Number of rows in current page.
  int32_t numRowsInPage_{0};

  // Copy of data if data straddles buffer boundary.
  BufferPtr pageBuffer_;

  // Uncompressed data for the page. Rep-def-data in V1, data alone in V2.
  BufferPtr uncompressedData_;

  // First byte of uncompressed encoded data. Contains the encoded data as a
  // contiguous run of bytes.
  const char* pageData_{nullptr};

  // Dictionary contents.
  dwio::common::DictionaryValues dictionary_;
  thrift::Encoding::type dictionaryEncoding_;

  // Offset of current page's header from start of ColumnChunk.
  uint64_t pageStart_{0};

  // Offset of first byte after current page' header.
  uint64_t pageDataStart_{0};

  // Number of bytes starting at pageData_ for current encoded data.
  int32_t encodedDataSize_{0};

  // Below members Keep state between calls to readWithVisitor().

  // Original rows in Visitor.
  const vector_size_t* FOLLY_NULLABLE visitorRows_{nullptr};
  int32_t numVisitorRows_{0};

  // 'rowOfPage_' at the start of readWithVisitor().
  int64_t initialRowOfPage_{0};

  // Index in 'visitorRows_' for the first row that is beyond the
  // current page. Equals 'numVisitorRows_' if all are on current page.
  int32_t currentVisitorRow_{0};

  // Row relative to ColumnChunk for first unvisited row. 0 if nothing
  // visited.  The rows passed to readWithVisitor from rowsForPage()
  // are relative to this.
  int64_t firstUnvisited_{0};

  // Offset of 'visitorRows_[0]' relative too start of ColumnChunk.
  int64_t visitBase_{0};

  //  Temporary for rewriting rows to access in readWithVisitor when moving
  //  between pages. Initialized from the visitor.
  raw_vector<vector_size_t>* FOLLY_NULLABLE rowsCopy_{nullptr};

  // If 'rowsCopy_' is used, this is the difference between the rows in
  // 'rowsCopy_' and the row numbers in 'rows' given to readWithVisitor().
  int32_t rowNumberBias_{0};

  // Manages concatenating null flags read from multiple pages. If a
  // readWithVisitor is contined in one page, the visitor places the
  // nulls in the reader. If many pages are covered, some with and
  // some without nulls, we must make a a concatenated null flags to
  // return to the caller.
  dwio::common::BitConcatenation nullConcatenation_;

  // Decoders. Only one will be set at a time.
  std::unique_ptr<dwio::common::DirectDecoder<true>> directDecoder_;
  std::unique_ptr<RleDecoder<false>> rleDecoder_;
  // Add decoders for other encodings here.
};

template <typename Visitor>
void PageReader::readWithVisitor(Visitor& visitor) {
  constexpr bool hasFilter =
      !std::is_same<typename Visitor::FilterType, common::AlwaysTrue>::value;
  constexpr bool filterOnly =
      std::is_same<typename Visitor::Extract, dwio::common::DropValues>::value;
  bool mayProduceNulls = !filterOnly && visitor.allowNulls();
  auto rows = visitor.rows();
  auto numRows = visitor.numRows();
  auto& reader = visitor.reader();
  startVisit(folly::Range<const vector_size_t*>(rows, numRows));
  rowsCopy_ = &visitor.rowsCopy();
  folly::Range<const vector_size_t*> pageRows;
  const uint64_t* nulls = nullptr;
  bool isMultiPage = false;
  while (rowsForPage(reader, pageRows, nulls)) {
    bool nullsFromFastPath = false;
    int32_t numValuesBeforePage = reader.numValues();
    visitor.setNumValuesBias(numValuesBeforePage);
    visitor.setRows(pageRows);
    auto& scanState = reader.scanState();
    bool useDictionary = false;
    if (isDictionary()) {
      useDictionary = true;
      if (scanState.dictionary.values != dictionary_.values) {
        scanState.dictionary = dictionary_;
        if (hasFilter) {
          makeFilterCache(scanState);
        }
        scanState.updateRawState();
      }
    } else {
      if (scanState.dictionary.values) {
        reader.dedictionarize();
        scanState.dictionary.clear();
      }
    }

    if (nulls) {
      nullsFromFastPath = dwio::common::useFastPath<Visitor, true>(visitor);
      if (useDictionary) {
        auto dictVisitor = visitor.toDictionaryColumnVisitor();
        rleDecoder_->readWithVisitor<true>(nulls, dictVisitor);
      } else {
        directDecoder_->readWithVisitor<true>(nulls, visitor);
      }
    } else {
      if (useDictionary) {
        auto dictVisitor = visitor.toDictionaryColumnVisitor();
        rleDecoder_->readWithVisitor<false>(nullptr, dictVisitor);
      } else {
        directDecoder_->readWithVisitor<false>(nulls, visitor);
      }
    }
    if (currentVisitorRow_ < numVisitorRows_ || isMultiPage) {
      if (mayProduceNulls) {
        if (!isMultiPage) {
          // Do not reuse nulls concatenation buffer if previous
          // results are hanging on to it.
          if (multiPageNulls_ && !multiPageNulls_->unique()) {
            multiPageNulls_ = nullptr;
          }
          nullConcatenation_.reset(multiPageNulls_);
        }
        if (!nulls) {
          nullConcatenation_.appendOnes(
              reader.numValues() - numValuesBeforePage);
        } else if (reader.returnReaderNulls()) {
          // Nulls from decoding go directly to result.
          nullConcatenation_.append(
              reader.nullsInReadRange()->template as<uint64_t>(),
              0,
              reader.numValues() - numValuesBeforePage);
        } else {
          // Add the nulls produced from the decoder to the result.
          auto firstNullIndex = nullsFromFastPath ? 0 : numValuesBeforePage;
          nullConcatenation_.append(
              reader.mutableNulls(0),
              firstNullIndex,
              firstNullIndex + reader.numValues() - numValuesBeforePage);
        }
      }
      isMultiPage = true;
    }
    // The passing rows on non-first pages are relative to the start
    // of the page, adjust them to be relative to start of this
    // read. This can happen on the first processed page as well if
    // the first page of scan did not contain any of the rows to
    // visit.
    if (hasFilter && rowNumberBias_) {
      reader.offsetOutputRows(numValuesBeforePage, rowNumberBias_);
    }
  }
  if (isMultiPage) {
    reader.setNulls(mayProduceNulls ? nullConcatenation_.buffer() : nullptr);
  }
}

} // namespace facebook::velox::parquet
