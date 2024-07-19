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

#include "velox/common/compression/Compression.h"
#include "velox/dwio/common/BitConcatenation.h"
#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/parquet/reader/BooleanDecoder.h"
#include "velox/dwio/parquet/reader/DeltaBpDecoder.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/reader/RleBpDataDecoder.h"
#include "velox/dwio/parquet/reader/StringDecoder.h"

#include <arrow/util/rle_encoding.h>

namespace facebook::velox::parquet {

/// Manages access to pages inside a ColumnChunk. Interprets page headers and
/// encodings and presents the combination of pages and encoded values as a
/// continuous stream accessible via readWithVisitor().
class PageReader {
 public:
  PageReader(
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      memory::MemoryPool& pool,
      ParquetTypeWithIdPtr fileType,
      common::CompressionKind codec,
      int64_t chunkSize,
      const date::time_zone* sessionTimezone)
      : pool_(pool),
        inputStream_(std::move(stream)),
        type_(std::move(fileType)),
        maxRepeat_(type_->maxRepeat_),
        maxDefine_(type_->maxDefine_),
        isTopLevel_(maxRepeat_ == 0 && maxDefine_ <= 1),
        codec_(codec),
        chunkSize_(chunkSize),
        nullConcatenation_(pool_),
        sessionTimezone_(sessionTimezone) {
    type_->makeLevelInfo(leafInfo_);
  }

  // This PageReader constructor is for unit test only.
  PageReader(
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      memory::MemoryPool& pool,
      common::CompressionKind codec,
      int64_t chunkSize,
      const date::time_zone* sessionTimezone = nullptr)
      : pool_(pool),
        inputStream_(std::move(stream)),
        maxRepeat_(0),
        maxDefine_(1),
        isTopLevel_(maxRepeat_ == 0 && maxDefine_ <= 1),
        codec_(codec),
        chunkSize_(chunkSize),
        nullConcatenation_(pool_),
        sessionTimezone_(sessionTimezone) {}

  /// Advances 'numRows' top level rows.
  void skip(int64_t numRows);

  /// Decodes repdefs for 'numTopLevelRows'. Use getLengthsAndNulls()
  /// to access the lengths and nulls for the different nesting
  /// levels.
  void decodeRepDefs(int32_t numTopLevelRows);

  /// Returns lengths and/or nulls from 'begin' to 'end' for the level of
  /// 'info' using 'mode'. 'maxItems' is the maximum number of nulls and lengths
  /// to produce. 'lengths' is only filled for mode kList. 'nulls' is filled
  /// from bit position 'nullsStartIndex'. Returns the number of lengths/nulls
  /// filled.
  int32_t getLengthsAndNulls(
      LevelMode mode,
      const arrow::LevelInfo& info,
      int32_t begin,
      int32_t end,
      int32_t maxItems,
      int32_t* lengths,
      uint64_t* nulls,
      int32_t nullsStartIndex) const;

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

  // Returns the current string dictionary as a FlatVector<StringView>.
  const VectorPtr& dictionaryValues(const TypePtr& type);

  // True if the current page holds dictionary indices.
  bool isDictionary() const {
    return encoding_ == thrift::Encoding::PLAIN_DICTIONARY ||
        encoding_ == thrift::Encoding::RLE_DICTIONARY;
  }

  void clearDictionary() {
    dictionary_.clear();
    dictionaryValues_.reset();
  }

  bool isDeltaBinaryPacked() const {
    return encoding_ == thrift::Encoding::DELTA_BINARY_PACKED;
  }

  /// Returns the range of repdefs for the top level rows covered by the last
  /// decoderepDefs().
  std::pair<int32_t, int32_t> repDefRange() const {
    return {repDefBegin_, repDefEnd_};
  }

  // Parses the PageHeader at 'inputStream_', and move the bufferStart_ and
  // bufferEnd_ to the corresponding positions.
  thrift::PageHeader readPageHeader();

  const date::time_zone* sessionTimezone() const {
    return sessionTimezone_;
  }

 private:
  // Indicates that we only want the repdefs for the next page. Used when
  // prereading repdefs with seekToPage.
  static constexpr int64_t kRepDefOnly = -1;

  // In 'numRowsInPage_', indicates that the page's def levels must be
  // consulted to determine number of leaf values.
  static constexpr int32_t kRowsUnknown = -1;

  // If the current page has nulls, returns a nulls bitmap owned by 'this'. This
  // is filled for 'numRows' bits.
  const uint64_t* readNulls(int32_t numRows, BufferPtr& buffer);

  // Skips the define decoder, if any, for 'numValues' top level
  // rows. Returns the number of non-nulls skipped. The range is the
  // current page.
  int32_t skipNulls(int32_t numRows);

  // Initializes a filter result cache for the dictionary in 'state'.
  void makeFilterCache(dwio::common::ScanState& state);

  // Makes a decoder based on 'encoding_' for bytes from ''pageData_' to
  // 'pageData_' + 'encodedDataSize_'.
  void makedecoder();

  // Reads and skips pages until finding a data page that contains
  // 'row'. Reads and sets 'rowOfPage_' and 'numRowsInPage_' and
  // initializes a decoder for the found page. row kRepDefOnly means
  // getting repdefs for the next page. If non-top level column, 'row'
  // is interpreted in terms of leaf rows, including leaf
  // nulls. Seeking ahead of pages covered by decodeRepDefs is not
  // allowed for non-top level columns.
  void seekToPage(int64_t row);

  // Preloads the repdefs for the column chunk. To avoid preloading,
  // would need a way too clone the input stream so that one stream
  // reads ahead for repdefs and the other tracks the data. This is
  // supported by CacheInputStream but not the other
  // SeekableInputStreams.
  void preloadRepDefs();

  // Sets row number info after reading a page header. If 'forRepDef',
  // does not set non-top level row numbers by repdefs. This is on
  // when seeking a non-top level page for the first time, i.e. for
  // getting the repdefs.
  void setPageRowInfo(bool forRepDef);

  // Updates row position / rep defs consumed info to refer to the first of the
  // next page.
  void updateRowInfoAfterPageSkipped();

  void prepareDataPageV1(const thrift::PageHeader& pageHeader, int64_t row);
  void prepareDataPageV2(const thrift::PageHeader& pageHeader, int64_t row);
  void prepareDictionary(const thrift::PageHeader& pageHeader);
  void makeDecoder();

  // For a non-top level leaf, reads the defs and sets 'leafNulls_' and
  // 'numRowsInPage_' accordingly. This is used for non-top level leaves when
  // 'hasChunkRepDefs_' is false.
  void readPageDefLevels();

  // Returns a pointer to contiguous space for the next 'size' bytes
  // from current position. Copies data into 'copy' if the range
  // straddles buffers. Allocates or resizes 'copy' as needed.
  const char* readBytes(int32_t size, BufferPtr& copy);

  // Decompresses data starting at 'pageData_', consuming 'compressedsize' and
  // producing up to 'uncompressedSize' bytes. The start of the decoding
  // result is returned. an intermediate copy may be made in 'decompresseddata_'
  const char* decompressData(
      const char* pageData,
      uint32_t compressedSize,
      uint32_t uncompressedSize);

  template <typename T>
  T readField(const char* FOLLY_NONNULL& ptr) {
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
  // nullsInReadRange_' to that or to nullptr if no null
  // flags. Returns the data of nullsInReadRange in 'nulls'. Copies
  // dictionary information into 'reader'. If 'hasFilter' is true,
  // sets up dictionary hit cache. If the new page is direct and
  // previous pages are dictionary, converts any accumulated results
  // into flat. 'mayProduceNulls' should be true if nulls may occur in
  // the result if they occur in the data.
  bool rowsForPage(
      dwio::common::SelectiveColumnReader& reader,
      bool hasFilter,
      bool mayProduceNulls,
      folly::Range<const vector_size_t*>& rows,
      const uint64_t* FOLLY_NULLABLE& nulls);

  // Calls the visitor, specialized on the data type since not all visitors
  // apply to all types.
  template <
      typename Visitor,
      typename std::enable_if<
          !std::is_same_v<typename Visitor::DataType, folly::StringPiece> &&
              !std::is_same_v<typename Visitor::DataType, int8_t>,
          int>::type = 0>
  void
  callDecoder(const uint64_t* nulls, bool& nullsFromFastPath, Visitor visitor) {
    if (nulls) {
      nullsFromFastPath = dwio::common::useFastPath<Visitor, true>(visitor) &&
          (!this->type_->type()->isLongDecimal()) &&
          (this->type_->type()->isShortDecimal() ? isDictionary() : true);

      if (isDictionary()) {
        auto dictVisitor = visitor.toDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<true>(nulls, dictVisitor);
      } else if (encoding_ == thrift::Encoding::DELTA_BINARY_PACKED) {
        nullsFromFastPath = false;
        deltaBpDecoder_->readWithVisitor<true>(nulls, visitor);
      } else {
        directDecoder_->readWithVisitor<true>(
            nulls, visitor, nullsFromFastPath);
      }
    } else {
      if (isDictionary()) {
        auto dictVisitor = visitor.toDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<false>(nullptr, dictVisitor);
      } else if (encoding_ == thrift::Encoding::DELTA_BINARY_PACKED) {
        deltaBpDecoder_->readWithVisitor<false>(nulls, visitor);
      } else {
        directDecoder_->readWithVisitor<false>(
            nulls, visitor, !this->type_->type()->isShortDecimal());
      }
    }
  }

  template <
      typename Visitor,
      typename std::enable_if<
          std::is_same_v<typename Visitor::DataType, folly::StringPiece>,
          int>::type = 0>
  void
  callDecoder(const uint64_t* nulls, bool& nullsFromFastPath, Visitor visitor) {
    if (nulls) {
      if (isDictionary()) {
        nullsFromFastPath = dwio::common::useFastPath<Visitor, true>(visitor);
        auto dictVisitor = visitor.toStringDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<true>(nulls, dictVisitor);
      } else {
        nullsFromFastPath = false;
        stringDecoder_->readWithVisitor<true>(nulls, visitor);
      }
    } else {
      if (isDictionary()) {
        auto dictVisitor = visitor.toStringDictionaryColumnVisitor();
        dictionaryIdDecoder_->readWithVisitor<false>(nullptr, dictVisitor);
      } else {
        stringDecoder_->readWithVisitor<false>(nulls, visitor);
      }
    }
  }

  template <
      typename Visitor,
      typename std::enable_if<
          std::is_same_v<typename Visitor::DataType, int8_t>,
          int>::type = 0>
  void
  callDecoder(const uint64_t* nulls, bool& nullsFromFastPath, Visitor visitor) {
    VELOX_CHECK(!isDictionary(), "BOOLEAN types are never dictionary-encoded")
    if (nulls) {
      nullsFromFastPath = false;
      booleanDecoder_->readWithVisitor<true>(nulls, visitor);
    } else {
      booleanDecoder_->readWithVisitor<false>(nulls, visitor);
    }
  }

  // Returns the number of passed rows/values gathered by
  // 'reader'. Only numRows() is set for a filter-only case, only
  // numValues() is set for a non-filtered case.
  template <bool hasFilter>
  static int32_t numRowsInReader(
      const dwio::common::SelectiveColumnReader& reader) {
    if (hasFilter) {
      return reader.numRows();
    } else {
      return reader.numValues();
    }
  }

  memory::MemoryPool& pool_;

  std::unique_ptr<dwio::common::SeekableInputStream> inputStream_;
  ParquetTypeWithIdPtr type_;
  const int32_t maxRepeat_;
  const int32_t maxDefine_;
  const bool isTopLevel_;

  const common::CompressionKind codec_;
  const int64_t chunkSize_;
  const char* bufferStart_{nullptr};
  const char* bufferEnd_{nullptr};
  BufferPtr tempNulls_;
  BufferPtr nullsInReadRange_;
  BufferPtr multiPageNulls_;
  // Decoder for single bit definition levels. the arrow decoders are used for
  // multibit levels pending fixing RleBpDecoder for the case.
  std::unique_ptr<RleBpDecoder> defineDecoder_;
  std::unique_ptr<::arrow::util::RleDecoder> repeatDecoder_;
  std::unique_ptr<::arrow::util::RleDecoder> wideDefineDecoder_;

  // True for a leaf column for which repdefs are loaded for the whole column
  // chunk. This is typically the leaftmost leaf of a list. Other leaves under
  // the list can read repdefs as they go since the list lengths are already
  // known.
  bool hasChunkRepDefs_{false};

  // index of current page in 'numLeavesInPage_' -1 means before first page.
  int32_t pageIndex_{-1};

  // Number of leaf values in each data page of column chunk.
  std::vector<int32_t> numLeavesInPage_;

  // First position in '*levels_' for the range of last decodeRepDefs().
  int32_t repDefBegin_{0};

  // First position in '*levels_' after the range covered in last
  // decodeRepDefs().
  int32_t repDefEnd_{0};

  // Definition levels for the column chunk.
  raw_vector<int16_t> definitionLevels_;

  // Repetition levels for the column chunk.
  raw_vector<int16_t> repetitionLevels_;

  // Number of valid bits in 'leafNulls_'
  int32_t leafNullsSize_{0};

  // Number of leaf nulls read.
  int32_t numLeafNullsConsumed_{0};

  // Leaf nulls extracted from 'repetitionLevels_/definitionLevels_'
  raw_vector<uint64_t> leafNulls_;

  // Encoding of current page.
  thrift::Encoding::type encoding_;

  // Row number of first value in current page from start of ColumnChunk.
  int64_t rowOfPage_{0};

  // Number of rows in current page.
  int32_t numRowsInPage_{0};

  // Number of repdefs in page. Not the same as number of rows for a non-top
  // level column.
  int32_t numRepDefsInPage_{0};

  // Copy of data if data straddles buffer boundary.
  BufferPtr pageBuffer_;

  // decompressed data for the page. Rep-def-data in V1, data alone in V2.
  BufferPtr decompressedData_;

  // First byte of decompressed encoded data. Contains the encoded data as a
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
  const vector_size_t* visitorRows_{nullptr};
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
  raw_vector<vector_size_t>* rowsCopy_{nullptr};

  // If 'rowsCopy_' is used, this is the difference between the rows in
  // 'rowsCopy_' and the row numbers in 'rows' given to readWithVisitor().
  int32_t rowNumberBias_{0};

  // Manages concatenating null flags read from multiple pages. If a
  // readWithVisitor is contined in one page, the visitor places the
  // nulls in the reader. If many pages are covered, some with and
  // some without nulls, we must make a a concatenated null flags to
  // return to the caller.
  dwio::common::BitConcatenation nullConcatenation_;

  // LevelInfo for reading nulls for the leaf column 'this' represents.
  arrow::LevelInfo leafInfo_;

  // Base values of dictionary when reading a string dictionary.
  VectorPtr dictionaryValues_;

  const date::time_zone* sessionTimezone_{nullptr};

  // Decoders. Only one will be set at a time.
  std::unique_ptr<dwio::common::DirectDecoder<true>> directDecoder_;
  std::unique_ptr<RleBpDataDecoder> dictionaryIdDecoder_;
  std::unique_ptr<StringDecoder> stringDecoder_;
  std::unique_ptr<BooleanDecoder> booleanDecoder_;
  std::unique_ptr<DeltaBpDecoder> deltaBpDecoder_;
  // Add decoders for other encodings here.
};

FOLLY_ALWAYS_INLINE dwio::common::compression::CompressionOptions
getParquetDecompressionOptions(common::CompressionKind kind) {
  dwio::common::compression::CompressionOptions options;

  if (kind == common::CompressionKind_ZLIB ||
      kind == common::CompressionKind_GZIP) {
    options.format.zlib.windowBits =
        dwio::common::compression::Compressor::PARQUET_ZLIB_WINDOW_BITS;
  } else if (
      kind == common::CompressionKind_LZ4 ||
      kind == common::CompressionKind_LZO) {
    options.format.lz4_lzo.isHadoopFrameFormat = true;
  }
  return options;
}

template <typename Visitor>
void PageReader::readWithVisitor(Visitor& visitor) {
  constexpr bool hasFilter =
      !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
  constexpr bool filterOnly =
      std::is_same_v<typename Visitor::Extract, dwio::common::DropValues>;
  bool mayProduceNulls = !filterOnly && visitor.allowNulls();
  auto rows = visitor.rows();
  auto numRows = visitor.numRows();
  auto& reader = visitor.reader();
  startVisit(folly::Range<const vector_size_t*>(rows, numRows));
  rowsCopy_ = &visitor.rowsCopy();
  folly::Range<const vector_size_t*> pageRows;
  const uint64_t* nulls = nullptr;
  bool isMultiPage = false;
  while (rowsForPage(reader, hasFilter, mayProduceNulls, pageRows, nulls)) {
    bool nullsFromFastPath = false;
    int32_t numValuesBeforePage = numRowsInReader<hasFilter>(reader);
    visitor.setNumValuesBias(numValuesBeforePage);
    visitor.setRows(pageRows);
    callDecoder(nulls, nullsFromFastPath, visitor);
    if (encoding_ == thrift::Encoding::DELTA_BINARY_PACKED &&
        deltaBpDecoder_->validValuesCount() == 0) {
      VELOX_DCHECK(
          deltaBpDecoder_->bufferStart() == pageData_ + encodedDataSize_,
          "Once all data in the delta binary packed decoder has been read, "
          "its buffer ptr should be moved to the end of the page.");
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
              numRowsInReader<hasFilter>(reader) - numValuesBeforePage);
        } else if (reader.returnReaderNulls()) {
          // Nulls from decoding go directly to result.
          nullConcatenation_.append(
              reader.nullsInReadRange()->template as<uint64_t>(),
              0,
              numRowsInReader<hasFilter>(reader) - numValuesBeforePage);
        } else {
          // Add the nulls produced from the decoder to the result.
          auto firstNullIndex = nullsFromFastPath ? 0 : numValuesBeforePage;
          nullConcatenation_.append(
              reader.mutableNulls(0),
              firstNullIndex,
              firstNullIndex + numRowsInReader<hasFilter>(reader) -
                  numValuesBeforePage);
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
