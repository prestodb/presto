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

#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"
#include "velox/vector/FlatVector.h"

#include <arrow/util/rle_encoding.h>
#include <snappy.h>
#include <thrift/protocol/TCompactProtocol.h> //@manual
#include <zlib.h>
#include <zstd.h>

namespace facebook::velox::parquet {

using thrift::Encoding;
using thrift::PageHeader;

void PageReader::readNextPage(int64_t row) {
  defineDecoder_.reset();
  repeatDecoder_.reset();
  // 'rowOfPage_' is the row number of the first row of the next page.
  rowOfPage_ += numRowsInPage_;
  for (;;) {
    PageHeader pageHeader = readPageHeader(chunkSize_ - pageStart_);
    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;

    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE:
        prepareDataPageV1(pageHeader, row);
        break;
      case thrift::PageType::DATA_PAGE_V2:
        prepareDataPageV2(pageHeader, row);
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        prepareDictionary(pageHeader);
        continue;
      default:
        break; // ignore INDEX page type and any other custom extensions
    }
    if (row < rowOfPage_ + numRowsInPage_) {
      break;
    }
    rowOfPage_ += numRowsInPage_;
    dwio::common::skipBytes(
        pageHeader.compressed_page_size,
        inputStream_.get(),
        bufferStart_,
        bufferEnd_);
  }
}

PageHeader PageReader::readPageHeader(int64_t remainingSize) {
  // Note that sizeof(PageHeader) may be longer than actually read
  std::shared_ptr<thrift::ThriftBufferedTransport> transport;
  std::unique_ptr<apache::thrift::protocol::TCompactProtocolT<
      thrift::ThriftBufferedTransport>>
      protocol;
  char copy[sizeof(PageHeader)];
  bool wasInBuffer = false;
  if (bufferEnd_ == bufferStart_) {
    const void* buffer;
    int32_t size;
    inputStream_->Next(&buffer, &size);
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + size;
  }
  if (bufferEnd_ - bufferStart_ >= sizeof(PageHeader)) {
    wasInBuffer = true;
    transport = std::make_shared<thrift::ThriftBufferedTransport>(
        bufferStart_, sizeof(PageHeader));
    protocol = std::make_unique<apache::thrift::protocol::TCompactProtocolT<
        thrift::ThriftBufferedTransport>>(transport);
  } else {
    dwio::common::readBytes(
        std::min<int64_t>(remainingSize, sizeof(PageHeader)),
        inputStream_.get(),
        &copy,
        bufferStart_,
        bufferEnd_);

    transport = std::make_shared<thrift::ThriftBufferedTransport>(
        copy, sizeof(PageHeader));
    protocol = std::make_unique<apache::thrift::protocol::TCompactProtocolT<
        thrift::ThriftBufferedTransport>>(transport);
  }
  PageHeader pageHeader;
  uint64_t readBytes = pageHeader.read(protocol.get());
  pageDataStart_ = pageStart_ + readBytes;
  // Unread the bytes that were not consumed.
  if (wasInBuffer) {
    bufferStart_ += readBytes;
  } else {
    std::vector<uint64_t> start = {pageDataStart_};
    dwio::common::PositionProvider position(start);
    inputStream_->seekToPosition(position);
    bufferStart_ = bufferEnd_ = nullptr;
  }
  return pageHeader;
}

const char* PageReader::readBytes(int32_t size, BufferPtr& copy) {
  if (bufferEnd_ == bufferStart_) {
    const void* buffer = nullptr;
    int32_t bufferSize = 0;
    if (!inputStream_->Next(&buffer, &bufferSize)) {
      VELOX_FAIL("Read past end");
    }
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + bufferSize;
  }
  if (bufferEnd_ - bufferStart_ >= size) {
    bufferStart_ += size;
    return bufferStart_ - size;
  }
  dwio::common::ensureCapacity<char>(copy, size, &pool_);
  dwio::common::readBytes(
      size,
      inputStream_.get(),
      copy->asMutable<char>(),
      bufferStart_,
      bufferEnd_);
  return copy->as<char>();
}

const char* FOLLY_NONNULL PageReader::uncompressData(
    const char* pageData,
    uint32_t compressedSize,
    uint32_t uncompressedSize) {
  switch (codec_) {
    case thrift::CompressionCodec::UNCOMPRESSED:
      return pageData;
    case thrift::CompressionCodec::SNAPPY: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);

      size_t sizeFromSnappy;
      if (!snappy::GetUncompressedLength(
              pageData, compressedSize, &sizeFromSnappy)) {
        VELOX_FAIL("Snappy uncompressed size not available");
      }
      VELOX_CHECK_EQ(uncompressedSize, sizeFromSnappy);
      snappy::RawUncompress(
          pageData, compressedSize, uncompressedData_->asMutable<char>());
      return uncompressedData_->as<char>();
    }
    case thrift::CompressionCodec::ZSTD: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);

      auto ret = ZSTD_decompress(
          uncompressedData_->asMutable<char>(),
          uncompressedSize,
          pageData,
          compressedSize);
      VELOX_CHECK(
          !ZSTD_isError(ret),
          "ZSTD returned an error: ",
          ZSTD_getErrorName(ret));
      return uncompressedData_->as<char>();
    }
    case thrift::CompressionCodec::GZIP: {
      dwio::common::ensureCapacity<char>(
          uncompressedData_, uncompressedSize, &pool_);
      z_stream stream;
      memset(&stream, 0, sizeof(stream));
      constexpr int WINDOW_BITS = 15;
      // Determine if this is libz or gzip from header.
      constexpr int DETECT_CODEC = 32;
      // Initialize decompressor.
      auto ret = inflateInit2(&stream, WINDOW_BITS | DETECT_CODEC);
      VELOX_CHECK(
          (ret == Z_OK),
          "zlib inflateInit failed: {}",
          stream.msg ? stream.msg : "");
      // Decompress.
      stream.next_in =
          const_cast<Bytef*>(reinterpret_cast<const Bytef*>(pageData));
      stream.avail_in = static_cast<uInt>(compressedSize);
      stream.next_out =
          reinterpret_cast<Bytef*>(uncompressedData_->asMutable<char>());
      stream.avail_out = static_cast<uInt>(uncompressedSize);
      ret = inflate(&stream, Z_FINISH);
      VELOX_CHECK(
          ret == Z_STREAM_END,
          "GZipCodec failed: {}",
          stream.msg ? stream.msg : "");
      return uncompressedData_->as<char>();
    }
    default:
      VELOX_FAIL("Unsupported Parquet compression type '{}'", codec_);
  }
}

void PageReader::prepareDataPageV1(const PageHeader& pageHeader, int64_t row) {
  VELOX_CHECK(
      pageHeader.type == thrift::PageType::DATA_PAGE &&
      pageHeader.__isset.data_page_header);
  numRowsInPage_ = pageHeader.data_page_header.num_values;
  if (numRowsInPage_ + rowOfPage_ <= row) {
    return;
  }
  pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
  pageData_ = uncompressData(
      pageData_,
      pageHeader.compressed_page_size,
      pageHeader.uncompressed_page_size);
  auto pageEnd = pageData_ + pageHeader.uncompressed_page_size;
  if (maxRepeat_ > 0) {
    uint32_t repeatLength = readField<int32_t>(pageData_);
    pageData_ += repeatLength;
    repeatDecoder_ = std::make_unique<RleDecoder<false>>(
        pageData_,
        pageData_ + repeatLength,
        arrow::bit_util::NumRequiredBits(maxRepeat_));
    pageData_ += repeatLength;
  }

  if (maxDefine_ > 0) {
    auto defineLength = readField<uint32_t>(pageData_);
    defineDecoder_ = std::make_unique<RleDecoder<false>>(
        pageData_,
        pageData_ + defineLength,
        arrow::bit_util::NumRequiredBits(maxDefine_));
    pageData_ += defineLength;
  }
  encodedDataSize_ = pageEnd - pageData_;

  encoding_ = pageHeader.data_page_header.encoding;
  makeDecoder();
}

void PageReader::prepareDataPageV2(const PageHeader& pageHeader, int64_t row) {
  VELOX_CHECK(pageHeader.__isset.data_page_header_v2);
  numRowsInPage_ = pageHeader.data_page_header_v2.num_values;
  if (numRowsInPage_ + rowOfPage_ <= row) {
    return;
  }

  uint32_t defineLength = maxDefine_ > 0
      ? pageHeader.data_page_header_v2.definition_levels_byte_length
      : 0;
  uint32_t repeatLength = maxRepeat_ > 0
      ? pageHeader.data_page_header_v2.repetition_levels_byte_length
      : 0;
  auto bytes = pageHeader.compressed_page_size;
  pageData_ = readBytes(bytes, pageBuffer_);

  if (repeatLength) {
    repeatDecoder_ = std::make_unique<RleDecoder<false>>(
        pageData_,
        pageData_ + repeatLength,
        arrow::bit_util::NumRequiredBits(maxRepeat_));
  }

  if (maxDefine_ > 0) {
    defineDecoder_ = std::make_unique<RleDecoder<false>>(
        pageData_ + repeatLength,
        pageData_ + repeatLength + defineLength,
        arrow::bit_util::NumRequiredBits(maxDefine_));
  }
  auto levelsSize = repeatLength + defineLength;
  pageData_ += levelsSize;
  if (pageHeader.data_page_header_v2.__isset.is_compressed ||
      pageHeader.data_page_header_v2.is_compressed) {
    pageData_ = uncompressData(
        pageData_,
        pageHeader.compressed_page_size - levelsSize,
        pageHeader.uncompressed_page_size - levelsSize);
  }
  encodedDataSize_ = pageHeader.uncompressed_page_size - levelsSize;
  encoding_ = pageHeader.data_page_header_v2.encoding;
  makeDecoder();
}

void PageReader::prepareDictionary(const PageHeader& pageHeader) {
  dictionary_.numValues = pageHeader.dictionary_page_header.num_values;
  dictionaryEncoding_ = pageHeader.dictionary_page_header.encoding;
  dictionary_.sorted = pageHeader.dictionary_page_header.__isset.is_sorted &&
      pageHeader.dictionary_page_header.is_sorted;
  VELOX_CHECK(
      dictionaryEncoding_ == Encoding::PLAIN_DICTIONARY ||
      dictionaryEncoding_ == Encoding::PLAIN);

  if (codec_ != thrift::CompressionCodec::UNCOMPRESSED) {
    pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
    pageData_ = uncompressData(
        pageData_,
        pageHeader.compressed_page_size,
        pageHeader.uncompressed_page_size);
  }

  auto parquetType = type_->parquetType_.value();
  switch (parquetType) {
    case thrift::Type::INT32:
    case thrift::Type::INT64:
    case thrift::Type::FLOAT:
    case thrift::Type::DOUBLE: {
      int32_t typeSize = (parquetType == thrift::Type::INT32 ||
                          parquetType == thrift::Type::FLOAT)
          ? sizeof(float)
          : sizeof(double);
      auto numBytes = dictionary_.numValues * typeSize;
      dictionary_.values = AlignedBuffer::allocate<char>(numBytes, &pool_);
      if (pageData_) {
        memcpy(dictionary_.values->asMutable<char>(), pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes,
            inputStream_.get(),
            dictionary_.values->asMutable<char>(),
            bufferStart_,
            bufferEnd_);
      }
      break;
    }
    case thrift::Type::BYTE_ARRAY: {
      dictionary_.values =
          AlignedBuffer::allocate<StringView>(dictionary_.numValues, &pool_);
      auto numBytes = pageHeader.uncompressed_page_size;
      auto values = dictionary_.values->asMutable<StringView>();
      dictionary_.strings = AlignedBuffer::allocate<char>(numBytes, &pool_);
      auto strings = dictionary_.strings->asMutable<char>();
      if (pageData_) {
        memcpy(strings, pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes, inputStream_.get(), strings, bufferStart_, bufferEnd_);
      }
      auto header = strings;
      for (auto i = 0; i < dictionary_.numValues; ++i) {
        auto length = *reinterpret_cast<const int32_t*>(header);
        values[i] = StringView(header + sizeof(int32_t), length);
        header += length + sizeof(int32_t);
      }
      VELOX_CHECK_EQ(header, strings + numBytes);
      break;
    }
    case thrift::Type::INT96:
    case thrift::Type::FIXED_LEN_BYTE_ARRAY:
    default:
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
  }
}

void PageReader::makeFilterCache(dwio::common::ScanState& state) {
  VELOX_CHECK(
      !state.dictionary2.values, "Parquet supports only one dictionary");
  state.filterCache.resize(state.dictionary.numValues);
  simd::memset(
      state.filterCache.data(),
      dwio::common::FilterResult::kUnknown,
      state.filterCache.size());
  state.rawState.filterCache = state.filterCache.data();
}

namespace {
int32_t parquetTypeBytes(thrift::Type::type type) {
  switch (type) {
    case thrift::Type::INT32:
    case thrift::Type::FLOAT:
      return 4;
    case thrift::Type::INT64:
    case thrift::Type::DOUBLE:
      return 8;
    default:
      VELOX_FAIL("Type does not have a byte width {}", type);
  }
}
} // namespace

void PageReader::makeDecoder() {
  auto parquetType = type_->parquetType_.value();
  switch (encoding_) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
      rleDecoder_ = std::make_unique<RleDecoder<false>>(
          pageData_ + 1, pageData_ + encodedDataSize_, pageData_[0]);
      break;
    case Encoding::PLAIN:
      switch (parquetType) {
        case thrift::Type::BYTE_ARRAY:
          stringDecoder_ = std::make_unique<StringDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        default:
          directDecoder_ = std::make_unique<dwio::common::DirectDecoder<true>>(
              std::make_unique<dwio::common::SeekableArrayInputStream>(
                  pageData_, encodedDataSize_),
              false,
              parquetTypeBytes(type_->parquetType_.value()));
      }
      break;
    case Encoding::DELTA_BINARY_PACKED:
    default:
      VELOX_UNSUPPORTED("Encoding not supported yet");
  }
}

void PageReader::skip(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    readNextPage(firstUnvisited_ + numRows);
    toSkip -= rowOfPage_ - firstUnvisited_;
  }
  firstUnvisited_ += numRows;

  // Skip nulls
  toSkip = skipNulls(toSkip);

  // Skip the decoder
  if (isDictionary()) {
    rleDecoder_->skip(toSkip);
  } else if (directDecoder_) {
    directDecoder_->skip(toSkip);
  } else if (stringDecoder_) {
    stringDecoder_->skip(toSkip);
  } else {
    VELOX_FAIL("No decoder to skip");
  }
}

int32_t PageReader::skipNulls(int32_t numValues) {
  if (!defineDecoder_) {
    return numValues;
  }
  VELOX_CHECK_EQ(1, maxDefine_);
  dwio::common::ensureCapacity<bool>(tempNulls_, numValues, &pool_);
  tempNulls_->setSize(0);
  bool allOnes;
  defineDecoder_->readBits(
      numValues, tempNulls_->asMutable<uint64_t>(), &allOnes);
  if (allOnes) {
    return numValues;
  }
  auto words = tempNulls_->as<uint64_t>();
  return bits::countBits(words, 0, numValues);
}

void PageReader::skipNullsOnly(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    readNextPage(firstUnvisited_ + numRows);
    firstUnvisited_ += numRows;
    toSkip = firstUnvisited_ - rowOfPage_;
  }
  firstUnvisited_ += numRows;

  // Skip nulls
  skipNulls(toSkip);
}

void PageReader::readNullsOnly(int64_t numValues, BufferPtr& buffer) {
  auto toRead = numValues;
  if (buffer) {
    dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  }
  nullConcatenation_.reset(buffer);
  while (toRead) {
    auto availableOnPage = rowOfPage_ + numRowsInPage_ - firstUnvisited_;
    if (!availableOnPage) {
      readNextPage(firstUnvisited_);
      availableOnPage = numRowsInPage_;
    }
    auto numRead = std::min(availableOnPage, toRead);
    auto nulls = readNulls(numRead, nullsInReadRange_);
    toRead -= numRead;
    nullConcatenation_.append(nulls, 0, numRead);
    firstUnvisited_ += numRead;
  }
  buffer = nullConcatenation_.buffer();
}

const uint64_t* FOLLY_NULLABLE
PageReader::readNulls(int32_t numValues, BufferPtr& buffer) {
  if (!defineDecoder_) {
    buffer = nullptr;
    return nullptr;
  }
  VELOX_CHECK_EQ(1, maxDefine_);
  dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  bool allOnes;
  defineDecoder_->readBits(numValues, buffer->asMutable<uint64_t>(), &allOnes);
  return allOnes ? nullptr : buffer->as<uint64_t>();
}

void PageReader::startVisit(folly::Range<const vector_size_t*> rows) {
  visitorRows_ = rows.data();
  numVisitorRows_ = rows.size();
  currentVisitorRow_ = 0;
  initialRowOfPage_ = rowOfPage_;
  visitBase_ = firstUnvisited_;
}

bool PageReader::rowsForPage(
    dwio::common::SelectiveColumnReader& reader,
    bool hasFilter,
    folly::Range<const vector_size_t*>& rows,
    const uint64_t* FOLLY_NULLABLE& nulls) {
  if (currentVisitorRow_ == numVisitorRows_) {
    return false;
  }
  int32_t numToVisit;
  // Check if the first row to go to is in the current page. If not, seek to the
  // page that contains the row.
  auto rowZero = visitBase_ + visitorRows_[currentVisitorRow_];
  if (rowZero >= rowOfPage_ + numRowsInPage_) {
    readNextPage(rowZero);
  }
  auto& scanState = reader.scanState();
  if (isDictionary()) {
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

  // Then check how many of the rows to visit are on the same page as the
  // current one.
  int32_t firstOnNextPage = rowOfPage_ + numRowsInPage_ - visitBase_;
  if (firstOnNextPage > visitorRows_[numVisitorRows_ - 1]) {
    // All the remaining rows are on this page.
    numToVisit = numVisitorRows_ - currentVisitorRow_;
  } else {
    // Find the last row in the rows to visit that is on this page.
    auto rangeLeft = folly::Range<const int32_t*>(
        visitorRows_ + currentVisitorRow_,
        numVisitorRows_ - currentVisitorRow_);
    auto it =
        std::lower_bound(rangeLeft.begin(), rangeLeft.end(), firstOnNextPage);
    assert(it != rangeLeft.end());
    assert(it != rangeLeft.begin());
    numToVisit = it - (visitorRows_ + currentVisitorRow_);
  }
  // If the page did not change and this is the first call, we can return a view
  // on the original visitor rows.
  if (rowOfPage_ == initialRowOfPage_ && currentVisitorRow_ == 0) {
    nulls =
        readNulls(visitorRows_[numToVisit - 1] + 1, reader.nullsInReadRange());
    rowNumberBias_ = 0;
    rows = folly::Range<const vector_size_t*>(visitorRows_, numToVisit);
  } else {
    // We scale row numbers to be relative to first on this page.
    auto pageOffset = rowOfPage_ - visitBase_;
    rowNumberBias_ = visitorRows_[currentVisitorRow_];
    skip(rowNumberBias_ - pageOffset);
    // The decoder is positioned at 'visitorRows_[currentVisitorRow_']'
    // We copy the rows to visit with a bias, so that the first to visit has
    // offset 0.
    rowsCopy_->resize(numToVisit);
    auto copy = rowsCopy_->data();
    // Subtract 'rowNumberBias_' from the rows to visit on this page.
    // 'copy' has a writable tail of SIMD width, so no special case for end of
    // loop.
    for (auto i = 0; i < numToVisit; i += xsimd::batch<int32_t>::size) {
      auto numbers = xsimd::batch<int32_t>::load_unaligned(
                         &visitorRows_[i + currentVisitorRow_]) -
          rowNumberBias_;
      numbers.store_unaligned(copy);
      copy += xsimd::batch<int32_t>::size;
    }
    nulls = readNulls(rowsCopy_->back() + 1, reader.nullsInReadRange());
    rows = folly::Range<const vector_size_t*>(
        rowsCopy_->data(), rowsCopy_->size());
  }
  reader.prepareNulls(rows, nulls != nullptr, currentVisitorRow_);
  currentVisitorRow_ += numToVisit;
  firstUnvisited_ = visitBase_ + visitorRows_[currentVisitorRow_ - 1] + 1;
  return true;
}

const VectorPtr& PageReader::dictionaryValues() {
  if (!dictionaryValues_) {
    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &pool_,
        VARCHAR(),
        nullptr,
        dictionary_.numValues,
        dictionary_.values,
        std::vector<BufferPtr>{dictionary_.strings});
  }
  return dictionaryValues_;
}

} // namespace facebook::velox::parquet
