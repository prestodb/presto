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

#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/ColumnVisitors.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"
#include "velox/vector/FlatVector.h"

#include <thrift/protocol/TCompactProtocol.h> // @manual

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::parquet {

using thrift::Encoding;
using thrift::PageHeader;

struct __attribute__((__packed__)) Int96Timestamp {
  int32_t days;
  uint64_t nanos;
};

void PageReader::seekToPage(int64_t row) {
  defineDecoder_.reset();
  repeatDecoder_.reset();
  // 'rowOfPage_' is the row number of the first row of the next page.
  rowOfPage_ += numRowsInPage_;
  for (;;) {
    auto dataStart = pageStart_;
    if (chunkSize_ <= pageStart_) {
      // This may happen if seeking to exactly end of row group.
      numRepDefsInPage_ = 0;
      numRowsInPage_ = 0;
      break;
    }
    PageHeader pageHeader = readPageHeader();
    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;

    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE:
        prepareDataPageV1(pageHeader, row);
        break;
      case thrift::PageType::DATA_PAGE_V2:
        prepareDataPageV2(pageHeader, row);
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        if (row == kRepDefOnly) {
          skipBytes(
              pageHeader.compressed_page_size,
              inputStream_.get(),
              bufferStart_,
              bufferEnd_);
          continue;
        }
        prepareDictionary(pageHeader);
        continue;
      default:
        break; // ignore INDEX page type and any other custom extensions
    }
    if (row == kRepDefOnly || row < rowOfPage_ + numRowsInPage_) {
      break;
    }
    updateRowInfoAfterPageSkipped();
  }
}

PageHeader PageReader::readPageHeader() {
  TestValue::adjust(
      "facebook::velox::parquet::PageReader::readPageHeader", this);
  if (bufferEnd_ == bufferStart_) {
    const void* buffer;
    int32_t size;
    inputStream_->Next(&buffer, &size);
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + size;
  }

  std::shared_ptr<thrift::ThriftTransport> transport =
      std::make_shared<thrift::ThriftStreamingTransport>(
          inputStream_.get(), bufferStart_, bufferEnd_);
  apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport> protocol(
      transport);
  PageHeader pageHeader;
  uint64_t readBytes;
  readBytes = pageHeader.read(&protocol);

  pageDataStart_ = pageStart_ + readBytes;
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

const char* PageReader::decompressData(
    const char* pageData,
    uint32_t compressedSize,
    uint32_t uncompressedSize) {
  std::unique_ptr<dwio::common::SeekableInputStream> inputStream =
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          pageData, compressedSize, 0);
  auto streamDebugInfo =
      fmt::format("Page Reader: Stream {}", inputStream_->getName());
  std::unique_ptr<dwio::common::SeekableInputStream> decompressedStream =
      dwio::common::compression::createDecompressor(
          codec_,
          std::move(inputStream),
          uncompressedSize,
          pool_,
          getParquetDecompressionOptions(codec_),
          streamDebugInfo,
          nullptr,
          true,
          compressedSize);

  dwio::common::ensureCapacity<char>(
      decompressedData_, uncompressedSize, &pool_);
  decompressedStream->readFully(
      decompressedData_->asMutable<char>(), uncompressedSize);

  return decompressedData_->as<char>();
}

void PageReader::setPageRowInfo(bool forRepDef) {
  if (isTopLevel_ || forRepDef || maxRepeat_ == 0) {
    numRowsInPage_ = numRepDefsInPage_;
  } else if (hasChunkRepDefs_) {
    ++pageIndex_;
    VELOX_CHECK_LT(
        pageIndex_,
        numLeavesInPage_.size(),
        "Seeking past known repdefs for non top level column page {}",
        pageIndex_);
    numRowsInPage_ = numLeavesInPage_[pageIndex_];
  } else {
    numRowsInPage_ = kRowsUnknown;
  }
}

void PageReader::readPageDefLevels() {
  VELOX_CHECK(kRowsUnknown == numRowsInPage_ || maxDefine_ > 1);
  definitionLevels_.resize(numRepDefsInPage_);
  wideDefineDecoder_->GetBatch(definitionLevels_.data(), numRepDefsInPage_);
  leafNulls_.resize(bits::nwords(numRepDefsInPage_));
  leafNullsSize_ = getLengthsAndNulls(
      LevelMode::kNulls,
      leafInfo_,

      0,
      numRepDefsInPage_,
      numRepDefsInPage_,
      nullptr,
      leafNulls_.data(),
      0);
  numRowsInPage_ = leafNullsSize_;
  numLeafNullsConsumed_ = 0;
}

void PageReader::updateRowInfoAfterPageSkipped() {
  rowOfPage_ += numRowsInPage_;
  if (hasChunkRepDefs_) {
    numLeafNullsConsumed_ = rowOfPage_;
  }
}

void PageReader::prepareDataPageV1(const PageHeader& pageHeader, int64_t row) {
  VELOX_CHECK(
      pageHeader.type == thrift::PageType::DATA_PAGE &&
      pageHeader.__isset.data_page_header);
  numRepDefsInPage_ = pageHeader.data_page_header.num_values;
  setPageRowInfo(row == kRepDefOnly);
  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
      numRowsInPage_ + rowOfPage_ <= row) {
    dwio::common::skipBytes(
        pageHeader.compressed_page_size,
        inputStream_.get(),
        bufferStart_,
        bufferEnd_);

    return;
  }
  pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
  pageData_ = decompressData(
      pageData_,
      pageHeader.compressed_page_size,
      pageHeader.uncompressed_page_size);
  auto pageEnd = pageData_ + pageHeader.uncompressed_page_size;
  if (maxRepeat_ > 0) {
    uint32_t repeatLength = readField<int32_t>(pageData_);
    repeatDecoder_ = std::make_unique<::arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        ::arrow::bit_util::NumRequiredBits(maxRepeat_));

    pageData_ += repeatLength;
  }

  if (maxDefine_ > 0) {
    auto defineLength = readField<uint32_t>(pageData_);
    if (maxDefine_ == 1) {
      defineDecoder_ = std::make_unique<RleBpDecoder>(
          pageData_,
          pageData_ + defineLength,
          ::arrow::bit_util::NumRequiredBits(maxDefine_));
    }
    wideDefineDecoder_ = std::make_unique<::arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        defineLength,
        ::arrow::bit_util::NumRequiredBits(maxDefine_));
    pageData_ += defineLength;
  }
  encodedDataSize_ = pageEnd - pageData_;

  encoding_ = pageHeader.data_page_header.encoding;
  if (!hasChunkRepDefs_ && (numRowsInPage_ == kRowsUnknown || maxDefine_ > 1)) {
    readPageDefLevels();
  }

  if (row != kRepDefOnly) {
    makeDecoder();
  }
}

void PageReader::prepareDataPageV2(const PageHeader& pageHeader, int64_t row) {
  VELOX_CHECK(pageHeader.__isset.data_page_header_v2);
  numRepDefsInPage_ = pageHeader.data_page_header_v2.num_values;
  setPageRowInfo(row == kRepDefOnly);
  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
      numRowsInPage_ + rowOfPage_ <= row) {
    skipBytes(
        pageHeader.compressed_page_size,
        inputStream_.get(),
        bufferStart_,
        bufferEnd_);
    return;
  }

  uint32_t defineLength =
      pageHeader.data_page_header_v2.definition_levels_byte_length;
  uint32_t repeatLength =
      pageHeader.data_page_header_v2.repetition_levels_byte_length;

  auto bytes = pageHeader.compressed_page_size;
  pageData_ = readBytes(bytes, pageBuffer_);

  if (repeatLength) {
    repeatDecoder_ = std::make_unique<::arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        ::arrow::bit_util::NumRequiredBits(maxRepeat_));
  }

  if (maxDefine_ > 0) {
    defineDecoder_ = std::make_unique<RleBpDecoder>(
        pageData_ + repeatLength,
        pageData_ + repeatLength + defineLength,
        ::arrow::bit_util::NumRequiredBits(maxDefine_));
  }
  auto levelsSize = repeatLength + defineLength;
  pageData_ += levelsSize;
  if (pageHeader.data_page_header_v2.__isset.is_compressed &&
      pageHeader.data_page_header_v2.is_compressed &&
      (pageHeader.compressed_page_size - levelsSize > 0)) {
    pageData_ = decompressData(
        pageData_,
        pageHeader.compressed_page_size - levelsSize,
        pageHeader.uncompressed_page_size - levelsSize);
  }
  if (row == kRepDefOnly) {
    skipBytes(bytes, inputStream_.get(), bufferStart_, bufferEnd_);
    return;
  }

  encodedDataSize_ = pageHeader.uncompressed_page_size - levelsSize;
  encoding_ = pageHeader.data_page_header_v2.encoding;
  if (numRowsInPage_ == kRowsUnknown) {
    readPageDefLevels();
  }
  if (row != kRepDefOnly) {
    makeDecoder();
  }
}

void PageReader::prepareDictionary(const PageHeader& pageHeader) {
  dictionary_.numValues = pageHeader.dictionary_page_header.num_values;
  dictionaryEncoding_ = pageHeader.dictionary_page_header.encoding;
  dictionary_.sorted = pageHeader.dictionary_page_header.__isset.is_sorted &&
      pageHeader.dictionary_page_header.is_sorted;
  VELOX_CHECK(
      dictionaryEncoding_ == Encoding::PLAIN_DICTIONARY ||
      dictionaryEncoding_ == Encoding::PLAIN);

  if (codec_ != common::CompressionKind::CompressionKind_NONE) {
    pageData_ = readBytes(pageHeader.compressed_page_size, pageBuffer_);
    pageData_ = decompressData(
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
      if (type_->type()->isShortDecimal() &&
          parquetType == thrift::Type::INT32) {
        auto veloxTypeLength = type_->type()->cppSizeInBytes();
        auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
        dictionary_.values =
            AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      } else {
        dictionary_.values = AlignedBuffer::allocate<char>(numBytes, &pool_);
      }
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
      if (type_->type()->isShortDecimal() &&
          parquetType == thrift::Type::INT32) {
        auto values = dictionary_.values->asMutable<int64_t>();
        auto parquetValues = dictionary_.values->asMutable<int32_t>();
        for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
          // Expand the Parquet type length values to Velox type length.
          // We start from the end to allow in-place expansion.
          values[i] = parquetValues[i];
        }
      }
      break;
    }
    case thrift::Type::INT96: {
      auto numVeloxBytes = dictionary_.numValues * sizeof(Timestamp);
      dictionary_.values = AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      auto numBytes = dictionary_.numValues * sizeof(Int96Timestamp);
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
      // Expand the Parquet type length values to Velox type length.
      // We start from the end to allow in-place expansion.
      auto values = dictionary_.values->asMutable<Timestamp>();
      auto parquetValues = dictionary_.values->asMutable<char>();

      for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
        // Convert the timestamp into seconds and nanos since the Unix epoch,
        // 00:00:00.000000 on 1 January 1970.
        int64_t nanos;
        memcpy(
            &nanos,
            parquetValues + i * sizeof(Int96Timestamp),
            sizeof(int64_t));
        int32_t days;
        memcpy(
            &days,
            parquetValues + i * sizeof(Int96Timestamp) + sizeof(int64_t),
            sizeof(int32_t));
        values[i] = Timestamp::fromDaysAndNanos(days, nanos);
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
    case thrift::Type::FIXED_LEN_BYTE_ARRAY: {
      auto parquetTypeLength = type_->typeLength_;
      auto numParquetBytes = dictionary_.numValues * parquetTypeLength;
      auto veloxTypeLength = type_->type()->cppSizeInBytes();
      auto numVeloxBytes = dictionary_.numValues * veloxTypeLength;
      dictionary_.values = AlignedBuffer::allocate<char>(numVeloxBytes, &pool_);
      auto data = dictionary_.values->asMutable<char>();
      // Read the data bytes.
      if (pageData_) {
        memcpy(data, pageData_, numParquetBytes);
      } else {
        dwio::common::readBytes(
            numParquetBytes,
            inputStream_.get(),
            data,
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type()->isShortDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int64_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int64_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int64_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = __builtin_bswap64(values[i]);
        }
        break;
      } else if (type_->type()->isLongDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numVeloxBytes) {
          auto values = dictionary_.values->asMutable<int128_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Velox type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int128_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + veloxTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int128_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = bits::builtin_bswap128(values[i]);
        }
        break;
      }
      VELOX_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
    }
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
    case thrift::Type::INT96:
      return 12;
    default:
      VELOX_FAIL("Type does not have a byte width {}", type);
  }
}
} // namespace

void PageReader::preloadRepDefs() {
  hasChunkRepDefs_ = true;
  while (pageStart_ < chunkSize_) {
    seekToPage(kRepDefOnly);
    auto begin = definitionLevels_.size();
    auto numLevels = definitionLevels_.size() + numRepDefsInPage_;
    definitionLevels_.resize(numLevels);
    wideDefineDecoder_->GetBatch(
        definitionLevels_.data() + begin, numRepDefsInPage_);
    if (repeatDecoder_) {
      repetitionLevels_.resize(numLevels);

      repeatDecoder_->GetBatch(
          repetitionLevels_.data() + begin, numRepDefsInPage_);
    }
    leafNulls_.resize(bits::nwords(leafNullsSize_ + numRepDefsInPage_));
    auto numLeaves = getLengthsAndNulls(
        LevelMode::kNulls,
        leafInfo_,
        begin,
        begin + numRepDefsInPage_,
        numRepDefsInPage_,
        nullptr,
        leafNulls_.data(),
        leafNullsSize_);
    leafNullsSize_ += numLeaves;
    numLeavesInPage_.push_back(numLeaves);
  }

  // Reset the input to start of column chunk.
  std::vector<uint64_t> rewind = {0};
  pageStart_ = 0;
  dwio::common::PositionProvider position(rewind);
  inputStream_->seekToPosition(position);
  bufferStart_ = bufferEnd_ = nullptr;
  rowOfPage_ = 0;
  numRowsInPage_ = 0;
  pageData_ = nullptr;
}

void PageReader::decodeRepDefs(int32_t numTopLevelRows) {
  if (definitionLevels_.empty() && maxDefine_ > 0) {
    preloadRepDefs();
  }
  repDefBegin_ = repDefEnd_;
  int32_t numLevels = definitionLevels_.size();
  int32_t topFound = 0;
  int32_t i = repDefBegin_;
  if (maxRepeat_ > 0) {
    for (; i < numLevels; ++i) {
      if (repetitionLevels_[i] == 0) {
        ++topFound;
        if (topFound == numTopLevelRows + 1) {
          break;
        }
      }
    }
    repDefEnd_ = i;
  } else {
    repDefEnd_ = i + numTopLevelRows;
  }
}

int32_t PageReader::getLengthsAndNulls(
    LevelMode mode,
    const arrow::LevelInfo& info,
    int32_t begin,
    int32_t end,
    int32_t maxItems,
    int32_t* lengths,
    uint64_t* nulls,
    int32_t nullsStartIndex) const {
  arrow::ValidityBitmapInputOutput bits;
  bits.values_read_upper_bound = maxItems;
  bits.values_read = 0;
  bits.null_count = 0;
  bits.valid_bits = reinterpret_cast<uint8_t*>(nulls);
  bits.valid_bits_offset = nullsStartIndex;

  switch (mode) {
    case LevelMode::kNulls:
      DefLevelsToBitmap(
          definitionLevels_.data() + begin, end - begin, info, &bits);
      break;
    case LevelMode::kList: {
      arrow::DefRepLevelsToList(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits,
          lengths);
      // Convert offsets to lengths.
      for (auto i = 0; i < bits.values_read; ++i) {
        lengths[i] = lengths[i + 1] - lengths[i];
      }
      break;
    }
    case LevelMode::kStructOverLists: {
      DefRepLevelsToBitmap(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits);
      break;
    }
  }
  return bits.values_read;
}

void PageReader::makeDecoder() {
  auto parquetType = type_->parquetType_.value();
  switch (encoding_) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
      dictionaryIdDecoder_ = std::make_unique<RleBpDataDecoder>(
          pageData_ + 1, pageData_ + encodedDataSize_, pageData_[0]);
      break;
    case Encoding::PLAIN:
      switch (parquetType) {
        case thrift::Type::BOOLEAN:
          booleanDecoder_ = std::make_unique<BooleanDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        case thrift::Type::BYTE_ARRAY:
          stringDecoder_ = std::make_unique<StringDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        case thrift::Type::FIXED_LEN_BYTE_ARRAY:
          if (type_->type()->isVarbinary()) {
            stringDecoder_ = std::make_unique<StringDecoder>(
                pageData_, pageData_ + encodedDataSize_, type_->typeLength_);
          } else {
            directDecoder_ =
                std::make_unique<dwio::common::DirectDecoder<true>>(
                    std::make_unique<dwio::common::SeekableArrayInputStream>(
                        pageData_, encodedDataSize_),
                    false,
                    type_->typeLength_,
                    true);
          }
          break;
        default: {
          directDecoder_ = std::make_unique<dwio::common::DirectDecoder<true>>(
              std::make_unique<dwio::common::SeekableArrayInputStream>(
                  pageData_, encodedDataSize_),
              false,
              parquetTypeBytes(type_->parquetType_.value()));
        }
      }
      break;
    case Encoding::DELTA_BINARY_PACKED:
      switch (parquetType) {
        case thrift::Type::INT32:
        case thrift::Type::INT64:
          deltaBpDecoder_ = std::make_unique<DeltaBpDecoder>(pageData_);
          break;
        default:
          VELOX_UNSUPPORTED(
              "DELTA_BINARY_PACKED decoder only supports INT32 and INT64");
      }
      break;
    default:
      VELOX_UNSUPPORTED("Encoding not supported yet: {}", encoding_);
  }
}

void PageReader::skip(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    seekToPage(firstUnvisited_ + numRows);
    if (hasChunkRepDefs_) {
      numLeafNullsConsumed_ = rowOfPage_;
    }
    toSkip -= rowOfPage_ - firstUnvisited_;
  }
  firstUnvisited_ += numRows;

  // Skip nulls
  toSkip = skipNulls(toSkip);

  // Skip the decoder
  if (isDictionary()) {
    dictionaryIdDecoder_->skip(toSkip);
  } else if (directDecoder_) {
    directDecoder_->skip(toSkip);
  } else if (stringDecoder_) {
    stringDecoder_->skip(toSkip);
  } else if (booleanDecoder_) {
    booleanDecoder_->skip(toSkip);
  } else if (deltaBpDecoder_) {
    deltaBpDecoder_->skip(toSkip);
  } else {
    VELOX_FAIL("No decoder to skip");
  }
}

int32_t PageReader::skipNulls(int32_t numValues) {
  if (!defineDecoder_ && isTopLevel_) {
    return numValues;
  }
  VELOX_CHECK(1 == maxDefine_ || !leafNulls_.empty());
  dwio::common::ensureCapacity<bool>(tempNulls_, numValues, &pool_);
  tempNulls_->setSize(0);
  if (isTopLevel_) {
    bool allOnes;
    defineDecoder_->readBits(
        numValues, tempNulls_->asMutable<uint64_t>(), &allOnes);
    if (allOnes) {
      return numValues;
    }
  } else {
    readNulls(numValues, tempNulls_);
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
    seekToPage(firstUnvisited_ + numRows);
    firstUnvisited_ += numRows;
    toSkip = firstUnvisited_ - rowOfPage_;
  } else {
    firstUnvisited_ += numRows;
  }

  // Skip nulls
  skipNulls(toSkip);
}

void PageReader::readNullsOnly(int64_t numValues, BufferPtr& buffer) {
  VELOX_CHECK(!maxRepeat_);
  auto toRead = numValues;
  if (buffer) {
    dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  }
  nullConcatenation_.reset(buffer);
  while (toRead) {
    auto availableOnPage = rowOfPage_ + numRowsInPage_ - firstUnvisited_;
    if (!availableOnPage) {
      seekToPage(firstUnvisited_);
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
  if (maxDefine_ == 0) {
    buffer = nullptr;
    return nullptr;
  }
  dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  if (isTopLevel_) {
    VELOX_CHECK_EQ(1, maxDefine_);
    bool allOnes;
    defineDecoder_->readBits(
        numValues, buffer->asMutable<uint64_t>(), &allOnes);
    return allOnes ? nullptr : buffer->as<uint64_t>();
  }
  bits::copyBits(
      leafNulls_.data(),
      numLeafNullsConsumed_,
      buffer->asMutable<uint64_t>(),
      0,
      numValues);
  numLeafNullsConsumed_ += numValues;
  return buffer->as<uint64_t>();
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
    bool mayProduceNulls,
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
    seekToPage(rowZero);
    if (hasChunkRepDefs_) {
      numLeafNullsConsumed_ = rowOfPage_;
    }
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
      // If there are previous pages in the current read, nulls read
      // from them are in 'nullConcatenation_' Put this into the
      // reader for the time of dedictionarizing so we don't read
      // undefined dictionary indices.
      if (mayProduceNulls && reader.numValues()) {
        reader.setNulls(nullConcatenation_.buffer());
      }
      reader.dedictionarize();
      // The nulls across all pages are in nullConcatenation_. Clear
      // the nulls and let the prepareNulls below reserve nulls for
      // the new page.
      reader.setNulls(nullptr);
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

const VectorPtr& PageReader::dictionaryValues(const TypePtr& type) {
  if (!dictionaryValues_) {
    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &pool_,
        type,
        nullptr,
        dictionary_.numValues,
        dictionary_.values,
        std::vector<BufferPtr>{dictionary_.strings});
  }
  return dictionaryValues_;
}

} // namespace facebook::velox::parquet
