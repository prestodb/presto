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

#include "velox/dwio/text/reader/TextReader.h"
#include "velox/common/encode/Base64.h"
#include "velox/dwio/common/exception/Exceptions.h"
#include "velox/type/fbhive/HiveTypeParser.h"

#include <string>

namespace facebook::velox::text {

using common::CompressionKind;

using dwio::common::EOFError;
using dwio::common::RowReader;
using dwio::common::verify;

using folly::AsciiCaseInsensitive;
using folly::StringPiece;

constexpr const char* kTextfileCompressionExtensionGzip = ".gz";
constexpr const char* kTextfileCompressionExtensionDeflate = ".deflate";
constexpr const char* kTextfileCompressionExtensionZst = ".zst";

static std::string emptyString = std::string();

namespace {

void resizeVector(
    BaseVector* FOLLY_NULLABLE data,
    const vector_size_t insertionIdx) {
  if (data == nullptr) {
    return;
  }

  auto dataSize = data->size();
  if (dataSize == 0) {
    data->resize(10);
  } else if (dataSize <= insertionIdx) {
    if (data->type()->kind() == TypeKind::ARRAY) {
      auto oldSize = dataSize;
      auto newSize = dataSize * 2;
      data->resize(newSize);

      auto arrayVector = data->asChecked<ArrayVector>();
      auto rawOffsets = arrayVector->offsets()->asMutable<vector_size_t>();
      auto rawSizes = arrayVector->sizes()->asMutable<vector_size_t>();

      auto lastOffset = oldSize > 0 ? rawOffsets[oldSize - 1] : 0;
      auto lastSize = oldSize > 0 ? rawSizes[oldSize - 1] : 0;
      auto newOffset = oldSize > 0 ? lastOffset + lastSize : 0;

      for (auto i = oldSize; i < newSize; ++i) {
        rawSizes[i] = 0;
        rawOffsets[i] = newOffset;
      }
    } else if (data->type()->kind() == TypeKind::MAP) {
      auto oldSize = dataSize;
      auto newSize = dataSize * 2;
      data->resize(newSize);

      auto mapVector = data->asChecked<MapVector>();
      auto rawOffsets = mapVector->offsets()->asMutable<vector_size_t>();
      auto rawSizes = mapVector->sizes()->asMutable<vector_size_t>();

      auto lastOffset = oldSize > 0 ? rawOffsets[oldSize - 1] : 0;
      auto lastSize = oldSize > 0 ? rawSizes[oldSize - 1] : 0;
      auto newOffset = oldSize > 0 ? lastOffset + lastSize : 0;

      for (auto i = oldSize; i < newSize; ++i) {
        rawSizes[i] = 0;
        rawOffsets[i] = newOffset;
      }
    } else {
      data->resize(dataSize * 2);
    }
  }
}

bool endsWith(const std::string& str, const std::string& suffix) {
  return str.size() >= suffix.size() &&
      str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

void setCompressionSettings(
    const std::string& filename,
    CompressionKind& kind,
    dwio::common::compression::CompressionOptions& compressionOptions) {
  if (endsWith(filename, kTextfileCompressionExtensionGzip)) {
    kind = CompressionKind::CompressionKind_GZIP;
    compressionOptions.format.zlib.windowBits =
        15; // 2^15-byte deflate window size
  } else if (endsWith(filename, kTextfileCompressionExtensionDeflate)) {
    kind = CompressionKind::CompressionKind_ZLIB;
    compressionOptions.format.zlib.windowBits =
        -15; // raw deflate, 2^15-byte window size
  } else if (endsWith(filename, kTextfileCompressionExtensionZst)) {
    kind = CompressionKind::CompressionKind_ZSTD;
  } else {
    kind = CompressionKind::CompressionKind_NONE;
  }
}

} // namespace

FileContents::FileContents(
    MemoryPool& pool,
    const std::shared_ptr<const RowType>& t)
    : schema{t},
      input{nullptr},
      pool{pool},
      fileLength{0},
      compression{CompressionKind::CompressionKind_NONE},
      compressionOptions{},
      needsEscape{} {
  needsEscape.fill(false);
  needsEscape.at(0) = true;
}

TextRowReader::TextRowReader(
    std::shared_ptr<FileContents> fileContents,
    const RowReaderOptions& opts)
    : RowReader(),
      contents_{fileContents},
      schemaWithId_{TypeWithId::create(fileContents->schema)},
      scanSpec_{opts.scanSpec()},
      selectedSchema_{nullptr},
      options_{opts},
      columnSelector_{
          ColumnSelector::apply(opts.selector(), contents_->schema)},
      currentRow_{0},
      pos_{opts.offset()},
      atEOL_{false},
      atEOF_{false},
      atSOL_{false},
      atPhysicalEOF_{false},
      depth_{0},
      unreadIdx_{0},
      limit_{opts.limit()},
      fileLength_{getStreamLength()},
      varBinBuf_{
          std::make_shared<dwio::common::DataBuffer<char>>(contents_->pool)} {
  // Seek to first line at or after the specified region.
  if (contents_->compression == CompressionKind::CompressionKind_NONE) {
    /**
     * TODO: Inconsistent row skipping behavior (kept for Presto compatibility)
     *
     * Issue: When reading from byte offset > 0, we skip rows inclusively at the
     * start position, but when reading from byte 0, no rows are skipped. This
     * creates inconsistent behavior where a row at the boundary may be skipped
     * when it should be included.
     *
     * Example: If pos_ = 10 is the first byte of row 2, that entire row gets
     * skipped, even though it should be read.
     *
     * Proposed fix: streamPosition_ = (pos_ == 0) ? 0 : --pos_;
     * This would skip rows exclusively of pos_, ensuring consistent behavior.
     */
    const auto streamPosition_ = pos_;

    contents_->inputStream = contents_->input->read(
        streamPosition_,
        contents_->fileLength - streamPosition_,
        dwio::common::LogType::STREAM);

    if (pos_ != 0) {
      unreadData_.clear();
      (void)skipLine();
    }
    if (opts.skipRows() > 0) {
      (void)seekToRow(opts.skipRows());
    }
  } else {
    // compressed text files, the first split reads the whole file, rest read 0
    if (pos_ != 0) {
      atEOF_ = true;
    }
    limit_ = std::numeric_limits<uint64_t>::max();

    /**
     * The output buffer for decompression is allocated based on the
     * uncompressed length of the stream.
     *
     * For decompressors other than ZlibDecompressor, the uncompressed length is
     * obtained via getDecompressedLength, and blockSize serves only as a
     * fallbak when getDecompressedLength fails to return a valid length.
     *
     * ZlibDecompressor does not implement getDecompressedLength because the
     * DEFLATE algorithm used by zlib does not inherently includes the
     * uncompressed length in the compressed stream. As a result, blockSize is
     * used to set z_stream.avail_out during decompression to ensure enough
     * buffer allocated for the output. Since zlib requires avail_out to be a
     * uInt (unsigned int), blockSize is set to std::numeric_limits<unsigned
     * int>::max() for full compatibility.
     */
    const auto blockSize =
        (contents_->compression == CompressionKind::CompressionKind_ZLIB ||
         contents_->compression == CompressionKind::CompressionKind_GZIP)
        ? std::numeric_limits<unsigned int>::max()
        : std::numeric_limits<uint64_t>::max();

    contents_->inputStream = contents_->input->loadCompleteFile();
    auto name = contents_->inputStream->getName();
    contents_->decompressedInputStream = createDecompressor(
        contents_->compression,
        std::move(contents_->inputStream),
        blockSize,
        contents_->pool,
        contents_->compressionOptions,
        fmt::format("Text Reader: Stream {}", name),
        nullptr,
        true,
        contents_->fileLength);

    if (opts.skipRows() > 0) {
      (void)seekToRow(opts.skipRows());
    }
  }
}

uint64_t TextRowReader::next(
    uint64_t rows,
    VectorPtr& result,
    const Mutation* mutation) {
  if (atEOF_) {
    return 0;
  }

  auto& t = schemaWithId_;
  verify(
      t->type()->isRow(),
      "Top-level TypeKind of schema is not Row for file %s",
      getStreamNameData());

  auto projectSelectedType = options_.projectSelectedType();
  auto reqT =
      (projectSelectedType ? getSelectedType() : TypeWithId::create(getType()));
  verify(
      reqT->type()->isRow(),
      "Top-level TypeKind of schema is not Row for file %s",
      getStreamNameData());

  auto childCount = t->size();
  auto reqChildCount = reqT->size();

  // create top level RowVector
  auto rowVecPtr = BaseVector::create<RowVector>(
      reqT->type(), (vector_size_t)rows, &contents_->pool);

  vector_size_t rowsRead = 0;
  const auto initialPos = pos_;
  while (!atEOF_ && rowsRead < rows) {
    resetLine();
    uint64_t colIndex = 0;
    for (vector_size_t i = 0; i < childCount; i++) {
      if (colIndex >= reqT->size()) {
        break;
      }

      DelimType delim = DelimTypeNone;
      const auto& ct = t->childAt(i);
      const auto& rct = reqT->childAt(i);
      auto childVector = rowVecPtr->childAt(i).get();

      if (isSelectedField(ct)) {
        ++colIndex;
      } else if (colIndex < reqChildCount && !projectSelectedType) {
        // not selected and not projecting: set to null
        if (childVector != nullptr) {
          rowVecPtr->setNull(i, true);
          childVector = nullptr;
        }
        ++colIndex;
      } else {
        // not selected and projecting: just discard the field
        childVector = nullptr;
      }

      resizeVector(childVector, rowsRead);
      readElement(ct->type(), rct->type(), childVector, rowsRead, delim);
    }

    // set null property
    for (uint64_t i = colIndex; i < reqChildCount; i++) {
      auto childVector = rowVecPtr->childAt(i).get();

      if (childVector != nullptr) {
        rowVecPtr->setNull(static_cast<vector_size_t>(i), true);
      }
    }
    (void)skipLine();
    ++currentRow_;
    ++rowsRead;

    bool eof = false;
    if (contents_->compression == CompressionKind::CompressionKind_NONE) {
      eof = pos_ >= getLength();
    } else if (atPhysicalEOF_) {
      eof = pos_ >= contents_->decompressedInputStream->ByteCount();
    }

    if (eof) {
      setEOF();
    }

    // handle empty file
    if (initialPos == pos_ && atEOF_) {
      currentRow_ = 0;
      rowsRead = 0;
    }
  }

  // Resize the row vector to the actual number of rows read.
  // Handled here for both cases: pos_ > fileLength_ and pos_ > limit_
  rowVecPtr->resize(rowsRead);
  result = projectColumns(rowVecPtr, *scanSpec_, mutation);

  return rowsRead;
}

int64_t TextRowReader::nextRowNumber() {
  return atEOF_ ? -1 : static_cast<int64_t>(currentRow_) + 1;
}

int64_t TextRowReader::nextReadSize(uint64_t size) {
  return static_cast<int64_t>(std::min(fileLength_ - currentRow_, size));
}

void TextRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& /*stats*/) const {
  // No-op for non-selective reader.
}

void TextRowReader::resetFilterCaches() {
  // No-op for non-selective reader.
}

std::optional<size_t> TextRowReader::estimatedRowSize() const {
  return std::nullopt;
}

const ColumnSelector& TextRowReader::getColumnSelector() const {
  return columnSelector_;
}

std::shared_ptr<const TypeWithId> TextRowReader::getSelectedType() const {
  if (!selectedSchema_) {
    selectedSchema_ = columnSelector_.buildSelected();
  }
  return selectedSchema_;
}

uint64_t TextRowReader::getRowNumber() const {
  return currentRow_;
}

uint64_t TextRowReader::seekToRow(uint64_t rowNumber) {
  VELOX_CHECK_GT(
      rowNumber, currentRow_, "Text file cannot seek to earlier row");

  while (currentRow_ < rowNumber && !skipLine()) {
    currentRow_++;
    resetLine();
  }

  return currentRow_;
}

const RowReaderOptions& TextRowReader::getDefaultOpts() {
  static RowReaderOptions defaultOpts;
  return defaultOpts;
}

bool TextRowReader::isSelectedField(
    const std::shared_ptr<const TypeWithId>& type) {
  auto ci = type->id();
  return columnSelector_.shouldReadNode(ci);
}

const char* TextRowReader::getStreamNameData() const {
  return contents_->input->getName().data();
}

uint64_t TextRowReader::getLength() {
  if (fileLength_ == std::numeric_limits<uint64_t>::max()) {
    fileLength_ = getStreamLength();
  }
  return fileLength_;
}

uint64_t TextRowReader::getStreamLength() const {
  return contents_->input->getInputStream()->getLength();
}

void TextRowReader::setEOF() {
  atEOF_ = true;
  atEOL_ = true;
}

/// TODO: Update maximum depth after fixing issue with deeply nested complex
/// types
void TextRowReader::incrementDepth() {
  if (depth_ > 4) {
    dwio::common::parse_error("Schema nesting too deep");
  }
  depth_++;
}

void TextRowReader::decrementDepth(DelimType& delim) {
  if (depth_ == 0) {
    dwio::common::logic_error("Attempt to decrement nesting depth of 0");
  }
  depth_--;
  auto d = depth_ + DelimTypeEOR;
  if (delim > d) {
    setNone(delim);
  }
}

void TextRowReader::setEOE(DelimType& delim) {
  // Set delim if it is currently None or a more deeply
  // delimiter, to simply the code where aggregates
  // parse nested aggregates.
  auto d = depth_ + DelimTypeEOE;
  if (isNone(delim) || d < delim) {
    delim = d;
  }
}

void TextRowReader::resetEOE(DelimType& delim) {
  // Reset delim it is EOE or above.
  auto d = depth_ + DelimTypeEOE;
  if (delim >= d) {
    setNone(delim);
  }
}

bool TextRowReader::isEOE(DelimType delim) {
  // Test if delim is the EOE at the current depth.
  return (delim == (depth_ + DelimTypeEOE));
}

void TextRowReader::setEOR(DelimType& delim) {
  // Set delim if it is currently None or a more
  // deeply nested delimiter.
  auto d = depth_ + DelimTypeEOR;
  if (isNone(delim) || delim > d) {
    delim = d;
  }
}

bool TextRowReader::isEOR(DelimType delim) {
  // Return true if delim is the EOR for the current depth
  // or a less deeply nested depth.
  return (delim != DelimTypeNone && delim <= (depth_ + DelimTypeEOR));
}

bool TextRowReader::isOuterEOR(DelimType delim) {
  // Return true if delim is the EOR for the enclosing object.
  // For example, when parsing ARRAY elements, which leave delim
  // set to the EOR for their depth on return, isOuterEOR will
  // return true if we have reached the ARRAY EOR delimiter at
  // the end of the latest element.
  return (delim != DelimTypeNone && delim < (depth_ + DelimTypeEOR));
}

bool TextRowReader::isEOEorEOR(DelimType delim) {
  return (!isNone(delim) && delim <= (depth_ + DelimTypeEOE));
}

void TextRowReader::setNone(DelimType& delim) {
  delim = DelimTypeNone;
}

bool TextRowReader::isNone(DelimType delim) {
  return (delim == DelimTypeNone);
}

std::string&
TextRowReader::getString(TextRowReader& th, bool& isNull, DelimType& delim) {
  if (th.atEOL_) {
    delim = DelimTypeEOR; // top-level EOR
  }

  if (th.isEOEorEOR(delim)) {
    isNull = true;
    return emptyString;
  }

  bool wasEscaped = false;
  th.ownedString_.clear();

  /**
  Processing has to be done character by characater instad of chunk by chunk.
  This is to avoid edge case handling if escape character(s) are cut off at
  the end of the chunk.
  */
  while (true) {
    auto v = th.getByteOptimized(delim);
    if (!th.isNone(delim)) {
      break;
    }

    if (th.contents_->serDeOptions.isEscaped &&
        v == th.contents_->serDeOptions.escapeChar) {
      wasEscaped = true;
      th.ownedString_.append(1, static_cast<char>(v));
      v = th.getByteUncheckedOptimized(delim);
      if (!th.isNone(delim)) {
        break;
      }
    }
    th.ownedString_.append(1, static_cast<char>(v));
  }

  if (th.ownedString_ == th.contents_->serDeOptions.nullString) {
    isNull = true;
    return emptyString;
  }

  if (wasEscaped) {
    // We need to copy the data byte by byte only if there is at least one
    // escaped byte.
    uint64_t j = 0;
    for (uint64_t i = 0; i < th.ownedString_.length(); i++) {
      if (th.ownedString_[i] == th.contents_->serDeOptions.escapeChar &&
          i < th.ownedString_.length() - 1) {
        // Check if it's '\r' or '\n'.
        i++;
        if (th.ownedString_[i] == 'r') {
          th.ownedString_[j++] = '\r';
        } else if (th.ownedString_[i] == 'n') {
          th.ownedString_[j++] = '\n';
        } else {
          // Keep the next byte.
          th.ownedString_[j++] = th.ownedString_[i];
        }
      } else {
        th.ownedString_[j++] = th.ownedString_[i];
      }
    }
    th.ownedString_.resize(j);
  }

  return th.ownedString_;
}

uint8_t TextRowReader::getByte(DelimType& delim) {
  setNone(delim);
  auto v = getByteUnchecked(delim);
  if (isNone(delim)) {
    if (v == '\r') {
      v = getByteUnchecked<true>(delim); // always returns '\n' in this case
    }
    delim = getDelimType(v);
  }
  return v;
}

uint8_t TextRowReader::getByteOptimized(DelimType& delim) {
  setNone(delim);
  auto v = getByteUncheckedOptimized(delim);
  if (isNone(delim)) {
    if (v == '\r') {
      v = getByteUncheckedOptimized<true>(
          delim); // always returns '\n' in this case
    }
    delim = getDelimType(v);
  }
  return v;
}

DelimType TextRowReader::getDelimType(uint8_t v) {
  DelimType delim = DelimTypeNone;

  if (v == '\n') {
    atEOL_ = true;
    delim = DelimTypeEOR; // top level EOR

    /// TODO: Logically should be >=, kept as it is to align with presto reader.
    if (pos_ > limit_) {
      atEOF_ = true;
      delim = DelimTypeEOR;
    }
  } else if (v == contents_->serDeOptions.separators.at(depth_)) {
    setEOE(delim);
  } else {
    setNone(delim);
    uint64_t i = depth_;
    while (i > 0) {
      i--;
      if (v == contents_->serDeOptions.separators.at(i)) {
        delim = i + DelimTypeEOR; // level-based EOR
        break;
      }
    }
  }
  return delim;
}

template <bool skipLF>
char TextRowReader::getByteUnchecked(DelimType& delim) {
  if (atEOL_) {
    if (!skipLF) {
      delim = DelimTypeEOR; // top level EOR
    }
    return '\n';
  }

  try {
    char v;
    if (!unreadData_.empty()) {
      v = unreadData_[0];
      unreadData_.erase(0, 1);
    } else {
      contents_->inputStream->readFully(&v, 1);
    }
    pos_++;

    // only when previous char == '\r'
    if (skipLF) {
      if (v != '\n') {
        pos_--;
        return '\n';
      }
    } else {
      atSOL_ = false;
    }
    return v;
  } catch (EOFError&) {
  } catch (std::runtime_error& e) {
    if (std::string(e.what()).find("Short read of") != 0 && !skipLF) {
      throw;
    }
  }
  if (!skipLF) {
    setEOF();
    delim = DelimTypeEOR;
  }
  return '\n';
}

template <bool skipLF>
char TextRowReader::getByteUncheckedOptimized(DelimType& delim) {
  if (atEOL_) {
    if (!skipLF) {
      delim = DelimTypeEOR; // top level EOR
    }
    return '\n';
  }

  try {
    char v;
    if (contents_->compression != CompressionKind::CompressionKind_NONE &&
        preLoadedUnreadData_.empty()) {
      int length = 0;
      const void* buffer = nullptr;
      atPhysicalEOF_ =
          !contents_->decompressedInputStream->Next(&buffer, &length);
      if (!atPhysicalEOF_) {
        preLoadedUnreadData_ =
            std::string_view(reinterpret_cast<const char*>(buffer), length);
      }
    }

    if (unreadData_.empty() || unreadIdx_ >= unreadData_.size()) {
      bool updated = false;
      if (contents_->compression != CompressionKind::CompressionKind_NONE) {
        unreadData_.assign(
            preLoadedUnreadData_.data(), preLoadedUnreadData_.size());
        preLoadedUnreadData_ = {};
        updated = !unreadData_.empty();
      } else {
        int length = 0;
        const void* buffer = nullptr;
        if (contents_->inputStream->Next(&buffer, &length) && length > 0) {
          VELOX_CHECK_NOT_NULL(buffer);
          unreadData_.assign(reinterpret_cast<const char*>(buffer), length);
          updated = true;
        }
      }

      if (!updated) {
        setEOF();
        delim = DelimTypeEOR;
        return '\0';
      }
      unreadIdx_ = 0;
    }

    v = unreadData_[unreadIdx_++];
    pos_++;

    // only when previous char == '\r'
    if (skipLF) {
      if (v != '\n') {
        pos_--;
        return '\n';
      }
    } else {
      atSOL_ = false;
    }
    return v;
  } catch (EOFError&) {
  } catch (std::runtime_error& e) {
    if (std::string(e.what()).find("Short read of") != 0 && !skipLF) {
      throw;
    }
  }
  if (!skipLF) {
    setEOF();
    delim = DelimTypeEOR;
  }
  return '\n';
}

bool TextRowReader::getEOR(DelimType& delim, bool& isNull) {
  if (isEOR(delim)) {
    isNull = true;
    return true;
  }
  if (atEOL_) {
    delim = DelimTypeEOR; // top-level EOR
    isNull = true;
    return true;
  }
  bool wasAtSOL = atSOL_;
  setNone(delim);
  ownedString_.clear();
  const auto& ns = contents_->serDeOptions.nullString;
  uint8_t v = 0;
  while (true) {
    v = getByteUncheckedOptimized(delim);
    if (isNone(delim)) {
      if (v == '\r') {
        // always returns '\n' in this case
        v = getByteUncheckedOptimized<true>(delim);
      }
      delim = getDelimType(v);
    }

    if (isEOR(delim) || atEOL_) {
      if (ownedString_ == ns) {
        isNull = true;
      } else if (!ownedString_.empty()) {
        break;
      }
      setEOR(delim);
      return true;
    }
    if (ownedString_.size() >= ns.size() ||
        static_cast<char>(v) != ns[ownedString_.size()]) {
      break;
    }
    ownedString_.push_back(static_cast<char>(v));
  }

  unreadData_.insert(0, 1, static_cast<char>(v));
  pos_--;
  if (!ownedString_.empty()) {
    unreadData_.insert(0, ownedString_);
    pos_ -= ownedString_.size();
  }
  atEOL_ = false;
  atSOL_ = wasAtSOL;
  setNone(delim);
  return false;
}

bool TextRowReader::skipLine() {
  DelimType delim = DelimTypeNone;
  while (!atEOL_) {
    (void)getByteOptimized(delim);
  }
  /// TODO: Logically should be >=, kept as it is to align with presto reader
  if (pos_ > limit_) {
    setEOF();
    delim = DelimTypeEOR;
  }
  return atEOF_;
}

void TextRowReader::resetLine() {
  if (!atEOF_) {
    atEOL_ = false;
    VELOX_CHECK_EQ(depth_, 0);
  }
  atSOL_ = true;
}

template <typename T>
T TextRowReader::getInteger(TextRowReader& th, bool& isNull, DelimType& delim) {
  const std::string& str = getString(th, isNull, delim);

  if (str.empty()) {
    isNull = true;
  }
  if (isNull) {
    return 0;
  }

  // Test if s is not acceptable integer format for
  // the warehouse, for cases accepted by stol().
  char c = str[0];
  if (c != '-' && !std::isdigit(static_cast<unsigned char>(c))) {
    isNull = true;
    return 0;
  }

  int64_t v = 0;
  unsigned long long scanPos = 0;
  errno = 0;
  auto scanCount = sscanf(str.c_str(), "%" SCNd64 "%lln", &v, &scanPos);
  if (scanCount != 1 || errno == ERANGE) {
    isNull = true;
    return 0;
  }
  if (scanPos < str.size()) {
    // Check if the string is a valid decimal.
    for (uint64_t i = scanPos; i < str.size(); i++) {
      if (i == scanPos && str[i] == '.') {
        continue;
      }
      if (str[i] >= '0' && str[i] <= '9') {
        continue;
      }
      isNull = true;
      return 0;
    }
  }

  if (!std::is_same<T, int64_t>::value) {
    if (static_cast<int64_t>(static_cast<T>(v)) != v) {
      isNull = true;
      return 0;
    }
  }
  return static_cast<T>(v);
}

namespace {

static const StringView trueStringView = StringView{"TRUE"};
static const StringView falseStringView = StringView{"FALSE"};

} // namespace

bool TextRowReader::getBoolean(
    TextRowReader& th,
    bool& isNull,
    DelimType& delim) {
  const std::string& str = getString(th, isNull, delim);
  if (str.empty()) {
    isNull = true;
  }
  if (isNull) {
    return false;
  }
  if (str.compare(trueStringView) == 0) {
    return true;
  }
  if (str.compare(falseStringView) == 0) {
    return false;
  }

  switch (str.size()) {
    case 4:
      if (StringPiece(str).equals("TRUE", AsciiCaseInsensitive())) {
        return true;
      }
      break;
    case 5:
      if (StringPiece(str).equals("FALSE", AsciiCaseInsensitive())) {
        return false;
      }
      break;
    default:
      break;
  }

  isNull = true;
  return false;
}

namespace {

static const StringView NaNStringView = StringView{"NaN"};
static const StringView InfinityStringView = StringView{"Infinity"};
static const StringView ShortInfinityStringView = StringView{"Inf"};
static const StringView NegInfinityStringView = StringView{"-Infinity"};
static const StringView ShortNegInfinityStringView = StringView{"-Inf"};

bool unacceptableFloatingPoint(std::string& s) {
  for (int i = 0; i < s.size(); ++i) {
    char c = s.data()[i];
    if (!(std::isalpha(c) || c == '-')) {
      return false;
    }
  }

  bool isNaN =
      StringPiece(s).equals(StringPiece(NaNStringView), AsciiCaseInsensitive());

  bool isInf = StringPiece(s).equals(
      StringPiece(InfinityStringView), AsciiCaseInsensitive());
  bool isShortInf = StringPiece(s).equals(
      StringPiece(ShortInfinityStringView), AsciiCaseInsensitive());

  bool isNegInf = StringPiece(s).equals(
      StringPiece(NegInfinityStringView), AsciiCaseInsensitive());
  bool isShortNegInf = StringPiece(s).equals(
      StringPiece(ShortNegInfinityStringView), AsciiCaseInsensitive());

  return (!isNaN && !isInf && !isShortInf && !isNegInf && !isShortNegInf);
}

void trimStringInPlace(std::string& s) {
  const auto isNotSpace = [](unsigned char ch) { return ch > 0x20; };
  size_t start = 0;
  size_t end = s.size();

  // Find first non-whitespace character
  while (start < end && !isNotSpace(s[start])) {
    ++start;
  }

  // If the string is all whitespace
  if (start == end) {
    s.clear();
    return;
  }

  // Find last non-whitespace character
  size_t last = end - 1;
  while (last > start && !isNotSpace(s[last])) {
    --last;
  }

  // Erase leading and trailing whitespace
  s = s.substr(start, last - start + 1);
}

} // namespace

float TextRowReader::getFloat(
    TextRowReader& th,
    bool& isNull,
    DelimType& delim) {
  std::string& str = getString(th, isNull, delim);
  if (str.empty()) {
    isNull = true;
  }
  if (isNull) {
    return 0;
  }

  trimStringInPlace(str);

  if (str.data()[0] == '.') {
    th.ownedString_.insert(th.ownedString_.begin(), '0');
    str = th.ownedString_;
  }

  if (unacceptableFloatingPoint(str)) {
    isNull = true;
    return 0.0;
  }

  float v = 0.0;
  unsigned long long scanPos = 0;
  // We ignore ERANGE, since denormalized floats and
  // infinities are acceptable.
  auto scanCount = sscanf(str.c_str(), "%f%lln", &v, &scanPos);
  if (scanCount != 1 || scanPos < str.size()) {
    isNull = true;
    return 0.0;
  }
  return v;
}

double
TextRowReader::getDouble(TextRowReader& th, bool& isNull, DelimType& delim) {
  std::string& str = getString(th, isNull, delim);
  if (str.empty()) {
    isNull = true;
  }

  if (isNull) {
    return 0.0;
  }

  trimStringInPlace(str);

  if (str.data()[0] == '.') {
    th.ownedString_.insert(th.ownedString_.begin(), '0');
    str = th.ownedString_;
  }

  // Filter out values from non-warehouse sources which
  // other readers translate to null. Warehouse
  // readers require upper-case values.
  if (unacceptableFloatingPoint(str)) {
    isNull = true;
    return 0.0;
  }

  double v = 0.0;
  unsigned long long scanPos = 0;
  // We ignore ERANGE, since denormalized doubles and
  // infinities are acceptable.
  auto scanCount = sscanf(str.c_str(), "%lf%lln", &v, &scanPos);
  if (scanCount != 1 || scanPos < str.size()) {
    isNull = true;
    return 0.0;
  }
  return v;
}

/// TODO: Reconsider error handling strategy for malformed data
/// Currently, all read functions convert invalid/malformed data to NULL values.
/// This approach may produce incorrect query results, particularly for
/// aggregate operations where a high volume of NULLs can significantly skew
/// calculations (e.g., COUNT, AVG, SUM). Consider alternative strategies such
/// as throwing exceptions, logging warnings, or providing configurable error
/// handling modes.
void TextRowReader::readElement(
    const std::shared_ptr<const Type>& t,
    const std::shared_ptr<const Type>& reqT,
    BaseVector* FOLLY_NULLABLE data,
    vector_size_t insertionRow,
    DelimType& delim) {
  bool isNull = false;
  switch (t->kind()) {
    case TypeKind::INTEGER:
      switch (reqT->kind()) {
        case TypeKind::BIGINT:
          putValue<int32_t, int64_t>(
              getInteger<int32_t>, data, insertionRow, delim);
          break;
        case TypeKind::INTEGER:
          putValue<int32_t, int32_t>(
              getInteger<int32_t>, data, insertionRow, delim);
          break;
        default:
          VELOX_FAIL(
              "Requested type {} is not supported to be read as type {}",
              reqT->toString(),
              t->toString());
          break;
      }
      break;

    case TypeKind::BIGINT:
      putValue<int64_t, int64_t>(
          getInteger<int64_t>, data, insertionRow, delim);
      break;

    case TypeKind::SMALLINT:
      switch (reqT->kind()) {
        case TypeKind::BIGINT:
          putValue<int16_t, int64_t>(
              getInteger<int16_t>, data, insertionRow, delim);
          break;
        case TypeKind::INTEGER:
          putValue<int16_t, int32_t>(
              getInteger<int16_t>, data, insertionRow, delim);
          break;
        case TypeKind::SMALLINT:
          putValue<int16_t, int16_t>(
              getInteger<int16_t>, data, insertionRow, delim);
          break;
        default:
          VELOX_FAIL(
              "Requested type {} is not supported to be read as type {}",
              reqT->toString(),
              t->toString());
          break;
      }
      break;

    case TypeKind::VARBINARY: {
      const std::string& str = getString(*this, isNull, delim);

      // Early return if no data vector or at EOF
      if ((atEOF_ && atSOL_) || (data == nullptr)) {
        return;
      }

      const auto& flatVector = data->asChecked<FlatVector<StringView>>();
      if (!flatVector) {
        VELOX_FAIL(
            "Vector for column type does not match: expected FlatVector<StringView>, got {}",
            data ? data->type()->toString() : "null");
        return;
      }

      // Allocate a blob buffer
      size_t len = str.size();
      const auto blen = encoding::Base64::calculateDecodedSize(str.data(), len);
      varBinBuf_->resize(blen.value_or(0));

      // decode from base64 to the blob buffer.
      Status status = encoding::Base64::decode(
          str.data(), str.size(), varBinBuf_->data(), blen.value_or(0));

      if (status.code() == StatusCode::kOK) {
        flatVector->set(
            insertionRow,
            StringView(varBinBuf_->data(), static_cast<int32_t>(blen.value())));
      } else {
        // Not valid base64:  just copy as-is for compatibility.
        //
        // Note that some warehouse file have simply binary data
        // in what should be a base64-encoded field, and which
        // may result in extra rows.  Other readers behave as
        // below, so this provides compatibility, even if  all
        // readers should really reject these files.
        varBinBuf_->resize(str.size());

        VELOX_CHECK_NOT_NULL(str.data());

        len = str.size();
        memcpy(varBinBuf_->data(), str.data(), str.size());

        // Use StringView, set(vector_size_t idx, T value) fails because
        // strlen(varBinBuf_->data()) is undefined due to lack of null
        // terminator
        flatVector->set(
            insertionRow,
            StringView(varBinBuf_->data(), static_cast<int32_t>(str.size())));
      }

      if (isNull) {
        flatVector->setNull(insertionRow, true);
      }

      break;
    }
    case TypeKind::VARCHAR: {
      const std::string& str = getString(*this, isNull, delim);

      // Early return if no data vector or at EOF
      if ((atEOF_ && atSOL_) || (data == nullptr)) {
        return;
      }

      const auto& flatVector = data->asChecked<FlatVector<StringView>>();
      if (!flatVector) {
        VELOX_FAIL(
            "Vector for column type does not match: expected FlatVector<StringView>, got {}",
            data ? data->type()->toString() : "null");
        return;
      }

      flatVector->set(
          insertionRow,
          StringView(str.data(), static_cast<int32_t>(str.size())));

      if (isNull) {
        flatVector->setNull(insertionRow, true);
      }

      break;
    }

    case TypeKind::BOOLEAN:
      switch (reqT->kind()) {
        case TypeKind::BIGINT:
          putValue<bool, int64_t>(getBoolean, data, insertionRow, delim);
          break;
        case TypeKind::INTEGER:
          putValue<bool, int32_t>(getBoolean, data, insertionRow, delim);
          break;
        case TypeKind::SMALLINT:
          putValue<bool, int16_t>(getBoolean, data, insertionRow, delim);
          break;
        case TypeKind::TINYINT:
          putValue<bool, int8_t>(getBoolean, data, insertionRow, delim);
          break;
        case TypeKind::BOOLEAN:
          putValue<bool, bool>(getBoolean, data, insertionRow, delim);
          break;
        default:
          VELOX_FAIL(
              "Requested type {} is not supported to be read as type {}",
              reqT->toString(),
              t->toString());
          break;
      }
      break;

    case TypeKind::TINYINT:
      switch (reqT->kind()) {
        case TypeKind::BIGINT:
          putValue<int8_t, int64_t>(
              getInteger<int8_t>, data, insertionRow, delim);
          break;
        case TypeKind::INTEGER:
          putValue<int8_t, int32_t>(
              getInteger<int8_t>, data, insertionRow, delim);
          break;
        case TypeKind::SMALLINT:
          putValue<int8_t, int16_t>(
              getInteger<int8_t>, data, insertionRow, delim);
          break;
        case TypeKind::TINYINT:
          putValue<int8_t, int8_t>(
              getInteger<int8_t>, data, insertionRow, delim);
          break;
        default:
          VELOX_FAIL(
              "Requested type {} is not supported to be read as type {}",
              reqT->toString(),
              t->toString());
          break;
      }
      break;

    case TypeKind::ARRAY: {
      const auto& ct = t->childAt(0);
      const auto& arrayVector = data ? data->asChecked<ArrayVector>() : nullptr;

      incrementDepth();
      (void)getEOR(delim, isNull);

      if (arrayVector != nullptr) {
        auto rawSizes = arrayVector->sizes()->asMutable<vector_size_t>();
        auto rawOffsets = arrayVector->offsets()->asMutable<vector_size_t>();

        rawOffsets[insertionRow] = insertionRow > 0
            ? rawOffsets[insertionRow - 1] + rawSizes[insertionRow - 1]
            : 0;
        const int startElementIdx = rawOffsets[insertionRow];

        vector_size_t elementCount = 0;
        if (isNull) {
          arrayVector->setNull(insertionRow, isNull);
          rawSizes[insertionRow] = 0;
        } else {
          // Read elements until we reach the end of the array.
          while (!isOuterEOR(delim)) {
            setNone(delim);
            auto elementsVector = arrayVector->elements().get();
            resizeVector(elementsVector, startElementIdx + elementCount);

            readElement(
                ct,
                reqT->childAt(0),
                elementsVector,
                startElementIdx + elementCount,
                delim);

            // Update size on every iteration to allow the right size
            // inheritance in resizeVector.
            rawSizes[insertionRow] = ++elementCount;

            if (atEOF_ && atSOL_) {
              decrementDepth(delim);
              return;
            }
          }
        }

      } else {
        // Skip over array data to maintain correct stream position.
        while (!isOuterEOR(delim)) {
          setNone(delim);
          readElement(ct, reqT->childAt(0), nullptr, 0, delim);
        }
      }
      decrementDepth(delim);
      break;
    }

    case TypeKind::ROW: {
      const auto& childCount = t->size();
      const auto& rowVector = data ? data->asChecked<RowVector>() : nullptr;
      incrementDepth();

      if (rowVector != nullptr) {
        if (isNull) {
          rowVector->setNull(insertionRow, isNull);
        } else {
          for (uint64_t j = 0; j < childCount; j++) {
            if (!isOuterEOR(delim)) {
              setNone(delim);
            }

            // Get the child vector for this field.
            BaseVector* childVector = nullptr;
            if (j < reqT->size()) {
              childVector = rowVector->childAt(j).get();
            }
            resizeVector(childVector, insertionRow);
            readElement(
                t->childAt(j),
                j < reqT->size() ? reqT->childAt(j) : t->childAt(j),
                childVector,
                insertionRow,
                delim);

            if (atEOF_ && atSOL_) {
              decrementDepth(delim);
              return;
            }
          }
        }
      } else {
        // Skip over row data to maintain correct stream position.
        for (uint64_t j = 0; j < childCount; j++) {
          if (!isOuterEOR(delim)) {
            setNone(delim);
          }
          readElement(t->childAt(j), reqT->childAt(j), nullptr, 0, delim);
        }
      }

      decrementDepth(delim);
      setEOE(delim);
      break;
    }

    case TypeKind::MAP: {
      const auto& mapt = t->asMap();
      const auto& key = mapt.keyType();
      const auto& value = mapt.valueType();
      const auto& mapVector = data ? data->asChecked<MapVector>() : nullptr;
      incrementDepth();
      (void)getEOR(delim, isNull);

      if (mapVector != nullptr) {
        auto rawOffsets = mapVector->offsets()->asMutable<vector_size_t>();
        auto rawSizes = mapVector->sizes()->asMutable<vector_size_t>();

        rawOffsets[insertionRow] = insertionRow > 0
            ? rawOffsets[insertionRow - 1] + rawSizes[insertionRow - 1]
            : 0;
        const int startElementIdx = rawOffsets[insertionRow];

        vector_size_t elementCount = 0;
        if (isNull) {
          mapVector->setNull(insertionRow, isNull);
          rawSizes[insertionRow] = 0;
        } else {
          while (!isOuterEOR(delim)) {
            // Decode another element.
            setNone(delim);
            incrementDepth();

            // insert key
            auto keysVector = mapVector->mapKeys().get();
            resizeVector(keysVector, startElementIdx + elementCount);

            readElement(
                key,
                reqT->childAt(0),
                keysVector,
                startElementIdx + elementCount,
                delim);

            // Case for no value key.
            if (atEOF_ && atSOL_) {
              rawSizes[insertionRow] = elementCount;
              rawOffsets[insertionRow + 1] = startElementIdx + elementCount;
              decrementDepth(delim);
              decrementDepth(delim);
              return;
            }
            resetEOE(delim);

            // insert value
            auto valsVector = mapVector->mapValues().get();
            resizeVector(valsVector, startElementIdx + elementCount);

            readElement(
                value,
                reqT->childAt(1),
                valsVector,
                startElementIdx + elementCount,
                delim);

            rawSizes[insertionRow] = ++elementCount;

            decrementDepth(delim);
          }
        }

      } else {
        // Skip over map data to maintain correct stream position.
        while (!isOuterEOR(delim)) {
          setNone(delim);
          incrementDepth();
          readElement(key, reqT->childAt(0), nullptr, 0, delim);
          resetEOE(delim);
          readElement(value, reqT->childAt(1), nullptr, 0, delim);
          decrementDepth(delim);
        }
      }
      decrementDepth(delim);
      break;
    }

    case TypeKind::REAL:
      switch (reqT->kind()) {
        case TypeKind::REAL:
          putValue<float, float>(getFloat, data, insertionRow, delim);
          break;
        case TypeKind::DOUBLE:
          putValue<float, double>(getDouble, data, insertionRow, delim);
          break;
        default:
          VELOX_FAIL(
              "Requested type {} is not supported to be read as type {}",
              reqT->toString(),
              t->toString());
          break;
      }
      break;

    case TypeKind::DOUBLE:
      putValue<double, double>(getDouble, data, insertionRow, delim);
      break;

    case TypeKind::TIMESTAMP: {
      const std::string& s = getString(*this, isNull, delim);

      // Early return if no data vector or at EOF
      if ((atEOF_ && atSOL_) || (data == nullptr)) {
        return;
      }

      auto flatVector = data->asChecked<FlatVector<Timestamp>>();
      if (!flatVector) {
        VELOX_FAIL(
            "Vector for column type does not match: expected FlatVector<Timestamp>, got {}",
            data ? data->type()->toString() : "null");
        return;
      }

      if (s.empty()) {
        isNull = true;
        flatVector->setNull(insertionRow, true);
      } else {
        auto ts = util::Converter<TypeKind::TIMESTAMP>::tryCast(s).thenOrThrow(
            folly::identity,
            [&](const Status& status) { VELOX_USER_FAIL(status.message()); });
        ts.toGMT(Timestamp::defaultTimezone());
        flatVector->set(
            insertionRow, Timestamp{ts.getSeconds(), ts.getNanos()});
      }

      break;
    }

    default:
      VELOX_NYI("readElement unhandled type (kind code {})", t->kind());
  }

  ownedString_.clear();
}

uint64_t maxStreamsForType(const std::shared_ptr<const Type>& type) {
  switch (type->kind()) {
    case TypeKind::ROW:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::SMALLINT:
    case TypeKind::VARCHAR:
      return 1;
    default:
      return 0;
  }
}

template <class T, class reqT>
void TextRowReader::putValue(
    const std::function<T(TextRowReader& th, bool& isNull, DelimType& delim)>&
        f,
    BaseVector* FOLLY_NULLABLE data,
    vector_size_t insertionRow,
    DelimType& delim) {
  bool isNull = false;
  T v;
  if (isEOR(delim)) {
    isNull = true;
    v = 0;
  } else {
    v = f(*this, isNull, delim);
  }

  // Early return if no data vector or at EOF
  if ((atEOF_ && atSOL_) || (data == nullptr)) {
    return;
  }

  // Cast to FlatVector<reqT>
  auto flatVector = data ? data->asChecked<FlatVector<reqT>>() : nullptr;
  if (!flatVector) {
    VELOX_FAIL("Vector for column type does not match");
    return;
  }

  // Handle null property.
  if (isNull) {
    flatVector->setNull(insertionRow, isNull);
    return;
  }

  flatVector->set(insertionRow, v);
}

const std::shared_ptr<const RowType>& TextRowReader::getType() const {
  return contents_->schema;
}

TextReader::TextReader(
    const ReaderOptions& options,
    std::unique_ptr<BufferedInput> input)
    : options_{options} {
  auto schema = options_.fileSchema();
  VELOX_USER_CHECK_NOT_NULL(schema, "File schema for TEXT must be set.");

  if (!schema) {
    // Create dummy for testing.
    internalSchema_ = std::dynamic_pointer_cast<const RowType>(
        type::fbhive::HiveTypeParser().parse("struct<col0:string>"));
    DWIO_ENSURE_NOT_NULL(internalSchema_.get());
    schema = internalSchema_;
  }
  schemaWithId_ = TypeWithId::create(schema);
  contents_ = std::make_shared<FileContents>(options_.memoryPool(), schema);

  if (!contents_->schema->isRow()) {
    throw std::invalid_argument("file schema must be a ROW type");
  }

  contents_->input = std::move(input);

  // Find the size of the file using the option or filesystem.
  contents_->fileLength = std::min(
      options_.tailLocation(),
      static_cast<uint64_t>(contents_->input->getInputStream()->getLength()));

  /**
   * We are now allowing delimiters/separators and escape characters to be the
   * same. This could be error prone because we are checking for delimiters
   * before escape characters.
   *
   * Example:
   * delim = ','; escapeChar = ','
   * dataToParse = "1,,2"
   * Schema = ROW(ARRAY(VARCHAR()))
   *
   * Scenario 1: Check delimiter before escape (current implementation)
   * Output: ["1", NULL, "2"]
   *
   * Scenario 2: Check escape before delim
   * Output: ["1,2"]
   *
   * TODO: This is not a bug but would be good to be able to handle this
   * ambiguity
   */

  // Set the SerDe options.
  contents_->serDeOptions = options_.serDeOptions();
  if (contents_->serDeOptions.isEscaped) {
    for (auto delim : contents_->serDeOptions.separators) {
      contents_->needsEscape.at(delim) = true;
    }
    contents_->needsEscape.at(contents_->serDeOptions.escapeChar) = true;
  }

  // Validate SerDe options.
  VELOX_CHECK(
      contents_->serDeOptions.nullString.compare("\r") != 0,
      "\'\\r\' is not allowed to be nullString");
  VELOX_CHECK(
      contents_->serDeOptions.nullString.compare("\n") != 0,
      "\'\\n\n is not allowed to be nullString");

  setCompressionSettings(
      contents_->input->getName(),
      contents_->compression,
      contents_->compressionOptions);
}

std::optional<uint64_t> TextReader::numberOfRows() const {
  return std::nullopt;
}

std::unique_ptr<ColumnStatistics> TextReader::columnStatistics(
    uint32_t /*index*/) const {
  return nullptr;
}

const std::shared_ptr<const RowType>& TextReader::rowType() const {
  return contents_->schema;
}

CompressionKind TextReader::getCompression() const {
  return contents_->compression;
}

const std::shared_ptr<const TypeWithId>& TextReader::typeWithId() const {
  if (!typeWithId_) {
    typeWithId_ = TypeWithId::create(rowType());
  }
  return typeWithId_;
}

std::unique_ptr<RowReader> TextReader::createRowReader(
    const RowReaderOptions& opts) const {
  return std::make_unique<TextRowReader>(contents_, opts);
}

uint64_t TextReader::getFileLength() const {
  return contents_->fileLength;
}

uint64_t TextReader::getMemoryUse() {
  uint64_t memory = std::min(
      uint64_t(contents_->fileLength),
      contents_->input->getInputStream()->getNaturalReadSize());

  // Decompressor needs a buffer.
  if (contents_->compression != CompressionKind::CompressionKind_NONE) {
    memory *= 3;
  }

  return memory;
}

} // namespace facebook::velox::text
