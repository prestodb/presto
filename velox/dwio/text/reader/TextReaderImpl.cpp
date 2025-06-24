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

#include "velox/dwio/text/reader/TextReaderImpl.h"
#include "velox/type/fbhive/HiveTypeParser.h"

#include <string>

namespace facebook::velox::dwio::common {

using common::CompressionKind;

const std::string TEXTFILE_CODEC = "org.apache.hadoop.io.compress.GzipCodec";
const std::string TEXTFILE_COMPRESSION_EXTENSION = ".gz";
const std::string TEXTFILE_COMPRESSION_EXTENSION_RAW = ".deflate";

namespace {

void unsupportedBatchType(BaseVector* FOLLY_NULLABLE data) {
  logic_error(
      "unsupported vector type: %s",
      (data == nullptr) ? "null" : typeid(*data).name());
}

void resizeVector(BaseVector* data, vector_size_t insertionIdx) {
  auto dataSize = data->size();
  if (dataSize == 0) {
    data->resize(10);
  } else if (dataSize <= insertionIdx) {
    data->resize(dataSize * 2);
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
      compression{CompressionKind::CompressionKind_NONE} {
  needsEscape.fill(false);
  needsEscape.at(0) = true;
}

TextRowReaderImpl::TextRowReaderImpl(
    std::shared_ptr<FileContents> fileContents,
    const RowReaderOptions& opts)
    : RowReader(),
      contents_{fileContents},
      schemaWithId_{TypeWithId::create(fileContents->schema)},
      selectedSchema_{nullptr},
      options_{opts},
      columnSelector_{
          ColumnSelector::apply(opts.selector(), contents_->schema)},
      currentRow_{0},
      pos_{0},
      atEOL_{false},
      atEOF_{false},
      atSOL_{false},
      depth_{0},
      stringViewBuffer_{StringViewBufferHolder(&contents_->pool)} {
  limit_ = options_.limit();
  fileLength_ = getStreamLength();
  // seek to first line at or after the specified region.
  auto offset = opts.offset();
  if (contents_->compression == CompressionKind::CompressionKind_NONE) {
    if (offset != 0) {
      pos_ = offset;
      (void)skipLine();
    }
  } else {
    // compressed text files, the first split reads the whole file, rest read 0
    if (offset != 0) {
      atEOF_ = true;
    }
    limit_ = std::numeric_limits<uint64_t>::max();
  }
}

uint64_t TextRowReaderImpl::next(
    uint64_t rows,
    VectorPtr& result,
    const Mutation* /*mutation*/) {
  if (atEOF_) {
    return 0;
  }

  // initialize stream
  if (!contents_->inputStream) {
    contents_->inputStream =
        contents_->input->read(pos_, contents_->fileLength, LogType::STREAM);
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
  auto rowVecPtr = std::static_pointer_cast<RowVector>(
      BaseVector::create(reqT->type(), rows, &contents_->pool));

  vector_size_t rowsRead = 0;
  const auto initialPos = pos_;
  while (!atEOF_ && rowsRead < rows) {
    resetLine();
    uint64_t colIndex = 0;
    for (uint32_t i = 0; i < childCount; i++) {
      if (colIndex >= reqT->size()) {
        break;
      }

      DelimType delim = DelimTypeNone;
      const auto& ct = t->childAt(i);
      const auto& rct = reqT->childAt(i);

      /// TODO: PROJECTION

      auto childVector = rowVecPtr->childAt(i).get();
      resizeVector(childVector, rowsRead);
      readElement(ct->type(), rct->type(), childVector, rowsRead, delim);
      ++colIndex;
    }

    // set null property
    for (uint32_t i = colIndex; i < reqChildCount; i++) {
      auto childVector = rowVecPtr->childAt(i).get();

      if (childVector != nullptr) {
        rowVecPtr->setNullCount(rowVecPtr->getNullCount().value_or(0) + 1);
      }
    }
    (void)skipLine();
    ++currentRow_;

    rowsRead++;
    if (pos_ >= getLength()) {
      atEOF_ = true;
      rowVecPtr->resize(rowsRead);
    }

    // handle empty file
    if (initialPos == pos_ && atEOF_) {
      currentRow_ = 0;
    }
  }

  result = rowVecPtr;

  return rowsRead;
}

int64_t TextRowReaderImpl::nextRowNumber() {
  return atEOF_ ? -1 : static_cast<int64_t>(currentRow_) + 1;
}

int64_t TextRowReaderImpl::nextReadSize(uint64_t size) {
  return std::min(fileLength_ - currentRow_, size);
}

void TextRowReaderImpl::updateRuntimeStats(RuntimeStatistics& /*stats*/) const {
  // No-op for non-selective reader.
}

void TextRowReaderImpl::resetFilterCaches() {
  // No-op for non-selective reader.
}

std::optional<size_t> TextRowReaderImpl::estimatedRowSize() const {
  return std::nullopt;
}

const ColumnSelector& TextRowReaderImpl::getColumnSelector() const {
  return columnSelector_;
}

std::shared_ptr<const TypeWithId> TextRowReaderImpl::getSelectedType() const {
  if (!selectedSchema_) {
    selectedSchema_ = columnSelector_.buildSelected();
  }
  return selectedSchema_;
}

uint64_t TextRowReaderImpl::getRowNumber() const {
  return currentRow_;
}

uint64_t TextRowReaderImpl::seekToRow(uint64_t rowNumber) {
  VELOX_CHECK_LT(
      rowNumber, currentRow_, "Text file cannot seek to earlier row");

  while (currentRow_ < rowNumber && !skipLine()) {
    currentRow_++;
    resetLine();
  }

  return currentRow_;
}

bool TextRowReaderImpl::isSelectedField(
    const std::shared_ptr<const TypeWithId>& type) {
  auto ci = type->id();
  return columnSelector_.shouldReadNode(ci);
}

const char* TextRowReaderImpl::getStreamNameData() const {
  return contents_->input->getName().data();
}

uint64_t TextRowReaderImpl::getLength() {
  if (fileLength_ == std::numeric_limits<uint64_t>::max()) {
    fileLength_ = getStreamLength();
  }
  return fileLength_;
}

/// TODO: COMPLETE IMPLEMENTATION WITH DECOMPRESSED STREAM
uint64_t TextRowReaderImpl::getStreamLength() {
  return contents_->input->getInputStream()->getLength();
}

void TextRowReaderImpl::setEOF() {
  atEOF_ = true;
  atEOL_ = true;
}

void TextRowReaderImpl::incrementDepth() {
  if (depth_ >= 6) {
    parse_error("Schema nesting too deep");
  }
  depth_++;
}

void TextRowReaderImpl::decrementDepth(DelimType& delim) {
  if (depth_ == 0) {
    logic_error("Attempt to decrement nesting depth of 0");
  }
  depth_--;
  auto d = depth_ + DelimTypeEOR;
  if (delim > d) {
    setNone(delim);
  }
}

void TextRowReaderImpl::setEOE(DelimType& delim) {
  // Set delim if it is currently None or a more deeply
  // delimiter, to simply the code where aggregates
  // parse nested aggregates.
  auto d = depth_ + DelimTypeEOE;
  if (isNone(delim) || d < delim) {
    delim = d;
  }
}

void TextRowReaderImpl::resetEOE(DelimType& delim) {
  // Reset delim it is EOE or above.
  auto d = depth_ + DelimTypeEOE;
  if (delim >= d) {
    setNone(delim);
  }
}

bool TextRowReaderImpl::isEOE(DelimType delim) {
  // Test if delim is the EOE at the current depth.
  return (delim == (depth_ + DelimTypeEOE));
}

void TextRowReaderImpl::setEOR(DelimType& delim) {
  // Set delim if it is currently None or a more
  // deeply nested delimiter.
  auto d = depth_ + DelimTypeEOR;
  if (isNone(delim) || delim > d) {
    delim = d;
  }
}

bool TextRowReaderImpl::isEOR(DelimType delim) {
  // Return true if delim is the EOR for the current depth
  // or a less deeply nested depth.
  return (delim != DelimTypeNone && delim <= (depth_ + DelimTypeEOR));
}

bool TextRowReaderImpl::isOuterEOR(DelimType delim) {
  // Return true if delim is the EOR for the enclosing object.
  // For example, when parsing ARRAY elements, which leave delim
  // set to the EOR for their depth on return, isOuterEOR will
  // return true if we have reached the ARRAY EOR delimiter at
  // the end of the latest element.
  return (delim != DelimTypeNone && delim < (depth_ + DelimTypeEOR));
}

bool TextRowReaderImpl::isEOEorEOR(DelimType delim) {
  return (!isNone(delim) && delim <= (depth_ + DelimTypeEOE));
}

void TextRowReaderImpl::setNone(DelimType& delim) {
  delim = DelimTypeNone;
}

bool TextRowReaderImpl::isNone(DelimType delim) {
  return (delim == DelimTypeNone);
}

uint8_t TextRowReaderImpl::getByte(DelimType& delim) {
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

bool TextRowReaderImpl::getEOR(DelimType& delim, bool& isNull) {
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
  std::string s;
  const auto& ns = contents_->serDeOptions.nullString;
  uint8_t v = 0;
  while (true) {
    v = getByteUnchecked(delim);
    if (isNone(delim)) {
      if (v == '\r') {
        v = getByteUnchecked<true>(delim); // always returns '\n' in this case
      }
      delim = getDelimType(v);
    }

    if (isEOR(delim) || atEOL_) {
      if (s == ns) {
        isNull = true;
      } else if (!s.empty()) {
        break;
      }
      setEOR(delim);
      return true;
    }
    if (s.size() >= ns.size() || static_cast<char>(v) != ns[s.size()]) {
      break;
    }
    s.push_back(static_cast<char>(v));
  }

  unreadData_.insert(0, 1, static_cast<char>(v));
  pos_--;
  if (!s.empty()) {
    unreadData_.insert(0, s);
    pos_ -= s.size();
  }
  atEOL_ = false;
  atSOL_ = wasAtSOL;
  setNone(delim);
  return false;
}

bool TextRowReaderImpl::skipLine() {
  DelimType delim = DelimTypeNone;
  while (!atEOL_) {
    (void)getByte(delim);
  }
  if (pos_ > limit_) {
    atEOF_ = true;
  }
  return atEOF_;
}

void TextRowReaderImpl::resetLine() {
  stringViewBuffer_ = StringViewBufferHolder(&contents_->pool);
  if (!atEOF_) {
    atEOL_ = false;
    DWIO_ENSURE_EQ(depth_, 0);
  }
  atSOL_ = true;
}

StringView TextRowReaderImpl::getStringView(
    TextRowReaderImpl& th,
    bool& isNull,
    DelimType& delim) {
  if (th.atEOL_) {
    delim = DelimTypeEOR; // top-level EOR
  }

  if (th.isEOEorEOR(delim)) {
    isNull = true;
    return StringView("");
  }

  std::string s;
  bool wasEscaped = false;
  while (true) {
    auto v = th.getByte(delim);
    if (!th.isNone(delim)) {
      break;
    }
    if (th.contents_->serDeOptions.isEscaped &&
        v == th.contents_->serDeOptions.escapeChar) {
      wasEscaped = true;
      s.append(1, static_cast<char>(v));
      v = th.getByteUnchecked(delim);
      if (!th.isNone(delim)) {
        break;
      }
    }
    s.append(1, static_cast<char>(v));
  }

  if (s == th.contents_->serDeOptions.nullString) {
    isNull = true;
    return StringView("");
  }

  if (wasEscaped) {
    // We need to copy the data byte by byte only if there is at least one
    // escaped byte.
    uint64_t j = 0;
    for (uint64_t i = 0; i < s.length(); i++) {
      if (s[i] == th.contents_->serDeOptions.escapeChar && i < s.length() - 1) {
        // Check if it's '\r' or '\n'.
        i++;
        if (s[i] == 'r') {
          s[j++] = '\r';
        } else if (s[i] == 'n') {
          s[j++] = '\n';
        } else {
          // Keep the next byte.
          s[j++] = s[i];
        }
      } else {
        s[j++] = s[i];
      }
    }
    s.resize(j);
  }

  return th.stringViewBuffer_.getOwnedValue(s);
}

template <typename T>
T TextRowReaderImpl::getInteger(
    TextRowReaderImpl& th,
    bool& isNull,
    DelimType& delim) {
  auto s = getStringView(th, isNull, delim);

  if (s.empty()) {
    isNull = true;
  }
  if (isNull) {
    return 0;
  }

  // Test if s is not acceptable integer format for
  // the warehouse, for cases accepted by stol().
  auto strRef = s.data();
  auto c = strRef[0];
  if (c != '-' && !std::isdigit(static_cast<unsigned char>(c))) {
    isNull = true;
    return 0;
  }

  int64_t v = 0;
  unsigned long long scanPos = 0;
  errno = 0;
  auto scanCount = sscanf(s.data(), "%" SCNd64 "%lln", &v, &scanPos);
  if (scanCount != 1 || errno == ERANGE) {
    isNull = true;
    return 0;
  }
  if (scanPos < s.size()) {
    // Check if the string is a valid decimal.
    for (uint64_t i = scanPos; i < s.size(); i++) {
      if (i == scanPos && strRef[i] == '.') {
        continue;
      }
      if (strRef[i] >= '0' && strRef[i] <= '9') {
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

bool TextRowReaderImpl::getBoolean(
    TextRowReaderImpl& th,
    bool& isNull,
    DelimType& delim) {
  auto s = getStringView(th, isNull, delim);
  if (s.empty()) {
    isNull = true;
  }
  if (isNull) {
    return false;
  }
  if (s.compare(trueStringView) == 0) {
    return true;
  }
  if (s.compare(falseStringView) == 0) {
    return false;
  }

  auto strRef = s.data();
  switch (s.size()) {
    case 4:
      if (folly::StringPiece(strRef).equals(
              "TRUE", folly::AsciiCaseInsensitive())) {
        return true;
      }
      break;
    case 5:
      if (folly::StringPiece(strRef).equals(
              "FALSE", folly::AsciiCaseInsensitive())) {
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

bool unacceptableFloatingPoint(std::string& s) {
  for (auto c : s) {
    if (!(std::isalpha(c) || c == '-')) {
      return false;
    }
  }
  return (s != "NaN" && s != "Infinity" && s != "-Infinity");
}

} // namespace

float TextRowReaderImpl::getFloat(
    TextRowReaderImpl& th,
    bool& isNull,
    DelimType& delim) {
  auto strView = getStringView(th, isNull, delim);
  if (strView.empty()) {
    isNull = true;
  }
  if (isNull) {
    return 0;
  }

  auto str = strView.getString();
  trim(str);
  if (str[0] == '.') {
    str.insert(0, 1, '0');
  }

  // Filter out values from non-warehouse sources which
  // other readers translate to null. Warehouse
  // readers require upper-case values.
  if (unacceptableFloatingPoint(str)) {
    isNull = true;
    return 0.0;
  }

  float v = 0.0;
  unsigned long long scanPos = 0;
  // We ignore ERANGE, since denormalized floats and
  // infinities are acceptable.
  auto scanCount = sscanf(str.data(), "%f%lln", &v, &scanPos);
  if (scanCount != 1 || scanPos < str.size()) {
    isNull = true;
    return 0.0;
  }
  return v;
}

double TextRowReaderImpl::getDouble(
    TextRowReaderImpl& th,
    bool& isNull,
    DelimType& delim) {
  auto strView = getStringView(th, isNull, delim);
  if (strView.empty()) {
    isNull = true;
  }
  if (isNull) {
    return 0;
  }

  auto str = strView.getString();
  trim(str);

  if (str[0] == '.') {
    str.insert(0, 1, '0');
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
  auto scanCount = sscanf(str.data(), "%lf%lln", &v, &scanPos);
  if (scanCount != 1 || scanPos < str.size()) {
    isNull = true;
    return 0.0;
  }
  return v;
}

void TextRowReaderImpl::trim(std::string& s) {
  const auto isNotSpace = [](unsigned char ch) { return ch > 0x20; };
  const auto strBegin = std::find_if(s.begin(), s.end(), isNotSpace);
  if (strBegin == s.end()) {
    s = ""; // The string is all whitespace.
    return;
  }
  s.erase(0, strBegin - s.begin());
  s.erase(std::find_if(s.rbegin(), s.rend(), isNotSpace).base(), s.end());
}

void TextRowReaderImpl::readElement(
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
          unsupportedBatchType(data);
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
          unsupportedBatchType(data);
          break;
      }
      break;

    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR: {
      auto strView = getStringView(*this, isNull, delim);
      auto flatVector = data->asChecked<FlatVector<StringView>>();
      if (!flatVector) {
        VELOX_FAIL("Expected FlatVector but got {}", typeid(*data).name());
        return;
      }

      if ((atEOF_ && atSOL_) || (flatVector == nullptr)) {
        break;
      }

      flatVector->set(insertionRow, strView);

      if (isNull) {
        flatVector->setNull(insertionRow, true);
        flatVector->setNullCount(flatVector->getNullCount().value_or(0) + 1);
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
          unsupportedBatchType(data);
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
          unsupportedBatchType(data);
          break;
      }
      break;

    case TypeKind::ARRAY: {
      const auto& ct = t->childAt(0);
      const auto arrayVector = data->asChecked<ArrayVector>();
      incrementDepth();
      (void)getEOR(delim, isNull);

      if (arrayVector != nullptr) {
        auto rawSizes = arrayVector->sizes()->asMutable<vector_size_t>();
        auto rawOffsets = arrayVector->offsets()->asMutable<vector_size_t>();
        const int startElementIdx = rawOffsets[insertionRow];
        vector_size_t elementCount = 0;

        if (isNull) {
          arrayVector->setNull(insertionRow, isNull);
          arrayVector->setNullCount(
              arrayVector->getNullCount().value_or(0) + 1);
        } else {
          // Read elements until we reach the end of the array
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
            elementCount++;

            if (atEOF_ && atSOL_) {
              decrementDepth(delim);
              return;
            }
          }

          rawSizes[insertionRow] = elementCount;
        }

        /**
        TODO: Redundant rawOffsets update. Only update the next rawOffsets,
        remember to update rawOffsets even if its null forthis to work
        */
        for (auto i = insertionRow + 1; i <= arrayVector->size(); ++i) {
          rawOffsets[i] = startElementIdx + elementCount;
        }

      } else {
        // skip over array data to maintain correct stream position
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
      const auto& rowVector = data->asChecked<RowVector>();
      incrementDepth();

      if (rowVector != nullptr) {
        if (isNull) {
          rowVector->setNull(insertionRow, isNull);
          rowVector->setNullCount(rowVector->getNullCount().value_or(0) + 1);
        } else {
          for (uint64_t j = 0; j < childCount; j++) {
            if (!isOuterEOR(delim)) {
              setNone(delim);
            }

            // Get the child vector for this field
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
        // Skip over row data to maintain correct stream position
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
      auto& mapt = t->asMap();
      const auto& key = mapt.keyType();
      const auto& value = mapt.valueType();
      auto mapVector = data->asChecked<MapVector>();
      incrementDepth();
      (void)getEOR(delim, isNull);

      if (mapVector != nullptr) {
        auto rawOffsets = mapVector->offsets()->asMutable<vector_size_t>();
        auto rawSizes = mapVector->sizes()->asMutable<vector_size_t>();
        const auto startElementIdx = rawOffsets[insertionRow];
        vector_size_t elementCount = 0;
        if (isNull) {
          mapVector->setNull(insertionRow, isNull);
          mapVector->setNullCount(mapVector->getNullCount().value_or(0) + 1);
        } else {
          while (!isOuterEOR(delim)) {
            // decode another element
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

            // case for no value key
            if (atEOF_ && atSOL_) {
              rawSizes[insertionRow] = elementCount;
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

            ++elementCount;
            decrementDepth(delim);
          }

          rawSizes[insertionRow] = elementCount;
        }
        /**
        TODO: Redundant rawOffsets update. Only update the next
        rawOffsets, similar to Array
        */
        for (auto i = insertionRow + 1; i <= mapVector->size(); ++i) {
          rawOffsets[i] = startElementIdx + elementCount;
        }
      } else {
        // skip over map data to maintain correct stream position
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
          putValue<float, double>(getFloat, data, insertionRow, delim);
          break;
        default:
          unsupportedBatchType(data);
          break;
      }
      break;

    case TypeKind::DOUBLE:
      putValue<double, double>(getDouble, data, insertionRow, delim);
      break;

    case TypeKind::TIMESTAMP: {
      auto s = getStringView(*this, isNull, delim);
      // Early return if no data vector or at EOF
      if ((atEOF_ && atSOL_) || (data == nullptr)) {
        return;
      }

      auto flatVector = data->asChecked<FlatVector<Timestamp>>();
      if (!flatVector) {
        VELOX_FAIL("Expected FlatVector but got {}", typeid(*data).name());
        return;
      }

      if (s.empty()) {
        isNull = true;
        flatVector->setNull(insertionRow, true);
        flatVector->setNullCount(flatVector->getNullCount().value_or(0) + 1);
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
void TextRowReaderImpl::putValue(
    std::function<T(TextRowReaderImpl& th, bool& isNull, DelimType& delim)> f,
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
  auto flatVector = data->asChecked<FlatVector<reqT>>();
  if (!flatVector) {
    VELOX_FAIL("Expected FlatVector but got {}", typeid(*data).name());
    return;
  }

  flatVector->set(insertionRow, v);

  // handle null property
  if (isNull) {
    flatVector->setNull(insertionRow, true);
    flatVector->setNullCount(flatVector->getNullCount().value_or(0) + 1);
  }
}

TextReaderImpl::TextReaderImpl(
    std::unique_ptr<BufferedInput> input,
    const ReaderOptions& opts)
    : options_{opts} {
  auto schema = options_.fileSchema();
  if (!schema) {
    // create dummy for testing.
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

  // Find the size of the file using the option or filesystem
  contents_->fileLength = std::min(
      options_.tailLocation(),
      static_cast<uint64_t>(contents_->input->getInputStream()->getLength()));

  // Set the SerDe options.
  contents_->serDeOptions = options_.serDeOptions();
  if (contents_->serDeOptions.isEscaped) {
    for (auto delim : contents_->serDeOptions.separators) {
      contents_->needsEscape.at(delim) = true;
    }
    contents_->needsEscape.at(contents_->serDeOptions.escapeChar) = true;
  }

  // Set up the compression codec
  contents_->compression = CompressionKind::CompressionKind_NONE;
  auto& filename = contents_->input->getName();

  if (filename.size() > TEXTFILE_COMPRESSION_EXTENSION.size() &&
      filename.rfind(TEXTFILE_COMPRESSION_EXTENSION) ==
          (filename.size() - TEXTFILE_COMPRESSION_EXTENSION.size())) {
    contents_->compression = CompressionKind::CompressionKind_ZLIB;
  }

  if (filename.size() > TEXTFILE_COMPRESSION_EXTENSION_RAW.size() &&
      filename.rfind(TEXTFILE_COMPRESSION_EXTENSION_RAW) ==
          (filename.size() - TEXTFILE_COMPRESSION_EXTENSION_RAW.size())) {
    contents_->compression = CompressionKind::CompressionKind_ZLIB;
  }

  /// TODO: COMPLETE IMPLEMENTATION
  if (contents_->compression != CompressionKind::CompressionKind_NONE) {
    VELOX_UNSUPPORTED("Decompression not supported");
  }
}

const std::shared_ptr<const RowType>& TextRowReaderImpl::getType() const {
  return contents_->schema;
}

DelimType TextRowReaderImpl::getDelimType(uint8_t v) {
  DelimType delim = DelimTypeNone;

  if (v == '\n') {
    atEOL_ = true;
    delim = DelimTypeEOR; // top level EOR
    if (pos_ > limit_) {
      atEOF_ = true;
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
char TextRowReaderImpl::getByteUnchecked(DelimType& delim) {
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
    delim = 1;
  }
  return '\n';
}

std::optional<uint64_t> TextReaderImpl::numberOfRows() const {
  return std::nullopt;
}

std::unique_ptr<ColumnStatistics> TextReaderImpl::columnStatistics(
    uint32_t /*index*/) const {
  return nullptr;
}

const std::shared_ptr<const RowType>& TextReaderImpl::rowType() const {
  return contents_->schema;
}

CompressionKind TextReaderImpl::getCompression() const {
  return contents_->compression;
}

const std::shared_ptr<const TypeWithId>& TextReaderImpl::typeWithId() const {
  VELOX_UNSUPPORTED("Do not access from reader_");
}

std::unique_ptr<RowReader> TextReaderImpl::createRowReader(
    const RowReaderOptions& opts) const {
  return std::make_unique<TextRowReaderImpl>(contents_, opts);
}

uint64_t TextReaderImpl::getFileLength() const {
  return contents_->fileLength;
}

uint64_t TextReaderImpl::getMemoryUse() {
  uint64_t memory = std::min(
      uint64_t(contents_->fileLength),
      contents_->input->getInputStream()->getNaturalReadSize());

  // Decompressor needs a buffer
  if (contents_->compression != CompressionKind::CompressionKind_NONE) {
    memory *= 3;
  }

  return memory;
}

} // namespace facebook::velox::dwio::common
