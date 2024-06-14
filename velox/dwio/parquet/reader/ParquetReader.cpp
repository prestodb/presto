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

#include "velox/dwio/parquet/reader/ParquetReader.h"

#include <boost/algorithm/string.hpp>
#include <thrift/protocol/TCompactProtocol.h> //@manual

#include "velox/dwio/parquet/reader/ParquetColumnReader.h"
#include "velox/dwio/parquet/reader/StructColumnReader.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"

namespace facebook::velox::parquet {

/// Metadata and options for reading Parquet.
class ReaderBase {
 public:
  ReaderBase(
      std::unique_ptr<dwio::common::BufferedInput>,
      const dwio::common::ReaderOptions& options);

  virtual ~ReaderBase() = default;

  memory::MemoryPool& getMemoryPool() const {
    return pool_;
  }

  dwio::common::BufferedInput& bufferedInput() const {
    return *input_;
  }

  uint64_t fileLength() const {
    return fileLength_;
  }

  const thrift::FileMetaData& thriftFileMetaData() const {
    return *fileMetaData_;
  }

  FileMetaDataPtr fileMetaData() const {
    return FileMetaDataPtr(reinterpret_cast<const void*>(fileMetaData_.get()));
  }

  const std::shared_ptr<const RowType>& schema() const {
    return schema_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& schemaWithId() {
    return schemaWithId_;
  }

  bool isFileColumnNamesReadAsLowerCase() const {
    return options_.fileColumnNamesReadAsLowerCase();
  }

  /// Ensures that streams are enqueued and loading for the row group at
  /// 'currentGroup'. May start loading one or more subsequent groups.
  void scheduleRowGroups(
      const std::vector<uint32_t>& groups,
      int32_t currentGroup,
      StructColumnReader& reader);

  /// Returns the uncompressed size for columns in 'type' and its children in
  /// row group.
  int64_t rowGroupUncompressedSize(
      int32_t rowGroupIndex,
      const dwio::common::TypeWithId& type) const;

  /// Checks whether the specific row group has been loaded and
  /// the data still exists in the buffered inputs.
  bool isRowGroupBuffered(int32_t rowGroupIndex) const;

 private:
  // Reads and parses file footer.
  void loadFileMetaData();

  void initializeSchema();

  std::unique_ptr<ParquetTypeWithId> getParquetColumnInfo(
      uint32_t maxSchemaElementIdx,
      uint32_t maxRepeat,
      uint32_t maxDefine,
      uint32_t parentSchemaIdx,
      uint32_t& schemaIdx,
      uint32_t& columnIdx) const;

  TypePtr convertType(const thrift::SchemaElement& schemaElement) const;

  template <typename T>
  static std::shared_ptr<const RowType> createRowType(
      const std::vector<T>& children,
      bool fileColumnNamesReadAsLowerCase);

  memory::MemoryPool& pool_;
  const uint64_t footerEstimatedSize_;
  const uint64_t filePreloadThreshold_;
  // Copy of options. Must be owned by 'this'.
  const dwio::common::ReaderOptions options_;
  std::shared_ptr<velox::dwio::common::BufferedInput> input_;
  uint64_t fileLength_;
  std::unique_ptr<thrift::FileMetaData> fileMetaData_;
  RowTypePtr schema_;
  std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;

  const bool binaryAsString = false;

  // Map from row group index to pre-created loading BufferedInput.
  std::unordered_map<uint32_t, std::shared_ptr<dwio::common::BufferedInput>>
      inputs_;
};

ReaderBase::ReaderBase(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : pool_{options.memoryPool()},
      footerEstimatedSize_{options.footerEstimatedSize()},
      filePreloadThreshold_{options.filePreloadThreshold()},
      options_{options},
      input_{std::move(input)},
      fileLength_{input_->getReadFile()->size()} {
  VELOX_CHECK_GT(fileLength_, 0, "Parquet file is empty");
  VELOX_CHECK_GE(fileLength_, 12, "Parquet file is too small");

  loadFileMetaData();
  initializeSchema();
}

void ReaderBase::loadFileMetaData() {
  bool preloadFile =
      fileLength_ <= std::max(filePreloadThreshold_, footerEstimatedSize_);
  uint64_t readSize = preloadFile ? fileLength_ : footerEstimatedSize_;

  std::unique_ptr<dwio::common::SeekableInputStream> stream;
  if (preloadFile) {
    stream = input_->loadCompleteFile();
  } else {
    stream = input_->read(
        fileLength_ - readSize, readSize, dwio::common::LogType::FOOTER);
  }

  std::vector<char> copy(readSize);
  const char* bufferStart = nullptr;
  const char* bufferEnd = nullptr;
  dwio::common::readBytes(
      readSize, stream.get(), copy.data(), bufferStart, bufferEnd);
  VELOX_CHECK(
      strncmp(copy.data() + readSize - 4, "PAR1", 4) == 0,
      "No magic bytes found at end of the Parquet file");

  uint32_t footerLength;
  std::memcpy(&footerLength, copy.data() + readSize - 8, sizeof(uint32_t));
  VELOX_CHECK_LE(footerLength + 12, fileLength_);
  int32_t footerOffsetInBuffer = readSize - 8 - footerLength;
  if (footerLength > readSize - 8) {
    footerOffsetInBuffer = 0;
    auto missingLength = footerLength - readSize + 8;
    stream = input_->read(
        fileLength_ - footerLength - 8,
        missingLength,
        dwio::common::LogType::FOOTER);
    copy.resize(footerLength);
    std::memmove(copy.data() + missingLength, copy.data(), readSize - 8);
    bufferStart = nullptr;
    bufferEnd = nullptr;
    dwio::common::readBytes(
        missingLength, stream.get(), copy.data(), bufferStart, bufferEnd);
  }

  std::shared_ptr<thrift::ThriftTransport> thriftTransport =
      std::make_shared<thrift::ThriftBufferedTransport>(
          copy.data() + footerOffsetInBuffer, footerLength);
  auto thriftProtocol = std::make_unique<
      apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport>>(
      thriftTransport);
  fileMetaData_ = std::make_unique<thrift::FileMetaData>();
  fileMetaData_->read(thriftProtocol.get());
}

void ReaderBase::initializeSchema() {
  if (fileMetaData_->__isset.encryption_algorithm) {
    VELOX_UNSUPPORTED("Encrypted Parquet files are not supported");
  }

  VELOX_CHECK_GT(
      fileMetaData_->schema.size(),
      1,
      "Invalid Parquet schema: Need at least one non-root column in the file");
  VELOX_CHECK_EQ(
      fileMetaData_->schema[0].repetition_type,
      thrift::FieldRepetitionType::REQUIRED,
      "Invalid Parquet schema: root element must be REQUIRED");
  VELOX_CHECK_GT(
      fileMetaData_->schema[0].num_children,
      0,
      "Invalid Parquet schema: root element must have at least 1 child");

  uint32_t maxDefine = 0;
  uint32_t maxRepeat = 0;
  uint32_t schemaIdx = 0;
  uint32_t columnIdx = 0;
  uint32_t maxSchemaElementIdx = fileMetaData_->schema.size() - 1;
  // Setting the parent schema index of the root("hive_schema") to be 0, which
  // is the root itself. This is ok because it's never required to check the
  // parent of the root in getParquetColumnInfo().
  schemaWithId_ = getParquetColumnInfo(
      maxSchemaElementIdx, maxRepeat, maxDefine, 0, schemaIdx, columnIdx);
  schema_ = createRowType(
      schemaWithId_->getChildren(), isFileColumnNamesReadAsLowerCase());
}

std::unique_ptr<ParquetTypeWithId> ReaderBase::getParquetColumnInfo(
    uint32_t maxSchemaElementIdx,
    uint32_t maxRepeat,
    uint32_t maxDefine,
    uint32_t parentSchemaIdx,
    uint32_t& schemaIdx,
    uint32_t& columnIdx) const {
  VELOX_CHECK(fileMetaData_ != nullptr);
  VELOX_CHECK_LT(schemaIdx, fileMetaData_->schema.size());

  auto& schema = fileMetaData_->schema;
  uint32_t curSchemaIdx = schemaIdx;
  auto& schemaElement = schema[curSchemaIdx];
  bool isRepeated = false;
  bool isOptional = false;

  if (schemaElement.__isset.repetition_type) {
    if (schemaElement.repetition_type !=
        thrift::FieldRepetitionType::REQUIRED) {
      maxDefine++;
    }
    if (schemaElement.repetition_type ==
        thrift::FieldRepetitionType::REPEATED) {
      maxRepeat++;
      isRepeated = true;
    }
    if (schemaElement.repetition_type ==
        thrift::FieldRepetitionType::OPTIONAL) {
      isOptional = true;
    }
  }

  auto name = schemaElement.name;
  if (isFileColumnNamesReadAsLowerCase()) {
    folly::toLowerAscii(name);
  }
  if (!schemaElement.__isset.type) { // inner node
    VELOX_CHECK(
        schemaElement.__isset.num_children && schemaElement.num_children > 0,
        "Node has no children but should");

    std::vector<std::unique_ptr<ParquetTypeWithId::TypeWithId>> children;

    auto curSchemaIdx = schemaIdx;
    for (int32_t i = 0; i < schemaElement.num_children; i++) {
      auto child = getParquetColumnInfo(
          maxSchemaElementIdx,
          maxRepeat,
          maxDefine,
          curSchemaIdx,
          ++schemaIdx,
          columnIdx);
      children.push_back(std::move(child));
    }
    VELOX_CHECK(!children.empty());

    if (schemaElement.__isset.converted_type) {
      switch (schemaElement.converted_type) {
        case thrift::ConvertedType::MAP_KEY_VALUE:
          // If the MAP_KEY_VALUE annotated group's parent is a MAP, it should
          // be the repeated key_value group that directly contains the key and
          // value children.
          if (schema[parentSchemaIdx].converted_type ==
              thrift::ConvertedType::MAP) {
            // TODO: the group names need to be checked. According to the spec,
            // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
            // the name of the schema element being 'key_value' is
            // also an indication of this is a map type
            VELOX_CHECK_EQ(
                schemaElement.repetition_type,
                thrift::FieldRepetitionType::REPEATED);
            VELOX_CHECK_EQ(children.size(), 2);

            auto type = TypeFactory<TypeKind::MAP>::create(
                children[0]->type(), children[1]->type());
            return std::make_unique<ParquetTypeWithId>(
                std::move(type),
                std::move(children),
                curSchemaIdx, // TODO: there are holes in the ids
                maxSchemaElementIdx,
                ParquetTypeWithId::kNonLeaf, // columnIdx,
                std::move(name),
                std::nullopt,
                std::nullopt,
                maxRepeat,
                maxDefine,
                isOptional,
                isRepeated);
          }

          // For backward-compatibility, a group annotated with MAP_KEY_VALUE
          // that is not contained by a MAP-annotated group should be handled as
          // a MAP-annotated group.
          [[fallthrough]];

        case thrift::ConvertedType::LIST:
        case thrift::ConvertedType::MAP: {
          VELOX_CHECK_EQ(children.size(), 1);
          const auto& child = children[0];
          auto type = child->type();
          isRepeated = true;
          // This level will not have the "isRepeated" info in the parquet
          // schema since parquet schema will have a child layer which will have
          // the "repeated info" which we are ignoring here, hence we set the
          // isRepeated to true eg
          // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
          return std::make_unique<ParquetTypeWithId>(
              std::move(type),
              std::move(*(ParquetTypeWithId*)child.get()).moveChildren(),
              curSchemaIdx, // TODO: there are holes in the ids
              maxSchemaElementIdx,
              ParquetTypeWithId::kNonLeaf, // columnIdx,
              std::move(name),
              std::nullopt,
              std::nullopt,
              maxRepeat + 1,
              maxDefine,
              isOptional,
              isRepeated);
        }

        default:
          VELOX_UNREACHABLE(
              "Invalid SchemaElement converted_type: {}, name: {}",
              schemaElement.converted_type,
              schemaElement.name);
      }
    } else {
      if (schemaElement.repetition_type ==
          thrift::FieldRepetitionType::REPEATED) {
        if (schema[parentSchemaIdx].converted_type ==
            thrift::ConvertedType::LIST) {
          // TODO: the group names need to be checked. According to spec,
          // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
          // the name of the schema element being 'array' is
          // also an indication of this is a list type
          // child of LIST
          VELOX_CHECK_GE(children.size(), 1);
          if (children.size() == 1 && name != "array" &&
              name != schema[parentSchemaIdx].name + "_tuple") {
            auto type =
                TypeFactory<TypeKind::ARRAY>::create(children[0]->type());
            return std::make_unique<ParquetTypeWithId>(
                std::move(type),
                std::move(children),
                curSchemaIdx,
                maxSchemaElementIdx,
                ParquetTypeWithId::kNonLeaf, // columnIdx,
                std::move(name),
                std::nullopt,
                std::nullopt,
                maxRepeat,
                maxDefine,
                isOptional,
                isRepeated);
          } else {
            // According to the spec of list backward compatibility
            // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
            // "If the repeated field is a group with multiple fields, then its
            // type is the element type and elements are required." when there
            // are multiple fields, creating a new row type instance which has
            // all the fields as its children.
            // TODO: since this is newly created node, its schemaIdx actually
            // doesn't exist from the footer schema. Reusing the curSchemaIdx
            // but potentially could have issue.
            auto childrenRowType =
                createRowType(children, isFileColumnNamesReadAsLowerCase());
            std::vector<std::unique_ptr<ParquetTypeWithId::TypeWithId>>
                rowChildren;
            // In this legacy case, there is no middle layer between "array"
            // node and the children nodes. Below creates this dummy middle
            // layer to mimic the non-legacy case and fill the gap.
            rowChildren.emplace_back(std::make_unique<ParquetTypeWithId>(
                childrenRowType,
                std::move(children),
                curSchemaIdx,
                maxSchemaElementIdx,
                ParquetTypeWithId::kNonLeaf,
                "dummy",
                std::nullopt,
                std::nullopt,
                maxRepeat,
                maxDefine,
                isOptional,
                isRepeated));
            auto res = std::make_unique<ParquetTypeWithId>(
                TypeFactory<TypeKind::ARRAY>::create(childrenRowType),
                std::move(rowChildren),
                curSchemaIdx,
                maxSchemaElementIdx,
                ParquetTypeWithId::kNonLeaf, // columnIdx,
                std::move(name),
                std::nullopt,
                std::nullopt,
                maxRepeat,
                maxDefine,
                isOptional,
                isRepeated);
            return res;
          }
        } else if (
            schema[parentSchemaIdx].converted_type ==
                thrift::ConvertedType::MAP ||
            schema[parentSchemaIdx].converted_type ==
                thrift::ConvertedType::MAP_KEY_VALUE) {
          // children  of MAP
          VELOX_CHECK_EQ(children.size(), 2);
          auto type = TypeFactory<TypeKind::MAP>::create(
              children[0]->type(), children[1]->type());
          return std::make_unique<ParquetTypeWithId>(
              std::move(type),
              std::move(children),
              curSchemaIdx, // TODO: there are holes in the ids
              maxSchemaElementIdx,
              ParquetTypeWithId::kNonLeaf, // columnIdx,
              std::move(name),
              std::nullopt,
              std::nullopt,
              maxRepeat,
              maxDefine,
              isOptional,
              isRepeated);
        }
      } else {
        // Row type
        auto type = createRowType(children, isFileColumnNamesReadAsLowerCase());
        return std::make_unique<ParquetTypeWithId>(
            std::move(type),
            std::move(children),
            curSchemaIdx,
            maxSchemaElementIdx,
            ParquetTypeWithId::kNonLeaf, // columnIdx,
            std::move(name),
            std::nullopt,
            std::nullopt,
            maxRepeat,
            maxDefine,
            isOptional,
            isRepeated);
      }
    }
  } else { // leaf node
    const auto veloxType = convertType(schemaElement);
    int32_t precision =
        schemaElement.__isset.precision ? schemaElement.precision : 0;
    int32_t scale = schemaElement.__isset.scale ? schemaElement.scale : 0;
    int32_t type_length =
        schemaElement.__isset.type_length ? schemaElement.type_length : 0;
    std::vector<std::unique_ptr<dwio::common::TypeWithId>> children;
    const std::optional<thrift::LogicalType> logicalType_ =
        schemaElement.__isset.logicalType
        ? std::optional<thrift::LogicalType>(schemaElement.logicalType)
        : std::nullopt;
    auto leafTypePtr = std::make_unique<ParquetTypeWithId>(
        veloxType,
        std::move(children),
        curSchemaIdx,
        maxSchemaElementIdx,
        columnIdx++,
        name,
        schemaElement.type,
        logicalType_,
        maxRepeat,
        maxDefine,
        isOptional,
        isRepeated,
        precision,
        scale,
        type_length);

    if (schemaElement.repetition_type ==
        thrift::FieldRepetitionType::REPEATED) {
      // Array
      children.clear();
      children.reserve(1);
      children.push_back(std::move(leafTypePtr));
      return std::make_unique<ParquetTypeWithId>(
          TypeFactory<TypeKind::ARRAY>::create(veloxType),
          std::move(children),
          curSchemaIdx,
          maxSchemaElementIdx,
          columnIdx - 1, // was already incremented for leafTypePtr
          std::move(name),
          std::nullopt,
          std::nullopt,
          maxRepeat,
          maxDefine - 1,
          isOptional,
          isRepeated);
    }
    return leafTypePtr;
  }

  VELOX_FAIL("Unable to extract Parquet column info.")
  return nullptr;
}

TypePtr ReaderBase::convertType(
    const thrift::SchemaElement& schemaElement) const {
  VELOX_CHECK(schemaElement.__isset.type && schemaElement.num_children == 0);
  VELOX_CHECK(
      schemaElement.type != thrift::Type::FIXED_LEN_BYTE_ARRAY ||
          schemaElement.__isset.type_length,
      "FIXED_LEN_BYTE_ARRAY requires length to be set");

  if (schemaElement.__isset.converted_type) {
    switch (schemaElement.converted_type) {
      case thrift::ConvertedType::INT_8:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "INT8 converted type can only be set for value of thrift::Type::INT32");
        return TINYINT();

      case thrift::ConvertedType::INT_16:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "INT16 converted type can only be set for value of thrift::Type::INT32");
        return SMALLINT();

      case thrift::ConvertedType::INT_32:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "INT32 converted type can only be set for value of thrift::Type::INT32");
        return INTEGER();

      case thrift::ConvertedType::INT_64:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT64,
            "INT64 converted type can only be set for value of thrift::Type::INT64");
        return BIGINT();

      case thrift::ConvertedType::UINT_8:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "UINT_8 converted type can only be set for value of thrift::Type::INT32");
        return TINYINT();

      case thrift::ConvertedType::UINT_16:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "UINT_16 converted type can only be set for value of thrift::Type::INT32");
        return SMALLINT();

      case thrift::ConvertedType::UINT_32:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "UINT_32 converted type can only be set for value of thrift::Type::INT32");
        return INTEGER();

      case thrift::ConvertedType::UINT_64:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT64,
            "UINT_64 converted type can only be set for value of thrift::Type::INT64");
        return BIGINT();

      case thrift::ConvertedType::DATE:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "DATE converted type can only be set for value of thrift::Type::INT32");
        return DATE();

      case thrift::ConvertedType::TIMESTAMP_MICROS:
      case thrift::ConvertedType::TIMESTAMP_MILLIS:
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT64,
            "TIMESTAMP_MICROS or TIMESTAMP_MILLIS converted type can only be set for value of thrift::Type::INT64");
        return TIMESTAMP();

      case thrift::ConvertedType::DECIMAL: {
        VELOX_CHECK(
            schemaElement.__isset.precision && schemaElement.__isset.scale,
            "DECIMAL requires a length and scale specifier!");
        return DECIMAL(schemaElement.precision, schemaElement.scale);
      }

      case thrift::ConvertedType::UTF8:
        switch (schemaElement.type) {
          case thrift::Type::BYTE_ARRAY:
          case thrift::Type::FIXED_LEN_BYTE_ARRAY:
            return VARCHAR();
          default:
            VELOX_FAIL(
                "UTF8 converted type can only be set for thrift::Type::(FIXED_LEN_)BYTE_ARRAY");
        }
      case thrift::ConvertedType::ENUM: {
        VELOX_CHECK_EQ(
            schemaElement.type,
            thrift::Type::BYTE_ARRAY,
            "ENUM converted type can only be set for value of thrift::Type::BYTE_ARRAY");
        return VARCHAR();
      }
      case thrift::ConvertedType::MAP:
      case thrift::ConvertedType::MAP_KEY_VALUE:
      case thrift::ConvertedType::LIST:
      case thrift::ConvertedType::TIME_MILLIS:
      case thrift::ConvertedType::TIME_MICROS:
      case thrift::ConvertedType::JSON:
      case thrift::ConvertedType::BSON:
      case thrift::ConvertedType::INTERVAL:
      default:
        VELOX_FAIL(
            "Unsupported Parquet SchemaElement converted type: {}",
            schemaElement.converted_type);
    }
  } else {
    switch (schemaElement.type) {
      case thrift::Type::type::BOOLEAN:
        return BOOLEAN();
      case thrift::Type::type::INT32:
        return INTEGER();
      case thrift::Type::type::INT64:
        return BIGINT();
      case thrift::Type::type::INT96:
        return TIMESTAMP(); // INT96 only maps to a timestamp
      case thrift::Type::type::FLOAT:
        return REAL();
      case thrift::Type::type::DOUBLE:
        return DOUBLE();
      case thrift::Type::type::BYTE_ARRAY:
      case thrift::Type::type::FIXED_LEN_BYTE_ARRAY:
        if (binaryAsString) {
          return VARCHAR();
        } else {
          return VARBINARY();
        }

      default:
        VELOX_FAIL(
            "Unknown Parquet SchemaElement type: {}", schemaElement.type);
    }
  }
}

template <typename T>
std::shared_ptr<const RowType> ReaderBase::createRowType(
    const std::vector<T>& children,
    bool fileColumnNamesReadAsLowerCase) {
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  for (auto& child : children) {
    auto childName = static_cast<const ParquetTypeWithId&>(*child).name_;
    if (fileColumnNamesReadAsLowerCase) {
      folly::toLowerAscii(childName);
    }
    childNames.push_back(std::move(childName));
    childTypes.push_back(child->type());
  }
  return TypeFactory<TypeKind::ROW>::create(
      std::move(childNames), std::move(childTypes));
}

void ReaderBase::scheduleRowGroups(
    const std::vector<uint32_t>& rowGroupIds,
    int32_t currentGroup,
    StructColumnReader& reader) {
  auto numRowGroupsToLoad = std::min(
      options_.prefetchRowGroups() + 1,
      static_cast<int64_t>(rowGroupIds.size() - currentGroup));
  for (auto i = 0; i < numRowGroupsToLoad; i++) {
    auto thisGroup = rowGroupIds[currentGroup + i];
    if (!inputs_[thisGroup]) {
      inputs_[thisGroup] = reader.loadRowGroup(thisGroup, input_);
    }
  }

  if (currentGroup >= 1) {
    inputs_.erase(rowGroupIds[currentGroup - 1]);
  }
}

int64_t ReaderBase::rowGroupUncompressedSize(
    int32_t rowGroupIndex,
    const dwio::common::TypeWithId& type) const {
  if (type.column() != ParquetTypeWithId::kNonLeaf) {
    VELOX_CHECK_LT(rowGroupIndex, fileMetaData_->row_groups.size());
    VELOX_CHECK_LT(
        type.column(), fileMetaData_->row_groups[rowGroupIndex].columns.size());
    return fileMetaData_->row_groups[rowGroupIndex]
        .columns[type.column()]
        .meta_data.total_uncompressed_size;
  }
  int64_t sum = 0;
  for (auto child : type.getChildren()) {
    sum += rowGroupUncompressedSize(rowGroupIndex, *child);
  }
  return sum;
}

bool ReaderBase::isRowGroupBuffered(int32_t rowGroupIndex) const {
  return inputs_.count(rowGroupIndex) != 0;
}

namespace {
struct ParquetStatsContext : dwio::common::StatsContext {};
} // namespace

class ParquetRowReader::Impl {
 public:
  Impl(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options)
      : pool_{readerBase->getMemoryPool()},
        readerBase_{readerBase},
        options_{options},
        rowGroups_{readerBase_->thriftFileMetaData().row_groups},
        nextRowGroupIdsIdx_{0},
        currentRowGroupPtr_{nullptr},
        rowsInCurrentRowGroup_{0},
        currentRowInGroup_{0} {
    // Validate the requested type is compatible with what's in the file
    std::function<std::string()> createExceptionContext = [&]() {
      std::string exceptionMessageContext = fmt::format(
          "The schema loaded in the reader does not match the schema in the file footer."
          "Input Name: {},\n"
          "File Footer Schema (without partition columns): {},\n"
          "Input Table Schema (with partition columns): {}\n",
          readerBase_->bufferedInput().getReadFile()->getName(),
          readerBase_->schema()->toString(),
          requestedType_->toString());
      return exceptionMessageContext;
    };

    if (rowGroups_.empty()) {
      return; // TODO
    }
    ParquetParams params(
        pool_, columnReaderStats_, readerBase_->fileMetaData());
    requestedType_ = options_.requestedType() ? options_.requestedType()
                                              : readerBase_->schema();
    columnReader_ = ParquetColumnReader::build(
        requestedType_,
        readerBase_->schemaWithId(), // Id is schema id
        params,
        *options_.getScanSpec());
    columnReader_->setFillMutatedOutputRows(
        options_.getRowNumberColumnInfo().has_value());

    filterRowGroups();
    if (!rowGroupIds_.empty()) {
      // schedule prefetch of first row group right after reading the metadata.
      // This is usually on a split preload thread before the split goes to
      // table scan.
      advanceToNextRowGroup();
    }
  }

  void filterRowGroups() {
    rowGroupIds_.reserve(rowGroups_.size());
    firstRowOfRowGroup_.reserve(rowGroups_.size());

    ParquetData::FilterRowGroupsResult res;
    columnReader_->filterRowGroups(0, ParquetStatsContext(), res);
    if (auto& metadataFilter = options_.getMetadataFilter()) {
      metadataFilter->eval(res.metadataFilterResults, res.filterResult);
    }

    uint64_t rowNumber = 0;
    for (auto i = 0; i < rowGroups_.size(); i++) {
      VELOX_CHECK_GT(rowGroups_[i].columns.size(), 0);
      auto fileOffset = rowGroups_[i].__isset.file_offset
          ? rowGroups_[i].file_offset
          : rowGroups_[i].columns[0].meta_data.__isset.dictionary_page_offset
          ? rowGroups_[i].columns[0].meta_data.dictionary_page_offset
          : rowGroups_[i].columns[0].meta_data.data_page_offset;
      VELOX_CHECK_GT(fileOffset, 0);
      auto rowGroupInRange =
          (fileOffset >= options_.getOffset() &&
           fileOffset < options_.getLimit());

      auto isExcluded =
          (i < res.totalCount && bits::isBitSet(res.filterResult.data(), i));
      auto isEmpty = rowGroups_[i].num_rows == 0;

      // Add a row group to read if it is within range and not empty and not in
      // the excluded list.
      if (rowGroupInRange && !isExcluded && !isEmpty) {
        rowGroupIds_.push_back(i);
        firstRowOfRowGroup_.push_back(rowNumber);
      }
      rowNumber += rowGroups_[i].num_rows;
    }
  }

  int64_t nextRowNumber() {
    if (currentRowInGroup_ >= rowsInCurrentRowGroup_ &&
        !advanceToNextRowGroup()) {
      return kAtEnd;
    }
    return firstRowOfRowGroup_[nextRowGroupIdsIdx_ - 1] + currentRowInGroup_;
  }

  int64_t nextReadSize(uint64_t size) {
    VELOX_CHECK_GT(size, 0);
    if (nextRowNumber() == kAtEnd) {
      return kAtEnd;
    }
    return std::min(size, rowsInCurrentRowGroup_ - currentRowInGroup_);
  }

  uint64_t next(
      uint64_t size,
      velox::VectorPtr& result,
      const dwio::common::Mutation* mutation) {
    auto rowsToRead = nextReadSize(size);
    if (rowsToRead == kAtEnd) {
      return 0;
    }
    VELOX_DCHECK_GT(rowsToRead, 0);
    if (!options_.getRowNumberColumnInfo().has_value()) {
      columnReader_->next(rowsToRead, result, mutation);
    } else {
      readWithRowNumber(
          columnReader_,
          options_,
          nextRowNumber(),
          rowsToRead,
          mutation,
          result);
    }

    currentRowInGroup_ += rowsToRead;
    return rowsToRead;
  }

  std::optional<size_t> estimatedRowSize() const {
    auto index =
        nextRowGroupIdsIdx_ < 1 ? 0 : rowGroupIds_[nextRowGroupIdsIdx_ - 1];
    return readerBase_->rowGroupUncompressedSize(
               index, *readerBase_->schemaWithId()) /
        rowGroups_[index].num_rows;
  }

  void updateRuntimeStats(dwio::common::RuntimeStatistics& stats) const {
    stats.skippedStrides += rowGroups_.size() - rowGroupIds_.size();
  }

  void resetFilterCaches() {
    columnReader_->resetFilterCaches();
  }

  bool isRowGroupBuffered(int32_t rowGroupIndex) const {
    return readerBase_->isRowGroupBuffered(rowGroupIndex);
  }

 private:
  bool advanceToNextRowGroup() {
    if (nextRowGroupIdsIdx_ == rowGroupIds_.size()) {
      return false;
    }

    auto nextRowGroupIndex = rowGroupIds_[nextRowGroupIdsIdx_];
    readerBase_->scheduleRowGroups(
        rowGroupIds_,
        nextRowGroupIdsIdx_,
        static_cast<StructColumnReader&>(*columnReader_));
    currentRowGroupPtr_ = &rowGroups_[rowGroupIds_[nextRowGroupIdsIdx_]];
    rowsInCurrentRowGroup_ = currentRowGroupPtr_->num_rows;
    currentRowInGroup_ = 0;
    nextRowGroupIdsIdx_++;
    columnReader_->seekToRowGroup(nextRowGroupIndex);
    return true;
  }

  memory::MemoryPool& pool_;
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions options_;

  // All row groups from file metadata.
  const std::vector<thrift::RowGroup>& rowGroups_;
  // Indices of row groups where stats match filters.
  std::vector<uint32_t> rowGroupIds_;
  std::vector<uint64_t> firstRowOfRowGroup_;
  uint32_t nextRowGroupIdsIdx_;
  const thrift::RowGroup* currentRowGroupPtr_{nullptr};
  uint64_t rowsInCurrentRowGroup_;
  uint64_t currentRowInGroup_;

  std::unique_ptr<dwio::common::SelectiveColumnReader> columnReader_;

  TypePtr requestedType_;

  dwio::common::ColumnReaderStatistics columnReaderStats_;
};

ParquetRowReader::ParquetRowReader(
    const std::shared_ptr<ReaderBase>& readerBase,
    const dwio::common::RowReaderOptions& options) {
  impl_ = std::make_unique<ParquetRowReader::Impl>(readerBase, options);
}

void ParquetRowReader::filterRowGroups() {
  impl_->filterRowGroups();
}

int64_t ParquetRowReader::nextRowNumber() {
  return impl_->nextRowNumber();
}

int64_t ParquetRowReader::nextReadSize(uint64_t size) {
  return impl_->nextReadSize(size);
}

uint64_t ParquetRowReader::next(
    uint64_t size,
    velox::VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  return impl_->next(size, result, mutation);
}

void ParquetRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  impl_->updateRuntimeStats(stats);
}

void ParquetRowReader::resetFilterCaches() {
  impl_->resetFilterCaches();
}

bool ParquetRowReader::isRowGroupBuffered(int32_t rowGroupIndex) const {
  return impl_->isRowGroupBuffered(rowGroupIndex);
}

std::optional<size_t> ParquetRowReader::estimatedRowSize() const {
  return impl_->estimatedRowSize();
}

ParquetReader::ParquetReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : readerBase_(std::make_shared<ReaderBase>(std::move(input), options)) {}

std::optional<uint64_t> ParquetReader::numberOfRows() const {
  return readerBase_->thriftFileMetaData().num_rows;
}

const velox::RowTypePtr& ParquetReader::rowType() const {
  return readerBase_->schema();
}

const std::shared_ptr<const dwio::common::TypeWithId>&
ParquetReader::typeWithId() const {
  return readerBase_->schemaWithId();
}

std::unique_ptr<dwio::common::RowReader> ParquetReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<ParquetRowReader>(readerBase_, options);
}

FileMetaDataPtr ParquetReader::fileMetaData() const {
  return readerBase_->fileMetaData();
}

} // namespace facebook::velox::parquet
