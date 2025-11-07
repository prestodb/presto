/*
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
#include "presto_cpp/main/operators/BroadcastFile.h"
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/thrift/gen-cpp2/presto_native_types.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/file/File.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;
using namespace facebook::presto;

namespace facebook::presto::operators {

#define PRESTO_BROADCAST_LIMIT_EXCEEDED(errorMessage)                        \
  _VELOX_THROW(                                                              \
      ::facebook::velox::VeloxRuntimeError,                                  \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(),          \
      ::facebook::presto::error_code::kExceededLocalBroadcastJoinMemoryLimit \
          .c_str(),                                                          \
      /* isRetriable */ false,                                               \
      "{}",                                                                  \
      errorMessage);

// static
std::unique_ptr<BroadcastFileInfo> BroadcastFileInfo::deserialize(
    const std::string& info) {
  const auto root = nlohmann::json::parse(info);
  auto broadcastFileInfo = std::make_unique<BroadcastFileInfo>();
  root.at("filePath").get_to(broadcastFileInfo->filePath_);
  return broadcastFileInfo;
}

BroadcastFileWriter::BroadcastFileWriter(
    const std::string& pathPrefix,
    uint64_t maxBroadcastBytes,
    uint64_t writeBufferSize,
    std::unique_ptr<VectorSerde::Options> serdeOptions,
    velox::memory::MemoryPool* pool)
    : serializer::SerializedPageFileWriter(
          pathPrefix,
          std::numeric_limits<uint64_t>::max(),
          writeBufferSize,
          "",
          std::move(serdeOptions),
          getNamedVectorSerde(VectorSerde::Kind::kPresto),
          pool),
      maxBroadcastBytes_(maxBroadcastBytes) {}

void BroadcastFileWriter::write(const RowVectorPtr& rowVector) {
  const auto numRows = rowVector->size();
  IndexRange range{0, numRows};
  folly::Range<IndexRange*> ranges{&range, 1};
  serializer::SerializedPageFileWriter::write(rowVector, ranges);
  numRows_ += numRows;
}

void BroadcastFileWriter::updateWriteStats(
    uint64_t writtenBytes,
    uint64_t /* flushTimeNs */,
    uint64_t /* fileWriteTimeNs */) {
  writtenBytes_ += writtenBytes;
  if (FOLLY_UNLIKELY(writtenBytes_ > maxBroadcastBytes_)) {
    PRESTO_BROADCAST_LIMIT_EXCEEDED(
        fmt::format(
            "Storage broadcast join exceeded per task broadcast limit "
            "writtenBytes_ {} vs maxBroadcastBytes_ {}",
            succinctBytes(writtenBytes_),
            succinctBytes(maxBroadcastBytes_)));
  }
}

uint64_t BroadcastFileWriter::flush() {
  const auto pageBytes = serializer::SerializedPageFileWriter::flush();
  if (pageBytes != 0) {
    pageSizes_.push_back(pageBytes);
  }
  return pageBytes;
}

void BroadcastFileWriter::closeFile() {
  if (currentFile_ == nullptr) {
    return;
  }
  writeFooter();
  serializer::SerializedPageFileWriter::closeFile();
}

void BroadcastFileWriter::writeFooter() {
  VELOX_CHECK(!pageSizes_.empty());

  facebook::presto::thrift::BroadcastFileFooter thriftFooter;
  thriftFooter.pageSizes_ref() = pageSizes_;
  auto serializedFooterBuf = thriftWriteIOBuf(thriftFooter);

  int64_t footerSize =
      static_cast<int64_t>(serializedFooterBuf->computeChainDataLength());
  auto sizeBuf = folly::IOBuf::create(sizeof(footerSize));
  sizeBuf->append(sizeof(footerSize));
  std::memcpy(sizeBuf->writableData(), &footerSize, sizeof(footerSize));

  currentFile_->write(std::move(serializedFooterBuf));
  currentFile_->write(std::move(sizeBuf));
}

void BroadcastFileWriter::noMoreData() {
  const auto fileInfos = serializer::SerializedPageFileWriter::finish();
  if (fileInfos.empty()) {
    return;
  }
  VELOX_CHECK_EQ(fileInfos.size(), 1);

  // Return stats for the single file with multiple pages
  auto fileNameVector =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_);
  auto maxSerializedSizeVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);
  auto numRowsVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);

  fileNameVector->set(0, StringView(fileInfos.back().path));
  maxSerializedSizeVector->set(0, fileInfos.back().size);
  numRowsVector->set(0, numRows_);

  fileStats_ = std::make_shared<RowVector>(
      pool_,
      ROW({"filepath", "maxserializedsize", "numrows"},
          {VARCHAR(), BIGINT(), BIGINT()}),
      nullptr,
      1,
      std::vector<VectorPtr>(
          {std::move(fileNameVector),
           std::move(maxSerializedSizeVector),
           std::move(numRowsVector)}));
}

RowVectorPtr BroadcastFileWriter::fileStats() {
  return fileStats_;
}

BroadcastFileReader::BroadcastFileReader(
    std::unique_ptr<BroadcastFileInfo>& broadcastFileInfo,
    std::shared_ptr<velox::filesystems::FileSystem> fileSystem,
    velox::memory::MemoryPool* pool)
    : pool_(pool), broadcastFileInfo_(std::move(broadcastFileInfo)) {
  auto readFile = fileSystem->openFileForRead(broadcastFileInfo_->filePath_);

  // Read footer first using raw pointer
  readFooter(readFile.get());

  // Then move the file to the input stream
  inputStream_ = std::make_unique<velox::common::FileInputStream>(
      std::move(readFile), 8 * 1024 * 1024, pool); // 8MB buffer
}

bool BroadcastFileReader::hasNext() {
  return numPagesRead_ < pageSizes_.size();
}

velox::BufferPtr BroadcastFileReader::next() {
  if (!hasNext()) {
    return nullptr;
  }

  int64_t pageSize = pageSizes_[numPagesRead_];
  VELOX_CHECK_GT(
      pageSize,
      0,
      "Invalid page size {} for page {} in broadcast file {}",
      pageSize,
      numPagesRead_,
      broadcastFileInfo_->filePath_);

  auto pageBuffer = AlignedBuffer::allocate<char>(pageSize, pool_, 0);
  inputStream_->readBytes(
      reinterpret_cast<uint8_t*>(pageBuffer->asMutable<char>()), pageSize);

  numBytes_ += pageSize;
  numPagesRead_++;

  return pageBuffer;
}

void BroadcastFileReader::readFooter(velox::ReadFile* readFile) {
  VELOX_CHECK(
      pageSizes_.empty(),
      "readFooter() called when footer already read for broadcast file {}",
      broadcastFileInfo_->filePath_);

  const auto fileSize = readFile->size();
  VELOX_CHECK_GT(fileSize, sizeof(int64_t));

  // Read footer size from the end of file
  int64_t footerSize;
  readFile->pread(
      fileSize - sizeof(footerSize),
      sizeof(footerSize),
      reinterpret_cast<char*>(&footerSize));

  // Validate footer size - must be valid if present
  VELOX_CHECK_GT(
      footerSize,
      0,
      "Invalid footer size {} in broadcast file {}",
      footerSize,
      broadcastFileInfo_->filePath_);

  VELOX_CHECK_LT(
      footerSize,
      fileSize,
      "Footer size {} must be less than file size {} in broadcast file {}",
      footerSize,
      fileSize,
      broadcastFileInfo_->filePath_);

  // Read the serialized thrift footer
  uint64_t footerOffset = fileSize - footerSize - sizeof(footerSize);
  std::string serializedFooter(footerSize, '\0');
  readFile->pread(footerOffset, footerSize, serializedFooter.data());

  // Deserialize the thrift footer
  auto thriftFooter =
      std::make_shared<facebook::presto::thrift::BroadcastFileFooter>();
  thriftRead(serializedFooter, thriftFooter);

  // Extract page sizes from thrift footer
  pageSizes_ = thriftFooter->pageSizes_ref().value();

  // Validate the footer contents
  VELOX_CHECK_GT(
      pageSizes_.size(),
      0,
      "Invalid number of pages {} in footer of broadcast file {}",
      pageSizes_.size(),
      broadcastFileInfo_->filePath_);
}

std::vector<int64_t> BroadcastFileReader::remainingPageSizes() const {
  if (pageSizes_.empty() || numPagesRead_ >= pageSizes_.size()) {
    return {}; // No remaining pages
  }

  // Return the portion of pageSizes_ that hasn't been read yet
  return std::vector<int64_t>(
      pageSizes_.begin() + numPagesRead_, pageSizes_.end());
}

folly::F14FastMap<std::string, int64_t> BroadcastFileReader::stats() {
  return {
      {"broadcastExchangeSource.numBytes", numBytes_},
      {"broadcastExchangeSource.numPages", numPagesRead_}};
}

} // namespace facebook::presto::operators
