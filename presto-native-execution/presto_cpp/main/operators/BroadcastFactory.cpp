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
#include "presto_cpp/main/operators/BroadcastFactory.h"
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "velox/common/file/File.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

namespace {
std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}
} // namespace

BroadcastFactory::BroadcastFactory(const std::string& basePath)
    : basePath_(basePath) {
  VELOX_CHECK(!basePath.empty(), "Base path for broadcast files is empty!");
  fileSystem_ = velox::filesystems::getFileSystem(basePath, nullptr);
}

std::unique_ptr<BroadcastFileWriter> BroadcastFactory::createWriter(
    const velox::RowTypePtr& inputType,
    uint64_t maxPageSize,
    velox::memory::MemoryPool* pool) {
  fileSystem_->mkdir(basePath_);
  return std::make_unique<BroadcastFileWriter>(
      fmt::format("{}/file_broadcast_{}", basePath_, makeUuid()),
      inputType,
      maxPageSize,
      pool);
}

std::shared_ptr<BroadcastFileReader> BroadcastFactory::createReader(
    std::unique_ptr<BroadcastFileInfo> fileInfo,
    velox::memory::MemoryPool* pool) {
  auto broadcastFileReader =
      std::make_shared<BroadcastFileReader>(fileInfo, fileSystem_, pool);
  return broadcastFileReader;
}

// static
std::unique_ptr<BroadcastFileInfo> BroadcastFileInfo::deserialize(
    const std::string& info) {
  const auto root = nlohmann::json::parse(info);
  auto broadcastFileInfo = std::make_unique<BroadcastFileInfo>();
  root.at("filePath").get_to(broadcastFileInfo->filePath_);
  return broadcastFileInfo;
}

BroadcastFileWriter::BroadcastFileWriter(
    const std::string& filename,
    const RowTypePtr& inputType,
    uint64_t maxPageSize,
    velox::memory::MemoryPool* pool)
    : inputType_(inputType),
      maxPageSize_(maxPageSize),
      pool_(pool),
      currentStreamArena_(std::make_unique<velox::StreamArena>(pool_)),
      serde_(std::make_unique<serializer::presto::PrestoVectorSerde>()) {
  writeFile_ = velox::filesystems::getFileSystem(filename, nullptr)
                   ->openFileForWrite(filename);
}

void BroadcastFileWriter::maybeInitializeSerializer() {
  if (currentPageSerializer_ != nullptr) {
    return;
  }
  currentPageSerializer_ = serde_->createIterativeSerializer(
      inputType_, 1, currentStreamArena_.get());
}

void BroadcastFileWriter::flushCurrentPage() {
  VELOX_CHECK_NOT_NULL(currentPageSerializer_);

  IOBufOutputStream pageStream(*pool_);
  currentPageSerializer_->flush(&pageStream);

  auto serializedData = pageStream.getIOBuf();
  VELOX_CHECK_NOT_NULL(serializedData);

  const auto pageSize = serializedData->computeChainDataLength();
  VELOX_CHECK_GT(pageSize, 0);

  for (auto& buf : *serializedData) {
    writeFile_->append(std::string_view(
        reinterpret_cast<const char*>(buf.data()), buf.size()));
  }

  writeFile_->flush();
  totalFileSize_ += pageSize;

  // Track page size for footer
  pageSizes_.push_back(pageSize);

  // Clear the current page serializer to prepare for next page.
  currentPageSerializer_->clear();
}

void BroadcastFileWriter::write(const RowVectorPtr& rowVector) {
  if (rowVector->size() == 0) {
    return;
  }

  maybeInitializeSerializer();

  std::vector<IndexRange> ranges;
  ranges.emplace_back(0, rowVector->size());

  currentPageSerializer_->append(
      rowVector, folly::Range(ranges.data(), ranges.size()));

  // Check if we need to flush the current page
  auto serializedSize = currentPageSerializer_->maxSerializedSize();
  if (serializedSize >= maxPageSize_) {
    flushCurrentPage();
  }

  numRows_ += rowVector->size();
}

void BroadcastFileWriter::writeFooter() {
  VELOX_CHECK(!pageSizes_.empty());

  // Footer format:
  // [num_pages(4)][page_size_1(8)]...[page_size_N(8)][footer_size(8)]
  uint32_t numPages = static_cast<uint32_t>(pageSizes_.size());
  int64_t footerSize =
      sizeof(numPages) + (numPages * sizeof(int64_t)) + sizeof(int64_t);

  // Write number of pages
  writeFile_->append(std::string_view(
      reinterpret_cast<const char*>(&numPages), sizeof(numPages)));

  // Write all page sizes
  for (int64_t pageSize : pageSizes_) {
    writeFile_->append(std::string_view(
        reinterpret_cast<const char*>(&pageSize), sizeof(pageSize)));
  }

  // Write footer size (so we can read backwards from end)
  writeFile_->append(std::string_view(
      reinterpret_cast<const char*>(&footerSize), sizeof(footerSize)));

  totalFileSize_ += footerSize;
}

void BroadcastFileWriter::noMoreData() {
  if (numRows_ > 0) {
    flushCurrentPage();
    writeFooter();
  }
  writeFile_->close();
}

RowVectorPtr BroadcastFileWriter::fileStats() {
  if (numRows_ == 0) {
    return nullptr;
  }

  // Return stats for the single file with multiple pages
  auto fileNameVector =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_);
  auto maxSerializedSizeVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);
  auto numRowsVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);

  fileNameVector->set(0, StringView(writeFile_ ? writeFile_->getName() : ""));
  maxSerializedSizeVector->set(0, totalFileSize_);
  numRowsVector->set(0, numRows_);

  return std::make_shared<RowVector>(
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

BroadcastFileReader::BroadcastFileReader(
    std::unique_ptr<BroadcastFileInfo>& broadcastFileInfo,
    std::shared_ptr<velox::filesystems::FileSystem> fileSystem,
    velox::memory::MemoryPool* pool)
    : pool_(pool), broadcastFileInfo_(std::move(broadcastFileInfo)) {
  readFile_ = fileSystem->openFileForRead(broadcastFileInfo_->filePath_);
  readFooter();
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

  VELOX_CHECK_LE(
      currentFileOffset_ + pageSize,
      readFile_->size(),
      "Invalid page size {} at offset {}, would exceed file size {}",
      pageSize,
      currentFileOffset_,
      readFile_->size());

  auto pageBuffer = AlignedBuffer::allocate<char>(pageSize, pool_, 0);
  readFile_->pread(currentFileOffset_, pageSize, pageBuffer->asMutable<char>());
  currentFileOffset_ += pageSize;

  numBytes_ += pageSize;
  numPagesRead_++;

  return pageBuffer;
}

void BroadcastFileReader::readFooter() {
  VELOX_CHECK(
      pageSizes_.empty(),
      "readFooter() called when footer already read for broadcast file {}",
      broadcastFileInfo_->filePath_);

  uint64_t fileSize = readFile_->size();
  VELOX_CHECK_GT(fileSize, sizeof(int64_t));

  // Read footer size from the end of file
  int64_t footerSize;
  readFile_->pread(
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

  // Read the footer content
  uint64_t footerOffset = fileSize - footerSize;

  // Read number of pages
  uint32_t numPages;
  readFile_->pread(
      footerOffset, sizeof(numPages), reinterpret_cast<char*>(&numPages));

  // Validate number of pages and footer format
  VELOX_CHECK_GT(
      numPages,
      0,
      "Invalid number of pages {} in footer of broadcast file {}",
      numPages,
      broadcastFileInfo_->filePath_);

  int64_t expectedFooterSize =
      sizeof(numPages) + (numPages * sizeof(int64_t)) + sizeof(int64_t);
  VELOX_CHECK_EQ(
      expectedFooterSize,
      footerSize,
      "Footer format mismatch in broadcast file {}: expected size {} but got {}",
      broadcastFileInfo_->filePath_,
      expectedFooterSize,
      footerSize);

  // Read all page sizes
  pageSizes_.resize(numPages);
  uint64_t pageSizesOffset = footerOffset + sizeof(numPages);
  readFile_->pread(
      pageSizesOffset,
      numPages * sizeof(int64_t),
      reinterpret_cast<char*>(pageSizes_.data()));
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
