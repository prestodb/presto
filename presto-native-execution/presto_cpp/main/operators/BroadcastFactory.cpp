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
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

namespace {
std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

// Helper struct to hold pool and size information for IOBuf destructor
struct PoolFreeInfo {
  std::shared_ptr<velox::memory::MemoryPool>
      pool; // Safe reference-counted pointer
  size_t size;
};

// Static function for IOBuf destructor that uses memory pool
void freePoolMemory(void* buf, void* userData) {
  auto* freeInfo = static_cast<PoolFreeInfo*>(userData);
  freeInfo->pool->free(buf, freeInfo->size);
  delete freeInfo;
}
} // namespace

BroadcastFactory::BroadcastFactory(const std::string& basePath)
    : basePath_(basePath) {
  VELOX_CHECK(!basePath.empty(), "Base path for broadcast files is empty!");
  fileSystem_ = velox::filesystems::getFileSystem(basePath, nullptr);
}

std::unique_ptr<BroadcastFileWriter> BroadcastFactory::createWriter(
    uint64_t writeBufferSize,
    std::unique_ptr<VectorSerde::Options> serdeOptions,
    velox::memory::MemoryPool* pool) {
  fileSystem_->mkdir(basePath_);
  return std::make_unique<BroadcastFileWriter>(
      fmt::format("{}/file_broadcast_{}", basePath_, makeUuid()),
      writeBufferSize,
      std::move(serdeOptions),
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
    const std::string& pathPrefix,
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
          pool) {}

void BroadcastFileWriter::write(const RowVectorPtr& rowVector) {
  const auto numRows = rowVector->size();
  IndexRange range{0, numRows};
  folly::Range<IndexRange*> ranges{&range, 1};
  serializer::SerializedPageFileWriter::write(rowVector, ranges);
  numRows_ += numRows;
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

  // Footer format:
  // [num_pages(4)][page_size_1(8)]...[page_size_N(8)][footer_size(8)]
  uint32_t numPages = static_cast<uint32_t>(pageSizes_.size());
  const auto footerSize =
      sizeof(numPages) + (numPages * sizeof(int64_t)) + sizeof(int64_t);

  void* buffer = pool_->allocate(footerSize);
  auto* freeInfo = new PoolFreeInfo{
      pool_->shared_from_this(), static_cast<size_t>(footerSize)};
  auto footerBuf =
      folly::IOBuf::takeOwnership(buffer, footerSize, freePoolMemory, freeInfo);

  char* data = reinterpret_cast<char*>(footerBuf->writableData());

  // Write number of pages
  std::memcpy(data, &numPages, sizeof(numPages));
  data += sizeof(numPages);

  // Write all page sizes
  for (const auto& pageSize : pageSizes_) {
    std::memcpy(data, &pageSize, sizeof(pageSize));
    data += sizeof(pageSize);
  }

  // Write footer size (so we can read backwards from end)
  std::memcpy(data, &footerSize, sizeof(footerSize));

  currentFile_->write(std::move(footerBuf));
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

  uint64_t fileSize = readFile->size();
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

  // Read the footer content
  uint64_t footerOffset = fileSize - footerSize;

  // Read number of pages
  uint32_t numPages;
  readFile->pread(
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
  readFile->pread(
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
