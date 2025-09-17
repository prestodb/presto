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
} // namespace

/// Create FileBroadcast to write files under specified basePath.
BroadcastFactory::BroadcastFactory(const std::string& basePath)
    : basePath_(basePath) {
  VELOX_CHECK(!basePath.empty(), "Base path for broadcast files is empty!");
  fileSystem_ = velox::filesystems::getFileSystem(basePath, nullptr);
}

std::unique_ptr<BroadcastFileWriter> BroadcastFactory::createWriter(
    memory::MemoryPool* pool,
    const RowTypePtr& inputType) {
  fileSystem_->mkdir(basePath_);
  auto filename =
      fmt::format("{}/file_broadcast_{}.bin", basePath_, makeUuid());
  return std::make_unique<BroadcastFileWriter>(
      filename, inputType, fileSystem_, pool);
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
    std::string_view filename,
    const RowTypePtr& inputType,
    std::shared_ptr<velox::filesystems::FileSystem> fileSystem,
    velox::memory::MemoryPool* pool)
    : fileSystem_(std::move(fileSystem)),
      filename_(filename),
      numRows_(0),
      maxSerializedSize_(0),
      pool_(pool),
      serde_(std::make_unique<serializer::presto::PrestoVectorSerde>()),
      inputType_(inputType) {}

void BroadcastFileWriter::collect(const RowVectorPtr& input) {
  write(input);
}

void BroadcastFileWriter::noMoreData() {
  if (writeFile_ != nullptr) {
    writeFile_->flush();
    writeFile_->close();
  }
}

RowVectorPtr BroadcastFileWriter::fileStats() {
  // No rows written.
  if (numRows_ == 0) {
    return nullptr;
  }

  auto fileNameVector =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_);
  fileNameVector->set(0, StringView(filename_));
  auto maxSerializedSizeVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);
  maxSerializedSizeVector->set(0, maxSerializedSize_);
  auto numRowsVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);
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

void BroadcastFileWriter::initializeWriteFile() {
  if (!writeFile_) {
    LOG(INFO) << "Opening broadcast file for write: " << filename_;
    writeFile_ = fileSystem_->openFileForWrite(filename_);
  }
}

void BroadcastFileWriter::write(const RowVectorPtr& rowVector) {
  auto numRows = rowVector->size();
  if (numRows == 0) {
    return;
  }

  initializeWriteFile();
  numRows_ += numRows;
  const IndexRange allRows{0, numRows};

  auto arena = std::make_unique<StreamArena>(pool_);
  auto serializer =
      serde_->createIterativeSerializer(inputType_, numRows, arena.get());

  serializer->append(rowVector, folly::Range(&allRows, 1));
  maxSerializedSize_ += serializer->maxSerializedSize();
  IOBufOutputStream out(*pool_);
  serializer->flush(&out);
  auto iobuf = out.getIOBuf();
  for (auto& range : *iobuf) {
    writeFile_->append(std::string_view(
        reinterpret_cast<const char*>(range.data()), range.size()));
  }
  writeFile_->flush();
}

BroadcastFileReader::BroadcastFileReader(
    std::unique_ptr<BroadcastFileInfo>& broadcastFileInfo,
    std::shared_ptr<velox::filesystems::FileSystem> fileSystem,
    velox::memory::MemoryPool* pool)
    : broadcastFileInfo_(std::move(broadcastFileInfo)),
      fileSystem_(fileSystem),
      hasData_(true),
      numBytes_(0),
      pool_(pool) {}

bool BroadcastFileReader::hasNext() {
  return hasData_;
}

velox::BufferPtr BroadcastFileReader::next() {
  if (!hasNext()) {
    return nullptr;
  }

  auto readFile = fileSystem_->openFileForRead(broadcastFileInfo_->filePath_);
  auto buffer = AlignedBuffer::allocate<char>(readFile->size(), pool_, 0);
  readFile->pread(0, readFile->size(), buffer->asMutable<char>());
  numBytes_ += readFile->size();
  hasData_ = false;
  return buffer;
}

folly::F14FastMap<std::string, int64_t> BroadcastFileReader::stats() {
  return {{"broadcastExchangeSource.numBytes", numBytes_}};
}

} // namespace facebook::presto::operators
