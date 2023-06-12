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
#include "presto_cpp/external/json/json.hpp"
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
  LOG(INFO) << "Opening broadcast file for write: " << filename;
  auto writeFile = fileSystem_->openFileForWrite(filename);
  return std::make_unique<BroadcastFileWriter>(
      std::move(writeFile), filename, pool, inputType);
}

std::shared_ptr<BroadcastFileReader> BroadcastFactory::createReader(
    const std::vector<BroadcastFileInfo> fileInfos,
    velox::memory::MemoryPool* pool) {
  auto broadcastFileReader = std::make_shared<BroadcastFileReader>(
      std::move(fileInfos), fileSystem_, pool);
  return broadcastFileReader;
}

// static
std::unique_ptr<BroadcastInfo> BroadcastInfo::deserialize(
    const std::string& info) {
  const auto root = nlohmann::json::parse(info);
  std::vector<BroadcastFileInfo> broadcastFileInfos;

  for (auto& fileInfo : root["fileInfos"]) {
    BroadcastFileInfo broadcastFileInfo;
    fileInfo.at("filePath").get_to(broadcastFileInfo.filePath_);
    broadcastFileInfos.emplace_back(broadcastFileInfo);
  }
  return std::make_unique<BroadcastInfo>(root["basePath"], broadcastFileInfos);
}

BroadcastInfo::BroadcastInfo(
    std::string basePath,
    std::vector<BroadcastFileInfo> fileInfos)
    : basePath_(basePath), fileInfos_(fileInfos) {}

BroadcastFileWriter::BroadcastFileWriter(
    std::unique_ptr<WriteFile> writeFile,
    std::string_view filename,
    velox::memory::MemoryPool* pool,
    const RowTypePtr& inputType)
    : writeFile_(std::move(writeFile)),
      filename_(filename),
      pool_(pool),
      serde_(std::make_unique<serializer::presto::PrestoVectorSerde>()),
      inputType_(inputType) {}

void BroadcastFileWriter::collect(const RowVectorPtr& input) {
  serialize(input);
}

void BroadcastFileWriter::noMoreData() {}

// TODO: Add file stats - size, checksum, number of rows.
RowVectorPtr BroadcastFileWriter::fileStats() {
  auto data = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_);
  data->set(0, StringView(filename_));
  return std::make_shared<RowVector>(
      pool_,
      ROW({"filepath"}, {VARCHAR()}),
      nullptr,
      1,
      std::vector<VectorPtr>({std::move(data)}));
}

void BroadcastFileWriter::serialize(const RowVectorPtr& rowVector) {
  auto numRows = rowVector->size();
  const IndexRange allRows{0, numRows};

  auto arena = std::make_unique<StreamArena>(pool_);
  auto serializer = serde_->createSerializer(inputType_, numRows, arena.get());

  serializer->append(rowVector, folly::Range(&allRows, 1));
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
    std::vector<BroadcastFileInfo> broadcastFileInfos,
    std::shared_ptr<velox::filesystems::FileSystem> fileSystem,
    velox::memory::MemoryPool* pool)
    : broadcastFileInfos_(broadcastFileInfos),
      fileSystem_(fileSystem),
      readfileIndex_(0),
      numFiles_(0),
      numBytes_(0),
      pool_(pool) {}

bool BroadcastFileReader::hasNext() {
  return readfileIndex_ < broadcastFileInfos_.size();
}

velox::BufferPtr BroadcastFileReader::next() {
  if (!hasNext()) {
    return nullptr;
  }

  auto readFile = fileSystem_->openFileForRead(
      broadcastFileInfos_[readfileIndex_].filePath_);
  auto buffer = AlignedBuffer::allocate<char>(readFile->size(), pool_, 0);
  readFile->pread(0, readFile->size(), buffer->asMutable<char>());
  ++readfileIndex_;
  ++numFiles_;
  numBytes_ += readFile->size();
  return buffer;
}

folly::F14FastMap<std::string, int64_t> BroadcastFileReader::stats() {
  return {
      {"broadcastExchangeSource.numFiles", numFiles_},
      {"broadcastExchangeSource.numBytes", numBytes_}};
}

} // namespace facebook::presto::operators
