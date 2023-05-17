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
#include "presto_cpp/main/operators/FileBroadcast.h"
#include "velox/serializers/PrestoSerializer.h"
using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

FileBroadcast::FileBroadcast(const std::string& basePath)
    : basePath_(basePath) {
  VELOX_CHECK(!basePath.empty(), "Root path for broadcast files is empty!");
  fileSystem_ = velox::filesystems::getFileSystem(basePath, nullptr);
}

std::shared_ptr<BroadcastFileReader> FileBroadcast::createReader(
    const std::string& broadcastFilePath,
    velox::memory::MemoryPool* pool) {
  LOG(INFO) << "Opening broadcast file for read: " << broadcastFilePath;
  auto readFile = fileSystem_->openFileForRead(broadcastFilePath);
  auto broadcastFileReader = std::make_shared<BroadcastFileReader>(
      std::move(readFile), broadcastFilePath, pool);
  return broadcastFileReader;
}

std::shared_ptr<BroadcastFileWriter> FileBroadcast::createWriter(
    memory::MemoryPool* pool,
    const RowTypePtr& inputType) {
  // auto filename = getFileName(basePath_, taskId, StageId);
  fileSystem_->mkdir(basePath_);
  auto filename = fmt::format("{}/file_broadcast.bin", basePath_);
  LOG(INFO) << "Opening broadcast file for write: " << filename;
  auto writeFile = fileSystem_->openFileForWrite(filename);
  auto broadcastFileWriter = std::make_shared<BroadcastFileWriter>(
      std::move(writeFile), filename, pool, inputType);
  return broadcastFileWriter;
}

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

void BroadcastFileWriter::collect(RowVectorPtr input) {
  serialize(input);
}
void BroadcastFileWriter::noMoreData() {
  writeFile_->flush();
}

RowVectorPtr BroadcastFileWriter::fileStats() {
  auto data = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_);
  data->set(0, StringView(filename_));
  return std::make_shared<RowVector>(
      pool_,
      ROW({"filepath"}, {VARCHAR()}),
      BufferPtr(nullptr),
      1,
      std::vector<VectorPtr>({std::move(data)}));
}

void BroadcastFileWriter::serialize(
    const RowVectorPtr& rowVector,
    const VectorSerde::Options* serdeOptions) {
  auto numRows = rowVector->size();
  std::vector<IndexRange> rows(numRows);
  for (int i = 0; i < numRows; i++) {
    rows[i] = IndexRange{i, 1};
  }

  auto arena = std::make_unique<StreamArena>(pool_);
  auto serializer =
      serde_->createSerializer(inputType_, numRows, arena.get(), serdeOptions);

  serializer->append(rowVector, folly::Range(rows.data(), numRows));
  IOBufOutputStream out(
      *pool_, nullptr, std::max<int64_t>(64 * 1024, rowVector->size()));
  serializer->flush(&out);
  auto iobuf = out.getIOBuf();
  for (auto& range : *iobuf) {
    writeFile_->append(std::string_view(
        reinterpret_cast<const char*>(range.data()), range.size()));
  }
  writeFile_->flush();
}

BroadcastFileReader::BroadcastFileReader(
    std::unique_ptr<ReadFile> readFile,
    std::string_view filename,
    velox::memory::MemoryPool* pool)
    : readFile_(std::move(readFile)), filename_(filename), pool_(pool) {}

bool BroadcastFileReader::hasNext() {
  return readFile_->bytesRead() != readFile_->size();
}

velox::BufferPtr BroadcastFileReader::next() {
  auto buffer = AlignedBuffer::allocate<char>(readFile_->size(), pool_, 0);
  readFile_->pread(0, readFile_->size(), buffer->asMutable<char>());
  return buffer;
}

} // namespace facebook::presto::operators
