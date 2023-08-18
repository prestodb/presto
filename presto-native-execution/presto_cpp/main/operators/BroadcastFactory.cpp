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

} // namespace facebook::presto::operators
