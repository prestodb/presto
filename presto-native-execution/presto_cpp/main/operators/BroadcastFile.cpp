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
#include <fmt/format.h>
#include <velox/common/encode/Base64.h>
#include <velox/common/file/FileSystems.h>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/thrift/gen-cpp2/presto_native_types.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/file/File.h"
#include "velox/common/time/Timer.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;
using namespace facebook::presto;

namespace facebook::presto::operators {

namespace {
// Read the footer to get all page sizes
void readFooter(
    velox::ReadFile* readFile,
    const std::string& filePath,
    std::vector<int64_t>& pageSizes) {
  VELOX_CHECK(
      pageSizes.empty(),
      "readFooter() called when footer already read for broadcast file {}",
      filePath);

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
      filePath);

  VELOX_CHECK_LT(
      footerSize,
      fileSize,
      "Footer size {} must be less than file size {} in broadcast file {}",
      footerSize,
      fileSize,
      filePath);

  // Read the serialized thrift footer
  uint64_t footerOffset = fileSize - footerSize - sizeof(footerSize);
  std::string serializedFooter(footerSize, '\0');
  readFile->pread(footerOffset, footerSize, serializedFooter.data());

  // Deserialize the thrift footer
  auto thriftFooter =
      std::make_shared<facebook::presto::thrift::BroadcastFileFooter>();
  thriftRead(serializedFooter, thriftFooter);

  // Extract page sizes from thrift footer
  pageSizes = thriftFooter->pageSizes_ref().value();

  // Validate the footer contents
  VELOX_CHECK_GT(
      pageSizes.size(),
      0,
      "Invalid number of pages {} in footer of broadcast file {}",
      pageSizes.size(),
      filePath);
}

// Empty when disabled, or when the file system cannot describe 'filePath'.
std::string serializeFileDescriptor(
    const std::string& filePath,
    bool descriptorEnabled) {
  if (!descriptorEnabled) {
    return {};
  }
  try {
    const std::optional<std::string> handle =
        velox::filesystems::getFileSystem(filePath, nullptr)
            ->serializeExtraFileInfo(filePath, {});
    if (!handle.has_value()) {
      return {};
    }
    // Unpadded: this rides an unescaped `?broadcastInfo=<json>` query value,
    // and folly's parser drops a parameter whose value contains a second '='.
    return velox::encoding::Base64::encodeUrl(handle.value());
  } catch (const std::exception& e) {
    // Readers open by path, so this costs the optimization, not the write.
    LOG_EVERY_N(WARNING, 1'000)
        << "Failed to serialize a descriptor for broadcast file " << filePath
        << ": " << e.what();
    return {};
  }
}

// Sets 'info.descriptor_' from 'root' when the writer supplied a usable one.
// Left empty for an older writer, a null/empty value, or a bad encoding.
void tryAddDescriptor(const nlohmann::json& root, BroadcastFileInfo& info) {
  const auto descriptor = root.find("descriptor");
  if (descriptor == root.end() || descriptor->is_null()) {
    return;
  }
  try {
    // get<std::string>() belongs inside the guard too: a descriptor of the
    // wrong JSON type throws type_error just as a bad encoding does.
    const std::string encodedDescriptor = descriptor->get<std::string>();
    if (!encodedDescriptor.empty()) {
      info.descriptor_ = velox::encoding::Base64::decodeUrl(encodedDescriptor);
    }
  } catch (const std::exception&) {
    // The exception text is dropped: it echoes the descriptor, which carries
    // bearer tokens.
    LOG_EVERY_N(WARNING, 1'000)
        << "Failed to decode the descriptor for broadcast file "
        << info.filePath_;
  }
}
} // namespace

#define PRESTO_BROADCAST_LIMIT_EXCEEDED(errorMessage)                        \
  _VELOX_THROW(                                                              \
      ::facebook::velox::VeloxRuntimeError,                                  \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(),          \
      ::facebook::presto::error_code::kExceededLocalBroadcastJoinMemoryLimit \
          .c_str(),                                                          \
      /* isRetriable */ false,                                               \
      "{}",                                                                  \
      errorMessage);

std::string redactBroadcastInfo(const std::string_view broadcastInfo) {
  // Callers also pass exception text that merely quotes a payload, so this is
  // not always JSON and an unparseable input carrying the key must fail safe.
  auto parsed = nlohmann::ordered_json::parse(
      broadcastInfo.begin(),
      broadcastInfo.end(),
      /*cb=*/nullptr,
      /*allow_exceptions=*/false);
  if (parsed.is_discarded() || !parsed.is_object()) {
    return broadcastInfo.find("descriptor") == std::string_view::npos
        ? std::string{broadcastInfo}
        : "<redacted>";
  }
  const auto descriptor = parsed.find("descriptor");
  // A descriptor that is absent or not a string carries no token to hide.
  if (descriptor == parsed.end() || !descriptor->is_string()) {
    return std::string{broadcastInfo};
  }
  *descriptor = "<redacted>";
  return parsed.dump();
}

// static
std::unique_ptr<BroadcastFileInfo> BroadcastFileInfo::deserialize(
    const std::string& info) {
  const auto root = nlohmann::json::parse(info);
  auto broadcastFileInfo = std::make_unique<BroadcastFileInfo>();
  root.at("filePath").get_to(broadcastFileInfo->filePath_);
  tryAddDescriptor(root, *broadcastFileInfo);
  return broadcastFileInfo;
}

BroadcastFileWriter::BroadcastFileWriter(
    const std::string& pathPrefix,
    uint64_t maxBroadcastBytes,
    uint64_t writeBufferSize,
    std::unique_ptr<VectorSerde::Options> serdeOptions,
    velox::memory::MemoryPool* pool,
    bool descriptorEnabled)
    : serializer::SerializedPageFileWriter(
          pathPrefix,
          std::numeric_limits<uint64_t>::max(),
          writeBufferSize,
          "",
          std::move(serdeOptions),
          getNamedVectorSerde("Presto"),
          pool),
      maxBroadcastBytes_(maxBroadcastBytes),
      descriptorEnabled_(descriptorEnabled) {}

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
  std::shared_ptr<FlatVector<StringView>> fileNameVector =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_);
  std::shared_ptr<FlatVector<int64_t>> maxSerializedSizeVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);
  std::shared_ptr<FlatVector<int64_t>> numRowsVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_);
  std::shared_ptr<FlatVector<StringView>> descriptorVector =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_);
  VELOX_CHECK_NOT_NULL(fileNameVector);
  VELOX_CHECK_NOT_NULL(maxSerializedSizeVector);
  VELOX_CHECK_NOT_NULL(numRowsVector);
  VELOX_CHECK_NOT_NULL(descriptorVector);

  const auto& filePath = fileInfos.back().path;
  const std::string encodedDescriptor =
      serializeFileDescriptor(filePath, descriptorEnabled_);
  fileNameVector->set(0, StringView(filePath));
  maxSerializedSizeVector->set(0, fileInfos.back().size);
  numRowsVector->set(0, numRows_);
  descriptorVector->set(0, StringView(encodedDescriptor));

  fileStats_ = std::make_shared<RowVector>(
      pool_,
      ROW(
          {{"filepath", VARCHAR()},
           {"maxserializedsize", BIGINT()},
           {"numrows", BIGINT()},
           {"descriptor", VARCHAR()}}),
      nullptr,
      1,
      std::vector<VectorPtr>(
          {std::move(fileNameVector),
           std::move(maxSerializedSizeVector),
           std::move(numRowsVector),
           std::move(descriptorVector)}));
}

RowVectorPtr BroadcastFileWriter::fileStats() {
  return fileStats_;
}

BroadcastFileReader::BroadcastFileReader(
    std::unique_ptr<BroadcastFileInfo>& broadcastFileInfo,
    std::shared_ptr<velox::filesystems::FileSystem> fileSystem,
    velox::memory::MemoryPool* pool)
    : pool_(pool),
      broadcastFileInfo_(std::move(broadcastFileInfo)),
      fileSystem_(std::move(fileSystem)) {
  // Both are const members, so checking once here covers every later use.
  VELOX_CHECK_NOT_NULL(broadcastFileInfo_);
  VELOX_CHECK_NOT_NULL(fileSystem_);
}

bool BroadcastFileReader::hasNext() {
  ensureFooterRead();
  return numPagesRead_ < pageSizes_.size();
}

velox::BufferPtr BroadcastFileReader::next() {
  ensureFooterRead();

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

  {
    velox::MicrosecondTimer timer(&fileReadWallTimeUs_);
    inputStream_->readBytes(
        reinterpret_cast<uint8_t*>(pageBuffer->asMutable<char>()), pageSize);
  }

  numBytes_ += pageSize;
  numPagesRead_++;

  return pageBuffer;
}

void BroadcastFileReader::close() {
  inputStream_.reset();
  closed_ = true;
}

void BroadcastFileReader::ensureFooterRead() {
  VELOX_CHECK(
      !closed_, "BroadcastFileReader is closed; cannot read after close()");
  if (inputStream_ != nullptr) {
    return;
  }

  std::unique_ptr<velox::ReadFile> readFile;
  {
    velox::MicrosecondTimer timer(&openFileAndReadFooterTimeUs_);
    if (!broadcastFileInfo_->descriptor_.empty()) {
      velox::filesystems::FileOptions options;
      options.extraFileInfo =
          std::make_shared<std::string>(broadcastFileInfo_->descriptor_);
      try {
        // Read the footer here too: a handle that opens but yields stale bytes
        // must fall back rather than fail the read.
        readFile = fileSystem_->openFileForRead(
            broadcastFileInfo_->filePath_, options);
        readFooter(readFile.get(), broadcastFileInfo_->filePath_, pageSizes_);
        ++descriptorOpenCount_;
      } catch (const std::exception&) {
        // The exception text is dropped: it may echo the descriptor, which
        // carries bearer tokens.
        readFile.reset();
        pageSizes_.clear();
        LOG_EVERY_N(WARNING, 1'000)
            << "Failed to read broadcast file from its handle, falling back to "
            << "a path open: " << broadcastFileInfo_->filePath_;
      }
    }
    if (readFile == nullptr) {
      readFile = fileSystem_->openFileForRead(broadcastFileInfo_->filePath_);
      readFooter(readFile.get(), broadcastFileInfo_->filePath_, pageSizes_);
      ++pathOpenCount_;
    }
  }

  // Create the input stream for sequential reads
  inputStream_ = std::make_unique<velox::common::FileInputStream>(
      std::move(readFile),
      SystemConfig::instance()->broadcastExchangeSourceReadBufferBytes(),
      pool_);
}

std::vector<int64_t> BroadcastFileReader::remainingPageSizes() {
  ensureFooterRead();

  if (pageSizes_.empty() || numPagesRead_ >= pageSizes_.size()) {
    return {}; // No remaining pages
  }

  // Return the portion of pageSizes_ that hasn't been read yet
  return std::vector<int64_t>(
      pageSizes_.begin() + numPagesRead_, pageSizes_.end());
}

folly::F14FastMap<std::string, int64_t> BroadcastFileReader::stats() const {
  return {
      {"broadcastExchangeSource.numBytes", numBytes_},
      {"broadcastExchangeSource.numPages", numPagesRead_},
      {"broadcastExchangeSource.openFileAndReadFooterTimeUs",
       openFileAndReadFooterTimeUs_},
      {"broadcastExchangeSource.fileReadWallTimeUs", fileReadWallTimeUs_},
      {"broadcastExchangeSource.descriptorOpenCount", descriptorOpenCount_},
      {"broadcastExchangeSource.pathOpenCount", pathOpenCount_}};
}

folly::F14FastMap<std::string, velox::RuntimeMetric>
BroadcastFileReader::metrics() const {
  return {
      {"broadcastExchangeSource.numBytes",
       velox::RuntimeMetric(numBytes_, velox::RuntimeCounter::Unit::kBytes)},
      {"broadcastExchangeSource.numPages", velox::RuntimeMetric(numPagesRead_)},
      {"broadcastExchangeSource.openFileAndReadFooterTimeNanos",
       velox::RuntimeMetric(
           openFileAndReadFooterTimeUs_ * 1'000,
           velox::RuntimeCounter::Unit::kNanos)},
      {"broadcastExchangeSource.fileReadWallTimeNanos",
       velox::RuntimeMetric(
           fileReadWallTimeUs_ * 1'000, velox::RuntimeCounter::Unit::kNanos)},
      {"broadcastExchangeSource.descriptorOpenCount",
       velox::RuntimeMetric(descriptorOpenCount_)},
      {"broadcastExchangeSource.pathOpenCount",
       velox::RuntimeMetric(pathOpenCount_)}};
}

} // namespace facebook::presto::operators
