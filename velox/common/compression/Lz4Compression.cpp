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

#include "velox/common/compression/Lz4Compression.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::common {
namespace {

constexpr int32_t kLz4DefaultCompressionLevel = 1;
constexpr int32_t kLz4MinCompressionLevel = 1;

#if (defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER < 10800)
constexpr int32_t kLegacyLz4MaxCompressionLevel = 12;
#endif

Status lz4Error(const char* prefixMessage, LZ4F_errorCode_t errorCode) {
  return Status::IOError(prefixMessage, LZ4F_getErrorName(errorCode));
}

LZ4F_preferences_t defaultPreferences() {
  LZ4F_preferences_t prefs;
  memset(&prefs, 0, sizeof(prefs));
  return prefs;
}

LZ4F_preferences_t defaultPreferences(int32_t compressionLevel) {
  LZ4F_preferences_t prefs = defaultPreferences();
  prefs.compressionLevel = compressionLevel;
  return prefs;
}
} // namespace

class Lz4CodecBase : public Codec {
 public:
  explicit Lz4CodecBase(int32_t compressionLevel);

  int32_t minCompressionLevel() const override;

  int32_t maxCompressionLevel() const override;

  int32_t defaultCompressionLevel() const override;

  int32_t compressionLevel() const override;

  CompressionKind compressionKind() const override;

 protected:
  const int32_t compressionLevel_;
};

class Lz4FrameCodec : public Lz4CodecBase {
 public:
  explicit Lz4FrameCodec(int32_t compressionLevel);

  uint64_t maxCompressedLength(uint64_t inputLength) override;

  Expected<uint64_t> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Expected<uint64_t> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  bool supportsStreamingCompression() const override;

  Expected<std::shared_ptr<StreamingCompressor>> makeStreamingCompressor()
      override;

  Expected<std::shared_ptr<StreamingDecompressor>> makeStreamingDecompressor()
      override;

  std::string_view name() const override {
    return "lz4";
  }

 protected:
  const LZ4F_preferences_t prefs_;
};

class Lz4RawCodec : public Lz4CodecBase {
 public:
  explicit Lz4RawCodec(int32_t compressionLevel);

  uint64_t maxCompressedLength(uint64_t inputLength) override;

  Expected<uint64_t> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Expected<uint64_t> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  std::string_view name() const override;
};

/// The Hadoop Lz4Codec source code can be found here:
/// https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
class Lz4HadoopCodec : public Lz4RawCodec, public HadoopCompressionFormat {
 public:
  Lz4HadoopCodec();

  uint64_t maxCompressedLength(uint64_t inputLength) override;

  Expected<uint64_t> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Expected<uint64_t> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  int32_t minCompressionLevel() const override;

  int32_t maxCompressionLevel() const override;

  int32_t defaultCompressionLevel() const override;

  std::string_view name() const override;

 private:
  Expected<uint64_t> decompressInternal(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;
};

class LZ4Compressor : public StreamingCompressor {
 public:
  explicit LZ4Compressor(int32_t compressionLevel);

  ~LZ4Compressor() override;

  Status init();

  Expected<CompressResult> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Expected<FlushResult> flush(uint8_t* output, uint64_t outputLength) override;

  Expected<EndResult> finalize(uint8_t* output, uint64_t outputLength) override;

 protected:
  Status
  compressBegin(uint8_t* output, size_t& outputLen, uint64_t& bytesWritten);

  int32_t compressionLevel_;
  LZ4F_compressionContext_t ctx_{nullptr};
  LZ4F_preferences_t prefs_;
  bool firstTime_;
};

class LZ4Decompressor : public StreamingDecompressor {
 public:
  LZ4Decompressor() {}

  ~LZ4Decompressor() override {
    if (ctx_ != nullptr) {
      LZ4F_freeDecompressionContext(ctx_);
    }
  }

  Expected<DecompressResult> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  bool isFinished() override;

  Status reset() override;

  Status init();

 protected:
  LZ4F_decompressionContext_t ctx_{nullptr};
  bool finished_{false};
};

LZ4Compressor::LZ4Compressor(int32_t compressionLevel)
    : compressionLevel_(compressionLevel) {}

LZ4Compressor::~LZ4Compressor() {
  if (ctx_ != nullptr) {
    LZ4F_freeCompressionContext(ctx_);
  }
}

Status LZ4Compressor::init() {
  LZ4F_errorCode_t ret;
  prefs_ = defaultPreferences(compressionLevel_);
  firstTime_ = true;

  ret = LZ4F_createCompressionContext(&ctx_, LZ4F_VERSION);
  VELOX_RETURN_IF(LZ4F_isError(ret), lz4Error("LZ4 init failed: {}", ret));
  return Status::OK();
}

Expected<StreamingCompressor::CompressResult> LZ4Compressor::compress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  auto inputSize = static_cast<size_t>(inputLength);
  auto outputSize = static_cast<size_t>(outputLength);
  uint64_t bytesWritten = 0;

  if (firstTime_) {
    // Output too small to write LZ4F header.
    if (outputLength < LZ4F_HEADER_SIZE_MAX) {
      return CompressResult{0, 0, true};
    }
    VELOX_RETURN_UNEXPECTED_NOT_OK(
        compressBegin(output, outputSize, bytesWritten));
  }

  if (outputSize < LZ4F_compressBound(inputSize, &prefs_)) {
    // Output too small to compress into.
    return CompressResult{0, bytesWritten, true};
  }
  auto numBytesOrError = LZ4F_compressUpdate(
      ctx_, output, outputSize, input, inputSize, nullptr /* options */);
  VELOX_RETURN_UNEXPECTED_IF(
      LZ4F_isError(numBytesOrError),
      lz4Error("LZ4 compress updated failed: {}", numBytesOrError));
  bytesWritten += static_cast<int64_t>(numBytesOrError);

  VELOX_DCHECK_LE(bytesWritten, outputSize);
  return CompressResult{inputLength, bytesWritten, false};
}

Expected<StreamingCompressor::FlushResult> LZ4Compressor::flush(
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(output);
  auto outputSize = static_cast<size_t>(outputLength);
  uint64_t bytesWritten = 0;

  if (firstTime_) {
    // Output too small to write LZ4F header.
    if (outputLength < LZ4F_HEADER_SIZE_MAX) {
      return FlushResult{0, true};
    }
    VELOX_RETURN_UNEXPECTED_NOT_OK(
        compressBegin(output, outputSize, bytesWritten));
  }

  if (outputSize < LZ4F_compressBound(0, &prefs_)) {
    // Output too small to flush into.
    return FlushResult{bytesWritten, true};
  }

  auto numBytesOrError =
      LZ4F_flush(ctx_, output, outputSize, nullptr /* options */);
  VELOX_RETURN_UNEXPECTED_IF(
      LZ4F_isError(numBytesOrError),
      lz4Error("LZ4 flush failed: {}", numBytesOrError));
  bytesWritten += static_cast<uint64_t>(numBytesOrError);

  VELOX_DCHECK_LE(bytesWritten, outputLength);
  return FlushResult{bytesWritten, false};
}

Expected<StreamingCompressor::EndResult> LZ4Compressor::finalize(
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(output);
  auto outputSize = static_cast<size_t>(outputLength);
  uint64_t bytesWritten = 0;

  if (firstTime_) {
    // Output too small to write LZ4F header.
    if (outputLength < LZ4F_HEADER_SIZE_MAX) {
      return EndResult{0, true};
    }
    VELOX_RETURN_UNEXPECTED_NOT_OK(
        compressBegin(output, outputSize, bytesWritten));
  }

  if (outputSize < LZ4F_compressBound(0, &prefs_)) {
    // Output too small to end frame into.
    return EndResult{bytesWritten, true};
  }

  auto numBytesOrError =
      LZ4F_compressEnd(ctx_, output, outputSize, nullptr /* options */);
  VELOX_RETURN_UNEXPECTED_IF(
      LZ4F_isError(numBytesOrError),
      lz4Error("LZ4 finalize failed: {}", numBytesOrError));
  bytesWritten += static_cast<uint64_t>(numBytesOrError);

  VELOX_DCHECK_LE(bytesWritten, outputLength);
  return EndResult{bytesWritten, false};
}

Status LZ4Compressor::compressBegin(
    uint8_t* output,
    size_t& outputLen,
    uint64_t& bytesWritten) {
  auto numBytesOrError = LZ4F_compressBegin(ctx_, output, outputLen, &prefs_);
  VELOX_RETURN_IF(
      LZ4F_isError(numBytesOrError),
      lz4Error("LZ4 compress begin failed: {}", numBytesOrError));
  firstTime_ = false;
  output += numBytesOrError;
  outputLen -= numBytesOrError;
  bytesWritten += static_cast<uint64_t>(numBytesOrError);
  return Status::OK();
}

Status LZ4Decompressor::init() {
  finished_ = false;
  auto ret = LZ4F_createDecompressionContext(&ctx_, LZ4F_VERSION);
  VELOX_RETURN_IF(LZ4F_isError(ret), lz4Error("LZ4 init failed: {}", ret));
  return Status::OK();
}

Status LZ4Decompressor::reset() {
#if defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER >= 10800
  // LZ4F_resetDecompressionContext appeared in 1.8.0
  if (ctx_ == nullptr) {
    return Status::Invalid("LZ4 decompression context is null.");
  }
  LZ4F_resetDecompressionContext(ctx_);
  finished_ = false;
  return Status::OK();
#else
  if (ctx_ != nullptr) {
    LZ4F_freeDecompressionContext(ctx_);
  }
  return init();
#endif
}

Expected<StreamingDecompressor::DecompressResult> LZ4Decompressor::decompress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  auto inputSize = static_cast<size_t>(inputLength);
  auto outputSize = static_cast<size_t>(outputLength);

  auto ret = LZ4F_decompress(
      ctx_, output, &outputSize, input, &inputSize, nullptr /* options */);
  VELOX_RETURN_UNEXPECTED_IF(
      LZ4F_isError(ret), lz4Error("LZ4 decompression failed: {}", ret));
  finished_ = (ret == 0);
  return DecompressResult{
      static_cast<uint64_t>(inputSize),
      static_cast<uint64_t>(outputSize),
      (inputSize == 0 && outputSize == 0)};
}

bool LZ4Decompressor::isFinished() {
  return finished_;
}

Lz4CodecBase::Lz4CodecBase(int32_t compressionLevel)
    : compressionLevel_(
          compressionLevel == kDefaultCompressionLevel
              ? kLz4DefaultCompressionLevel
              : compressionLevel) {}

int32_t Lz4CodecBase::minCompressionLevel() const {
  return kLz4MinCompressionLevel;
}

int32_t Lz4CodecBase::maxCompressionLevel() const {
#if (defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER < 10800)
  return kLegacyLz4MaxCompressionLevel;
#else
  return LZ4F_compressionLevel_max();
#endif
}

int32_t Lz4CodecBase::defaultCompressionLevel() const {
  return kLz4DefaultCompressionLevel;
}

int32_t Lz4CodecBase::compressionLevel() const {
  return compressionLevel_;
}

CompressionKind Lz4CodecBase::compressionKind() const {
  return CompressionKind_LZ4;
}

Lz4FrameCodec::Lz4FrameCodec(int32_t compressionLevel)
    : Lz4CodecBase(compressionLevel),
      prefs_(defaultPreferences(compressionLevel_)) {}

uint64_t Lz4FrameCodec::maxCompressedLength(uint64_t inputLen) {
  return static_cast<int64_t>(
      LZ4F_compressFrameBound(static_cast<size_t>(inputLen), &prefs_));
}

Expected<uint64_t> Lz4FrameCodec::compress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  auto ret = LZ4F_compressFrame(
      output,
      static_cast<size_t>(outputLength),
      input,
      static_cast<size_t>(inputLength),
      &prefs_);
  VELOX_RETURN_UNEXPECTED_IF(
      LZ4F_isError(ret), lz4Error("LZ4 compression failed: {}", ret));
  return static_cast<uint64_t>(ret);
}

Expected<uint64_t> Lz4FrameCodec::decompress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  return makeStreamingDecompressor().then(
      [&](const auto& decompressor) -> Expected<uint64_t> {
        uint64_t bytesWritten = 0;
        while (!decompressor->isFinished() && inputLength != 0) {
          auto maybeResult = decompressor->decompress(
              input, inputLength, output, outputLength);
          VELOX_RETURN_UNEXPECTED(maybeResult);

          const auto& result = maybeResult.value();
          input += result.bytesRead;
          inputLength -= result.bytesRead;
          output += result.bytesWritten;
          outputLength -= result.bytesWritten;
          bytesWritten += result.bytesWritten;
          VELOX_RETURN_UNEXPECTED_IF(
              result.outputTooSmall,
              Status::IOError("LZ4 decompression buffer too small."));
        }
        VELOX_RETURN_UNEXPECTED_IF(
            !decompressor->isFinished() || inputLength != 0,
            Status::IOError("LZ4 decompression failed."));
        return bytesWritten;
      });
}

bool Lz4FrameCodec::supportsStreamingCompression() const {
  return true;
}

Expected<std::shared_ptr<StreamingCompressor>>
Lz4FrameCodec::makeStreamingCompressor() {
  auto ptr = std::make_shared<LZ4Compressor>(compressionLevel_);
  VELOX_RETURN_UNEXPECTED_NOT_OK(ptr->init());
  return ptr;
}

Expected<std::shared_ptr<StreamingDecompressor>>
Lz4FrameCodec::makeStreamingDecompressor() {
  auto ptr = std::make_shared<LZ4Decompressor>();
  VELOX_RETURN_UNEXPECTED_NOT_OK(ptr->init());
  return ptr;
}

Lz4RawCodec::Lz4RawCodec(int32_t compressionLevel)
    : Lz4CodecBase(compressionLevel) {}

uint64_t Lz4RawCodec::maxCompressedLength(uint64_t inputLength) {
  return static_cast<uint64_t>(
      LZ4_compressBound(static_cast<int>(inputLength)));
}

Expected<uint64_t> Lz4RawCodec::compress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  uint64_t compressedSize;
#ifdef LZ4HC_CLEVEL_MIN
  constexpr int32_t kMinHcClevel = LZ4HC_CLEVEL_MIN;
#else // For older versions of the lz4 library.
  constexpr int32_t kMinHcClevel = 3;
#endif
  if (compressionLevel_ < kMinHcClevel) {
    compressedSize = LZ4_compress_default(
        reinterpret_cast<const char*>(input),
        reinterpret_cast<char*>(output),
        static_cast<int>(inputLength),
        static_cast<int>(outputLength));
  } else {
    compressedSize = LZ4_compress_HC(
        reinterpret_cast<const char*>(input),
        reinterpret_cast<char*>(output),
        static_cast<int>(inputLength),
        static_cast<int>(outputLength),
        compressionLevel_);
  }
  VELOX_RETURN_UNEXPECTED_IF(
      compressedSize == 0, Status::IOError("LZ4 compression failed."));
  return static_cast<uint64_t>(compressedSize);
}

Expected<uint64_t> Lz4RawCodec::decompress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  auto decompressedSize = LZ4_decompress_safe(
      reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output),
      static_cast<int>(inputLength),
      static_cast<int>(outputLength));
  VELOX_RETURN_UNEXPECTED_IF(
      decompressedSize < 0, Status::IOError("LZ4 decompression failed."));
  return static_cast<uint64_t>(decompressedSize);
}

std::string_view Lz4RawCodec::name() const {
  return "lz4_raw";
}

Lz4HadoopCodec::Lz4HadoopCodec() : Lz4RawCodec(kLz4DefaultCompressionLevel) {}

uint64_t Lz4HadoopCodec::maxCompressedLength(uint64_t inputLength) {
  return kPrefixLength + Lz4RawCodec::maxCompressedLength(inputLength);
}

Expected<uint64_t> Lz4HadoopCodec::compress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  VELOX_RETURN_UNEXPECTED_IF(
      outputLength < kPrefixLength,
      Status::IOError(
          "Output buffer too small for Lz4HadoopCodec compression."));

  return Lz4RawCodec::compress(
             input,
             inputLength,
             output + kPrefixLength,
             outputLength - kPrefixLength)
      .then([&](const auto& compressedSize) {
        // Prepend decompressed size in bytes and compressed size in bytes
        // to be compatible with Hadoop Lz4RawCodec.
        const uint32_t decompressedLength =
            folly::Endian::big(static_cast<uint32_t>(inputLength));
        const uint32_t compressedLength =
            folly::Endian::big(static_cast<uint32_t>(compressedSize));
        folly::storeUnaligned(output, decompressedLength);
        folly::storeUnaligned(output + sizeof(uint32_t), compressedLength);
        return kPrefixLength + compressedSize;
      });
}

Expected<uint64_t> Lz4HadoopCodec::decompress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_NOT_NULL(output);
  uint64_t decompressedSize;
  if (tryDecompressHadoop(
          input, inputLength, output, outputLength, decompressedSize)) {
    return decompressedSize;
  }
  // Fall back on raw LZ4 codec (for files produces by earlier versions of
  // Parquet C++).
  return Lz4RawCodec::decompress(input, inputLength, output, outputLength);
}

int32_t Lz4HadoopCodec::minCompressionLevel() const {
  return kDefaultCompressionLevel;
}

int32_t Lz4HadoopCodec::maxCompressionLevel() const {
  return kDefaultCompressionLevel;
}

int32_t Lz4HadoopCodec::defaultCompressionLevel() const {
  return kDefaultCompressionLevel;
}

std::string_view Lz4HadoopCodec::name() const {
  return "lz4_hadoop";
}

Expected<uint64_t> Lz4HadoopCodec::decompressInternal(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  return Lz4RawCodec::decompress(input, inputLength, output, outputLength);
}

std::unique_ptr<Codec> makeLz4FrameCodec(int32_t compressionLevel) {
  return std::make_unique<Lz4FrameCodec>(compressionLevel);
}

std::unique_ptr<Codec> makeLz4RawCodec(int32_t compressionLevel) {
  return std::make_unique<Lz4RawCodec>(compressionLevel);
}

std::unique_ptr<Codec> makeLz4HadoopCodec() {
  return std::make_unique<Lz4HadoopCodec>();
}
} // namespace facebook::velox::common
