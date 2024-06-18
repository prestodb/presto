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

#include "velox/dwio/common/compression/Compression.h"
#include "velox/common/compression/LzoDecompressor.h"
#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/common/compression/PagedInputStream.h"

#include <folly/logging/xlog.h>
#include <lz4.h>
#include <snappy.h>
#include <zlib.h>
#include <zstd.h>
#include <zstd_errors.h>

namespace facebook::velox::dwio::common::compression {

using dwio::common::encryption::Decrypter;
using dwio::common::encryption::Encrypter;
using facebook::velox::common::CompressionKind;
using memory::MemoryPool;

namespace {

class ZstdCompressor : public Compressor {
 public:
  explicit ZstdCompressor(int32_t level) : Compressor{level} {}

  uint64_t compress(const void* src, void* dest, uint64_t length) override;
};

uint64_t
ZstdCompressor::compress(const void* src, void* dest, uint64_t length) {
  auto ret = ZSTD_compress(dest, length, src, length, level_);
  if (ZSTD_isError(ret)) {
    // it's fine to hit dest size too small
    if (ZSTD_getErrorCode(ret) == ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall) {
      return length;
    }
    DWIO_RAISE("ZSTD returned an error: ", ZSTD_getErrorName(ret));
  }
  return ret;
}

class ZlibCompressor : public Compressor {
 public:
  explicit ZlibCompressor(int32_t level);

  ~ZlibCompressor() override;

  uint64_t compress(const void* src, void* dest, uint64_t length) override;

 private:
  bool isCompressCalled_;
  z_stream stream_;
};

ZlibCompressor::ZlibCompressor(int32_t level)
    : Compressor{level}, isCompressCalled_{false} {
  stream_.zalloc = Z_NULL;
  stream_.zfree = Z_NULL;
  stream_.opaque = Z_NULL;
  DWIO_ENSURE_EQ(
      deflateInit2(&stream_, level_, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY),
      Z_OK,
      "Error while calling deflateInit2() for zlib.");
}

ZlibCompressor::~ZlibCompressor() {
  auto ret = deflateEnd(&stream_);
  if (isCompressCalled_ && ret != Z_OK) {
    LOG(WARNING) << "Error in ~ZlibCompressor() " << ret;
  }
}

uint64_t
ZlibCompressor::compress(const void* src, void* dest, uint64_t length) {
  isCompressCalled_ = true;
  DWIO_ENSURE_EQ(deflateReset(&stream_), Z_OK, "Failed to reset deflate.");

  stream_.avail_in = static_cast<uint32_t>(length);
  stream_.next_in = reinterpret_cast<unsigned char*>(const_cast<void*>(src));
  stream_.next_out = reinterpret_cast<unsigned char*>(dest);
  stream_.avail_out = static_cast<uint32_t>(length);

  auto ret = deflate(&stream_, Z_FINISH);
  if (ret == Z_STREAM_END) {
    // all input is consumed with enough output buffer
  } else if (ret == Z_OK || ret == Z_BUF_ERROR) {
    // needs more output buffer
  } else {
    DWIO_RAISE("Failed to deflate input data. error: ", ret);
  }

  return stream_.total_out;
}

class ZlibDecompressor : public Decompressor {
 public:
  explicit ZlibDecompressor(
      uint64_t blockSize,
      int windowBits,
      const std::string& streamDebugInfo,
      bool izGzip = false);
  ~ZlibDecompressor() override;

  uint64_t decompress(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) override;

 protected:
  void reset() {
    auto result = inflateReset(&zstream_);
    DWIO_ENSURE_EQ(
        result,
        Z_OK,
        "Bad inflateReset in ZlibDecompressor::reset. error: ",
        result);
  }

  z_stream zstream_;
};

ZlibDecompressor::ZlibDecompressor(
    uint64_t blockSize,
    int windowBits,
    const std::string& streamDebugInfo,
    bool isGzip)
    : Decompressor{blockSize, streamDebugInfo} {
  zstream_.next_in = Z_NULL;
  zstream_.avail_in = 0;
  zstream_.zalloc = Z_NULL;
  zstream_.zfree = Z_NULL;
  zstream_.opaque = Z_NULL;
  zstream_.next_out = Z_NULL;
  zstream_.avail_out = folly::to<uInt>(blockSize);
  int zlibWindowBits = windowBits;
  constexpr int GZIP_DETECT_CODE = 32;
  if (isGzip) {
    zlibWindowBits = zlibWindowBits | GZIP_DETECT_CODE;
  }
  const auto result = inflateInit2(&zstream_, zlibWindowBits);
  DWIO_ENSURE_EQ(
      result,
      Z_OK,
      "Error from inflateInit2. error: ",
      result,
      " Info: ",
      streamDebugInfo_);
}

ZlibDecompressor::~ZlibDecompressor() {
  auto result = inflateEnd(&zstream_);
  DWIO_WARN_IF(
      result != Z_OK,
      "Error in ~ZlibDecompressor(). error: ",
      result,
      " Info: ",
      streamDebugInfo_);
}

uint64_t ZlibDecompressor::decompress(
    const char* src,
    uint64_t srcLength,
    char* dest,
    uint64_t destLength) {
  reset();
  zstream_.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(src));
  zstream_.avail_in = folly::to<uInt>(srcLength);
  zstream_.next_out = reinterpret_cast<Bytef*>(const_cast<char*>(dest));
  zstream_.avail_out = folly::to<uInt>(destLength);
  auto result = inflate(&zstream_, Z_FINISH);
  DWIO_ENSURE_EQ(
      result,
      Z_STREAM_END,
      "Error in ZlibDecompressor::decompress. error: ",
      result);
  return destLength - zstream_.avail_out;
}

class LzoAndLz4DecompressorCommon : public Decompressor {
 public:
  explicit LzoAndLz4DecompressorCommon(
      uint64_t blockSize,
      const CompressionKind& kind,
      bool isHadoopFrameFormat,
      const std::string& streamDebugInfo)
      : Decompressor{blockSize, streamDebugInfo},
        kind_(kind),
        isHadoopFrameFormat_(isHadoopFrameFormat) {}

  uint64_t decompress(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) override;

  virtual uint64_t decompressInternal(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) = 0;

 protected:
  CompressionKind kind_;
  // When compressor creates multiple compressed blocks, this will be
  // 'true', e.g., parquet uses this, whereas dwrf/orc creates single
  // compressed block.
  bool isHadoopFrameFormat_;
};

uint64_t LzoAndLz4DecompressorCommon::decompress(
    const char* src,
    uint64_t srcLength,
    char* dest,
    uint64_t destLength) {
  if (!isHadoopFrameFormat_) {
    return decompressInternal(src, srcLength, dest, destLength);
  }

  // For parquet, the format could be frame format, try to decompress that
  // format.
  uint32_t decompressedTotalSize = 0;
  auto* inputPtr = src;
  auto* outPtr = dest;
  uint64_t compressedSize = srcLength;
  auto uncompressedSize = destLength;

  while (compressedSize > 0) {
    DWIO_ENSURE_GE(
        compressedSize,
        dwio::common::INT_BYTE_SIZE,
        "{} decompression failed, input len is too small: {}",
        kind_,
        compressedSize);

    uint32_t decompressedBlockSize =
        folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
    inputPtr += dwio::common::INT_BYTE_SIZE;
    compressedSize -= dwio::common::INT_BYTE_SIZE;
    uint32_t remainingOutputSize = uncompressedSize - decompressedTotalSize;

    DWIO_ENSURE_GE(
        remainingOutputSize,
        decompressedBlockSize,
        "{} decompression failed, remainingOutputSize is less than "
        "decompressedBlockSize, remainingOutputSize: {}, "
        "decompressedBlockSize: {}",
        kind_,
        remainingOutputSize,
        decompressedBlockSize);

    if (compressedSize <= 0) {
      break;
    }

    do {
      // Check that input length should not be negative.
      DWIO_ENSURE_GE(
          compressedSize,
          dwio::common::INT_BYTE_SIZE,
          "{} decompression failed, input len is too small: {}",
          kind_,
          compressedSize);
      // Read the length of the next lz4/lzo compressed block.
      uint32_t compressedBlockSize =
          folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
      inputPtr += dwio::common::INT_BYTE_SIZE;
      compressedSize -= dwio::common::INT_BYTE_SIZE;

      if (compressedBlockSize == 0) {
        continue;
      }

      DWIO_ENSURE_LE(
          compressedBlockSize,
          compressedSize,
          "{} decompression failed, compressedBlockSize is greater than compressedSize, "
          "compressedBlockSize: {}, compressedSize: {}",
          kind_,
          compressedBlockSize,
          compressedSize);

      // Decompress this block.
      remainingOutputSize = uncompressedSize - decompressedTotalSize;
      uint64_t decompressedSize = decompressInternal(
          inputPtr,
          static_cast<int32_t>(compressedBlockSize),
          outPtr,
          static_cast<int32_t>(remainingOutputSize));

      DWIO_ENSURE_LE(
          decompressedSize,
          remainingOutputSize,
          "{} decompression failed, decompressedSize is not less than or equal to remainingOutputSize, "
          "decompressedSize: {}, remainingOutputSize: {}",
          ::facebook::velox::common::compressionKindToString(kind_),
          decompressedSize,
          remainingOutputSize);

      outPtr += decompressedSize;
      inputPtr += compressedBlockSize;
      compressedSize -= compressedBlockSize;
      decompressedBlockSize -= decompressedSize;
      decompressedTotalSize += decompressedSize;
    } while (decompressedBlockSize > 0);
  }

  DWIO_ENSURE_EQ(
      decompressedTotalSize,
      uncompressedSize,
      "{} decompression failed, decompressedTotalSize is not equal to uncompressedSize, "
      "decompressedTotalSize: {}, uncompressedSize: {}",
      kind_,
      decompressedTotalSize,
      uncompressedSize);

  return decompressedTotalSize;
}

class LzoDecompressor : public LzoAndLz4DecompressorCommon {
 public:
  explicit LzoDecompressor(
      uint64_t blockSize,
      bool isHadoopFrameFormat,
      const std::string& streamDebugInfo)
      : LzoAndLz4DecompressorCommon{
            blockSize,
            velox::common::CompressionKind_LZO,
            isHadoopFrameFormat,
            streamDebugInfo} {}

  uint64_t decompressInternal(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) override {
    return ::facebook::velox::common::compression::lzoDecompress(
        src, src + srcLength, dest, dest + destLength);
  }
};

class Lz4Decompressor : public LzoAndLz4DecompressorCommon {
 public:
  explicit Lz4Decompressor(
      uint64_t blockSize,
      bool isHadoopFrameFormat,
      const std::string& streamDebugInfo)
      : LzoAndLz4DecompressorCommon{
            blockSize,
            velox::common::CompressionKind_LZ4,
            isHadoopFrameFormat,
            streamDebugInfo} {}

  uint64_t decompressInternal(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) override;
};

uint64_t Lz4Decompressor::decompressInternal(
    const char* src,
    uint64_t srcLength,
    char* dest,
    uint64_t destLength) {
  int32_t result = LZ4_decompress_safe(
      src,
      dest,
      static_cast<int32_t>(srcLength),
      static_cast<int32_t>(destLength));

  DWIO_ENSURE_GE(
      result, 0, "lz4 failed to decompress. Info: ", streamDebugInfo_);
  return static_cast<uint64_t>(result);
}

// NOTE: We do not keep `ZSTD_DCtx' around on purpose, because if we keep it
// around, in flat map column reader we have hundreds of thousands of
// decompressors at same time and causing OOM.
class ZstdDecompressor : public Decompressor {
 public:
  explicit ZstdDecompressor(
      uint64_t blockSize,
      const std::string& streamDebugInfo)
      : Decompressor{blockSize, streamDebugInfo} {}

  uint64_t decompress(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) override;

  std::pair<int64_t, bool> getDecompressedLength(
      const char* src,
      uint64_t srcLength) const override;
};

uint64_t ZstdDecompressor::decompress(
    const char* src,
    uint64_t srcLength,
    char* dest,
    uint64_t destLength) {
  auto ret = ZSTD_decompress(dest, destLength, src, srcLength);
  DWIO_ENSURE(
      !ZSTD_isError(ret),
      "ZSTD returned an error: ",
      ZSTD_getErrorName(ret),
      " Info: ",
      streamDebugInfo_);
  return ret;
}

std::pair<int64_t, bool> ZstdDecompressor::getDecompressedLength(
    const char* src,
    uint64_t srcLength) const {
  auto uncompressedLength = ZSTD_getFrameContentSize(src, srcLength);
  // in the case when decompression size is not available, return the upper
  // bound
  if (uncompressedLength == ZSTD_CONTENTSIZE_UNKNOWN ||
      uncompressedLength == ZSTD_CONTENTSIZE_ERROR) {
    return {blockSize_, false};
  }
  DWIO_ENSURE_LE(
      uncompressedLength,
      blockSize_,
      "Insufficient buffer size. Info: ",
      streamDebugInfo_);
  return {uncompressedLength, true};
}

class SnappyDecompressor : public Decompressor {
 public:
  explicit SnappyDecompressor(
      uint64_t blockSize,
      const std::string& streamDebugInfo)
      : Decompressor{blockSize, streamDebugInfo} {}

  uint64_t decompress(
      const char* src,
      uint64_t srcLength,
      char* dest,
      uint64_t destLength) override;

  std::pair<int64_t, bool> getDecompressedLength(
      const char* src,
      uint64_t srcLength) const override;
};

uint64_t SnappyDecompressor::decompress(
    const char* src,
    uint64_t srcLength,
    char* dest,
    uint64_t destLength) {
  auto [length, _] = getDecompressedLength(src, srcLength);
  DWIO_ENSURE_GE(destLength, length);
  DWIO_ENSURE(
      snappy::RawUncompress(src, srcLength, dest),
      "Snappy decompress failed. Info: ",
      streamDebugInfo_);
  return length;
}

std::pair<int64_t, bool> SnappyDecompressor::getDecompressedLength(
    const char* src,
    uint64_t srcLength) const {
  size_t uncompressedLength;
  // in the case when decompression size is not available, return the upper
  // bound
  if (!snappy::GetUncompressedLength(src, srcLength, &uncompressedLength)) {
    return {blockSize_, false};
  }
  DWIO_ENSURE_LE(
      uncompressedLength,
      blockSize_,
      "Insufficient buffer size. Info: ",
      streamDebugInfo_);
  return {uncompressedLength, true};
}

// TODO: Is this really needed?
class ZlibDecompressionStream : public PagedInputStream,
                                private ZlibDecompressor {
 public:
  ZlibDecompressionStream(
      std::unique_ptr<dwio::common::SeekableInputStream> inStream,
      uint64_t blockSize,
      MemoryPool& pool,
      int windowBits,
      const std::string& streamDebugInfo,
      bool isGzip = false,
      bool useRawDecompression = false,
      size_t compressedLength = 0)
      : PagedInputStream{std::move(inStream), pool, streamDebugInfo, useRawDecompression, compressedLength},
        ZlibDecompressor{blockSize, windowBits, streamDebugInfo, isGzip} {}
  ~ZlibDecompressionStream() override = default;

  bool readOrSkip(const void** data, int32_t* size) override;
};

bool ZlibDecompressionStream::readOrSkip(const void** data, int32_t* size) {
  if (data) {
    VELOX_CHECK_EQ(pendingSkip_, 0);
  }
  // if the user pushed back, return them the partial buffer
  if (outputBufferLength_) {
    if (data) {
      *data = outputBufferPtr_;
    }
    *size = static_cast<int32_t>(outputBufferLength_);
    outputBufferPtr_ += outputBufferLength_;
    bytesReturned_ += outputBufferLength_;
    outputBufferLength_ = 0;
    return true;
  }
  if (state_ == State::HEADER || remainingLength_ == 0) {
    readHeader();
  }
  if (state_ == State::END) {
    return false;
  }
  if (inputBufferPtr_ == inputBufferPtrEnd_) {
    readBuffer(true);
  }
  size_t availSize = std::min(
      static_cast<size_t>(inputBufferPtrEnd_ - inputBufferPtr_),
      remainingLength_);
  if (state_ == State::ORIGINAL) {
    if (data) {
      *data = inputBufferPtr_;
    }
    *size = static_cast<int32_t>(availSize);
    outputBufferPtr_ = inputBufferPtr_ + availSize;
    outputBufferLength_ = 0;
  } else {
    DWIO_ENSURE_EQ(
        state_,
        State::START,
        "Unknown compression state in ZlibDecompressionStream::Next in ",
        getName(),
        " Info: ",
        ZlibDecompressor::streamDebugInfo_);
    prepareOutputBuffer(
        getDecompressedLength(inputBufferPtr_, availSize).first);

    reset();
    zstream_.next_in =
        reinterpret_cast<Bytef*>(const_cast<char*>(inputBufferPtr_));
    zstream_.avail_in = folly::to<uInt>(availSize);
    outputBufferPtr_ = outputBuffer_->data();
    zstream_.next_out =
        reinterpret_cast<Bytef*>(const_cast<char*>(outputBufferPtr_));
    zstream_.avail_out = folly::to<uInt>(blockSize_);
    int32_t result;
    do {
      result = inflate(
          &zstream_, availSize == remainingLength_ ? Z_FINISH : Z_SYNC_FLUSH);
      switch (result) {
        case Z_OK:
          remainingLength_ -= availSize;
          inputBufferPtr_ += availSize;
          readBuffer(true);
          availSize = std::min(
              static_cast<size_t>(inputBufferPtrEnd_ - inputBufferPtr_),
              remainingLength_);
          zstream_.next_in =
              reinterpret_cast<Bytef*>(const_cast<char*>(inputBufferPtr_));
          zstream_.avail_in = static_cast<uInt>(availSize);
          break;
        case Z_STREAM_END:
          break;
        default:
          DWIO_RAISE(
              "Error in ZlibDecompressionStream::Next in ",
              getName(),
              ". error: ",
              result,
              " Info: ",
              ZlibDecompressor::streamDebugInfo_);
      }
    } while (result != Z_STREAM_END);
    *size = static_cast<int32_t>(blockSize_ - zstream_.avail_out);
    if (data) {
      *data = outputBufferPtr_;
    }
    outputBufferLength_ = 0;
    outputBufferPtr_ += *size;
  }

  inputBufferPtr_ += availSize;
  remainingLength_ -= availSize;
  bytesReturned_ += *size;
  return true;
}

} // namespace

std::unique_ptr<Compressor> createCompressor(
    CompressionKind kind,
    const CompressionOptions& options) {
  switch (kind) {
    case CompressionKind::CompressionKind_NONE:
      return nullptr;
    case CompressionKind::CompressionKind_ZLIB: {
      XLOG_FIRST_N(INFO, 1) << fmt::format(
          "Initialized zlib compressor with compression level {}",
          options.format.zlib.compressionLevel);
      return std::make_unique<ZlibCompressor>(
          options.format.zlib.compressionLevel);
    }
    case CompressionKind::CompressionKind_ZSTD: {
      XLOG_FIRST_N(INFO, 1) << fmt::format(
          "Initialized zstd compressor with compression level {}",
          options.format.zstd.compressionLevel);
      return std::make_unique<ZstdCompressor>(
          options.format.zstd.compressionLevel);
    }
    case CompressionKind::CompressionKind_SNAPPY:
    case CompressionKind::CompressionKind_LZO:
    case CompressionKind::CompressionKind_LZ4:
    default:
      VELOX_UNSUPPORTED(
          "Unsupported compression type: {}", compressionKindToString(kind));
  }
  return nullptr;
}

std::unique_ptr<dwio::common::SeekableInputStream> createDecompressor(
    CompressionKind kind,
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    uint64_t blockSize,
    MemoryPool& pool,
    const CompressionOptions& options,
    const std::string& streamDebugInfo,
    const Decrypter* decrypter,
    bool useRawDecompression,
    size_t compressedLength) {
  std::unique_ptr<Decompressor> decompressor;
  switch (static_cast<int64_t>(kind)) {
    case CompressionKind::CompressionKind_NONE:
      if (!decrypter) {
        return input;
      }
      // decompressor remain as nullptr
      break;
    case CompressionKind::CompressionKind_ZLIB:
      if (!decrypter) {
        // When file is not encrypted, we can use zlib streaming codec to avoid
        // copying data
        return std::make_unique<ZlibDecompressionStream>(
            std::move(input),
            blockSize,
            pool,
            options.format.zlib.windowBits,
            streamDebugInfo,
            false,
            useRawDecompression,
            compressedLength);
      }
      decompressor = std::make_unique<ZlibDecompressor>(
          blockSize, options.format.zlib.windowBits, streamDebugInfo, false);
      break;
    case CompressionKind::CompressionKind_GZIP:
      if (!decrypter) {
        // When file is not encrypted, we can use zlib streaming codec to avoid
        // copying data
        return std::make_unique<ZlibDecompressionStream>(
            std::move(input),
            blockSize,
            pool,
            options.format.zlib.windowBits,
            streamDebugInfo,
            true,
            useRawDecompression,
            compressedLength);
      }
      decompressor = std::make_unique<ZlibDecompressor>(
          blockSize, options.format.zlib.windowBits, streamDebugInfo, true);
      break;
    case CompressionKind::CompressionKind_SNAPPY:
      decompressor =
          std::make_unique<SnappyDecompressor>(blockSize, streamDebugInfo);
      break;
    case CompressionKind::CompressionKind_LZO:
      decompressor = std::make_unique<LzoDecompressor>(
          blockSize,
          options.format.lz4_lzo.isHadoopFrameFormat,
          streamDebugInfo);
      break;
    case CompressionKind::CompressionKind_LZ4:
      decompressor = std::make_unique<Lz4Decompressor>(
          blockSize,
          options.format.lz4_lzo.isHadoopFrameFormat,
          streamDebugInfo);
      break;
    case CompressionKind::CompressionKind_ZSTD:
      decompressor =
          std::make_unique<ZstdDecompressor>(blockSize, streamDebugInfo);
      break;
    default:
      DWIO_RAISE("Unknown compression codec ", kind);
  }
  return std::make_unique<PagedInputStream>(
      std::move(input),
      pool,
      std::move(decompressor),
      decrypter,
      streamDebugInfo,
      useRawDecompression,
      compressedLength);
}

} // namespace facebook::velox::dwio::common::compression
