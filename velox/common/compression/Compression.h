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

#pragma once

#include <fmt/format.h>
#include <folly/Expected.h>
#include <folly/compression/Compression.h>
#include <string>

#include "velox/common/base/Status.h"

namespace facebook::velox::common {

enum CompressionKind {
  CompressionKind_NONE = 0,
  CompressionKind_ZLIB = 1,
  CompressionKind_SNAPPY = 2,
  CompressionKind_LZO = 3,
  CompressionKind_ZSTD = 4,
  CompressionKind_LZ4 = 5,
  CompressionKind_GZIP = 6,
  CompressionKind_MAX = INT64_MAX
};

std::unique_ptr<folly::compression::Codec> compressionKindToCodec(
    CompressionKind kind);

CompressionKind codecTypeToCompressionKind(folly::compression::CodecType type);

/// Get the name of the CompressionKind.
std::string compressionKindToString(CompressionKind kind);

CompressionKind stringToCompressionKind(const std::string& kind);

static constexpr uint64_t kDefaultCompressionBlockSize = 256 * 1024;

static constexpr int32_t kDefaultCompressionLevel =
    std::numeric_limits<int32_t>::min();

class StreamingCompressor;
class StreamingDecompressor;

struct CodecOptions {
  int32_t compressionLevel;

  CodecOptions(int32_t compressionLevel = kDefaultCompressionLevel)
      : compressionLevel(compressionLevel) {}

  virtual ~CodecOptions() = default;
};

/// Codec interface for compression and decompression.
/// The Codec class provides a common interface for various compression
/// algorithms to support one-shot compression and decompression.
///
/// For codecs that support streaming compression and decompression, the
/// makeStreamingCompressor() and makeStreamingDecompressor() functions can be
/// used to create streaming compressor and decompressor instances.
class Codec {
 public:
  virtual ~Codec() = default;

  // Create a kind for the given compression algorithm with CodecOptions.
  static Expected<std::unique_ptr<Codec>> create(
      CompressionKind kind,
      const CodecOptions& codecOptions = CodecOptions{});

  // Create a kind for the given compression algorithm.
  static Expected<std::unique_ptr<Codec>> create(
      CompressionKind kind,
      int32_t compressionLevel);

  // Return true if support for indicated kind has been enabled.
  static bool isAvailable(CompressionKind kind);

  /// Return true if indicated kind supports extracting uncompressed length
  /// from compressed data.
  static bool supportsGetUncompressedLength(CompressionKind kind);

  /// Return true if indicated kind supports one-shot compression with fixed
  /// compressed length.
  static bool supportsCompressFixedLength(CompressionKind kind);

  /// Return the smallest supported compression level.
  /// If the codec doesn't support compression level,
  /// `kUseDefaultCompressionLevel` will be returned.
  virtual int32_t minCompressionLevel() const = 0;

  /// Return the largest supported compression level.
  /// If the codec doesn't support compression level,
  /// `kUseDefaultCompressionLevel` will be returned.
  virtual int32_t maxCompressionLevel() const = 0;

  /// Return the default compression level.
  /// If the codec doesn't support compression level,
  /// `kUseDefaultCompressionLevel` will be returned.
  virtual int32_t defaultCompressionLevel() const = 0;

  /// Performs one-shot compression. The actual compressed length is returned.
  /// `outputLength` must first have been computed using maxCompressedLength().
  /// `input` and `output` must be valid, non-null pointers, otherwise
  /// compression will fail.
  /// Note: One-shot compression is not always compatible with streaming
  /// decompression. Depending on the codec (e.g. LZ4), different formats may be
  /// used.
  virtual Expected<uint64_t> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) = 0;

  /// One-shot decompression function. The actual decompressed length is
  /// returned.
  /// `outputLength` must be correct and therefore be obtained in advance.
  /// `input` and `output` must be valid, non-null pointers, otherwise
  /// decompression will fail.
  /// Note: One-shot decompression is not always compatible with streaming
  /// compression. Depending on the codec (e.g. LZ4), different formats may be
  /// used.
  virtual Expected<uint64_t> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) = 0;

  /// Performs one-shot compression.
  /// This function compresses data and writes the output up to the specified
  /// outputLength. If outputLength is too small to hold all the compressed
  /// data, the function doesn't fail. Instead, it returns the number of bytes
  /// actually written to the output buffer. Any remaining data that couldn't
  /// be written in this call will be written in subsequent calls to this
  /// function. This is useful when fixed-length compression blocks are required
  /// by the caller.
  virtual Expected<uint64_t> compressFixedLength(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength);

  // Maximum compressed length of given input length.
  virtual uint64_t maxCompressedLength(uint64_t inputLength) = 0;

  /// Retrieves the uncompressed length of the given compressed data using the
  /// specified compression library.
  /// If the input data is corrupted, returns `Unexpected` with
  /// `Status::IOError`. Not all compression libraries support this
  /// functionality. Use supportsGetUncompressedLength() to check before
  /// calling. If unsupported, returns `Unexpected` with `Status::Invalid`.
  virtual Expected<uint64_t> getUncompressedLength(
      const uint8_t* input,
      uint64_t inputLength) const;

  // Return true if indicated kind supports creating streaming de/compressor.
  virtual bool supportsStreamingCompression() const;

  /// Create a streaming compressor instance.
  /// Use supportsStreamingCompression() to check before calling.
  /// If unsupported, returns `Unexpected` with `Status::Invalid`.
  virtual Expected<std::shared_ptr<StreamingCompressor>>
  makeStreamingCompressor();

  /// Create a streaming decompressor instance.
  /// Use supportsStreamingCompression() to check before calling.
  /// If unsupported, returns `Unexpected` with `Status::Invalid`.
  virtual Expected<std::shared_ptr<StreamingDecompressor>>
  makeStreamingDecompressor();

  // This Codec's compression type.
  virtual CompressionKind compressionKind() const = 0;

  // This Codec's compression level, if applicable.
  virtual int32_t compressionLevel() const;

  // The name of this Codec's compression type.
  virtual std::string_view name() const = 0;

 private:
  // Initializes the codec's resources.
  virtual Status init();
};

/// Base class for streaming compressors. Unlike one-shot compression, streaming
/// compression can compress data with arbitrary length and write the compressed
/// data through multiple calls to compress().
/// The caller is responsible for providing a sufficiently large output buffer
/// for compress(), flush(), and finalize(), and checking the `outputTooSmall`
/// from the returned result. If `outputTooSmall` is true, the caller should
/// provide a larger output buffer and call the corresponding function again.
class StreamingCompressor {
 public:
  virtual ~StreamingCompressor() = default;

  struct CompressResult {
    uint64_t bytesRead;
    uint64_t bytesWritten;
    bool outputTooSmall;
  };

  struct FlushResult {
    uint64_t bytesWritten;
    bool outputTooSmall;
  };

  struct EndResult : FlushResult {};

  /// Compress some input.
  /// If CompressResult.outputTooSmall is true on return, compress() should be
  /// called again with a larger output buffer, such as doubling its size.
  /// `input` and `output` must be valid, non-null pointers, otherwise
  /// compression will fail.
  virtual Expected<CompressResult> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) = 0;

  /// Flush part of the compressed output.
  /// If FlushResult.outputTooSmall is true on return, flush() should be called
  /// again with a larger output buffer, such as doubling its size. `output`
  /// must be a valid, non-null pointer.
  virtual Expected<FlushResult> flush(
      uint8_t* output,
      uint64_t outputLength) = 0;

  /// End compressing, doing whatever is necessary to end the stream, and
  /// flushing the compressed output. `output` must be a valid, non-null
  /// pointer.
  /// If EndResult.outputTooSmall is true on return, finalize() should be
  /// called again with a larger output buffer, such as doubling its size.
  /// Otherwise, the StreamingCompressor should not be used anymore.
  virtual Expected<EndResult> finalize(
      uint8_t* output,
      uint64_t outputLength) = 0;
};

/// Base class for streaming decompressors. Streaming decompression can process
/// data with arbitrary length and write the decompressed data through multiple
/// calls to decompress().
/// The caller is responsible for providing a sufficiently large output buffer
/// for decompress(), and checking the `outputTooSmall` in the returned result.
/// If `outputTooSmall` is true, the caller should provide a larger output
/// buffer and call decompress() again.
class StreamingDecompressor {
 public:
  virtual ~StreamingDecompressor() = default;

  struct DecompressResult {
    uint64_t bytesRead;
    uint64_t bytesWritten;
    bool outputTooSmall;
  };

  /// Decompress some input.
  /// If DecompressResult.outputTooSmall is true on return, decompress() should
  /// be called again with a larger output buffer, such as doubling its size.
  /// `input` and `output` must be valid, non-null pointers, otherwise
  /// decompression will fail.
  virtual Expected<DecompressResult> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) = 0;

  // Return whether the compressed stream is finished.
  virtual bool isFinished() = 0;

  // Reinitialize decompressor, making it ready for a new compressed stream.
  virtual Status reset() = 0;
};

} // namespace facebook::velox::common

template <>
struct fmt::formatter<facebook::velox::common::CompressionKind>
    : fmt::formatter<std::string> {
  auto format(
      const facebook::velox::common::CompressionKind& s,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::common::compressionKindToString(s), ctx);
  }
};
