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

// Adapted from Apache Arrow.

#include "velox/dwio/parquet/common/BloomFilter.h"
#include "velox/dwio/parquet/common/XxHasher.h"
#include "velox/dwio/parquet/thrift/ThriftTransport.h"

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <cstdint>
#include <cstring>
#include <memory>

namespace facebook::velox::parquet {

constexpr uint32_t BlockSplitBloomFilter::SALT[kBitsSetPerBlock];

BlockSplitBloomFilter::BlockSplitBloomFilter(memory::MemoryPool* pool)
    : pool_(pool),
      hashStrategy_(HashStrategy::XXHASH),
      algorithm_(Algorithm::BLOCK),
      compressionStrategy_(CompressionStrategy::UNCOMPRESSED) {}

void BlockSplitBloomFilter::init(uint32_t numBytes) {
  if (numBytes < kMinimumBloomFilterBytes) {
    numBytes = kMinimumBloomFilterBytes;
  }

  // Get next power of 2 if it is not power of 2.
  if ((numBytes & (numBytes - 1)) != 0) {
    numBytes = static_cast<uint32_t>(bits::nextPowerOfTwo(numBytes));
  }

  if (numBytes > kMaximumBloomFilterBytes) {
    numBytes = kMaximumBloomFilterBytes;
  }

  numBytes_ = numBytes;
  data_ = AlignedBuffer::allocate<char>(numBytes_, pool_);
  memset(data_->asMutable<char>(), 0, numBytes_);

  this->hasher_ = std::make_unique<XxHasher>();
}

void BlockSplitBloomFilter::init(const uint8_t* bitset, uint32_t numBytes) {
  VELOX_CHECK(bitset != nullptr);

  if (numBytes < kMinimumBloomFilterBytes ||
      numBytes > kMaximumBloomFilterBytes || (numBytes & (numBytes - 1)) != 0) {
    VELOX_FAIL("Given length of bitset is illegal");
  }

  numBytes_ = numBytes;
  data_ = AlignedBuffer::allocate<char>(numBytes_, pool_);
  memcpy(data_->asMutable<char>(), bitset, numBytes_);

  this->hasher_ = std::make_unique<XxHasher>();
}

static void validateBloomFilterHeader(const thrift::BloomFilterHeader& header) {
  std::stringstream error;
  if (!header.algorithm.__isset.BLOCK) {
    error << "Unsupported Bloom filter algorithm: ";
    error << header.algorithm;
    VELOX_FAIL(error.str());
  }

  if (!header.hash.__isset.XXHASH) {
    error << "Unsupported Bloom filter hash: ", error << header.hash;
    VELOX_FAIL(error.str());
  }

  if (!header.compression.__isset.UNCOMPRESSED) {
    error << "Unsupported Bloom filter compression: ",
        error << header.compression;
    VELOX_FAIL(error.str());
  }

  if (header.numBytes <= 0 ||
      static_cast<uint32_t>(header.numBytes) >
          BloomFilter::kMaximumBloomFilterBytes) {
    error << "Bloom filter size is incorrect: " << header.numBytes
          << ". Must be in range (" << 0 << ", "
          << BloomFilter::kMaximumBloomFilterBytes << "].";
    VELOX_FAIL(error.str());
  }
}

BlockSplitBloomFilter BlockSplitBloomFilter::deserialize(
    dwio::common::SeekableInputStream* input,
    memory::MemoryPool& pool) {
  const void* headerBuffer;
  int32_t size;
  input->Next(&headerBuffer, &size);
  const char* bufferStart = reinterpret_cast<const char*>(headerBuffer);
  const char* bufferEnd = bufferStart + size;

  std::shared_ptr<thrift::ThriftTransport> transport =
      std::make_shared<thrift::ThriftStreamingTransport>(
          input, bufferStart, bufferEnd);
  apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport> protocol(
      transport);
  thrift::BloomFilterHeader header;
  uint32_t headerSize = header.read(&protocol);
  validateBloomFilterHeader(header);

  const int32_t bloomFilterSize = header.numBytes;
  if (bloomFilterSize + headerSize <= size) {
    // The bloom filter data is entirely contained in the buffer we just read
    // => just return it.
    BlockSplitBloomFilter bloomFilter(&pool);
    bloomFilter.init(
        reinterpret_cast<const uint8_t*>(headerBuffer) + headerSize,
        bloomFilterSize);
    return bloomFilter;
  }
  // We have read a part of the bloom filter already, copy it to the target
  // buffer and read the remaining part from the InputStream.
  auto buffer = AlignedBuffer::allocate<char>(bloomFilterSize, &pool);

  const auto bloomFilterSizeInHeaderBuffer = size - headerSize;
  if (bloomFilterSizeInHeaderBuffer > 0) {
    std::memcpy(
        buffer->asMutable<char>(),
        reinterpret_cast<const uint8_t*>(headerBuffer) + headerSize,
        bloomFilterSizeInHeaderBuffer);
  }
  const auto requiredReadSize = bloomFilterSize - bloomFilterSizeInHeaderBuffer;

  input->readFully(
      buffer->asMutable<char>() + bloomFilterSizeInHeaderBuffer,
      requiredReadSize);
  VELOX_CHECK_EQ(
      buffer->size(),
      bloomFilterSize,
      "Bloom Filter read failed: not enough data, read size: {}, actual size: {}",
      buffer->size(),
      bloomFilterSize);
  BlockSplitBloomFilter bloomFilter(&pool);
  bloomFilter.init(
      reinterpret_cast<const uint8_t*>(buffer->as<char>()), bloomFilterSize);
  return bloomFilter;
}

void BlockSplitBloomFilter::writeTo(
    velox::dwio::common::AppendOnlyBufferedStream* sink) const {
  VELOX_CHECK(sink != nullptr);

  thrift::BloomFilterHeader header;
  if (algorithm_ != BloomFilter::Algorithm::BLOCK) {
    VELOX_FAIL("BloomFilter does not support Algorithm other than BLOCK");
  }
  header.algorithm.__set_BLOCK(thrift::SplitBlockAlgorithm());
  if (hashStrategy_ != HashStrategy::XXHASH) {
    VELOX_FAIL("BloomFilter does not support Hash other than XXHASH");
  }
  header.hash.__set_XXHASH(thrift::XxHash());
  if (compressionStrategy_ != CompressionStrategy::UNCOMPRESSED) {
    VELOX_FAIL(
        "BloomFilter does not support Compression other than UNCOMPRESSED");
  }
  header.compression.__set_UNCOMPRESSED(thrift::Uncompressed());
  header.__set_numBytes(numBytes_);

  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> memBuffer =
      std::make_shared<apache::thrift::transport::TMemoryBuffer>();
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer>
      factory;
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol =
      factory.getProtocol(memBuffer);
  try {
    memBuffer->resetBuffer();
    header.write(protocol.get());
  } catch (std::exception& e) {
    std::stringstream ss;
    ss << "Couldn't serialize thrift: " << e.what() << "\n";
    VELOX_FAIL(ss.str());
  }
  uint8_t* outBuffer;
  uint32_t outLength;
  memBuffer->getBuffer(&outBuffer, &outLength);
  // write header
  sink->write(reinterpret_cast<const char*>(outBuffer), outLength);
  // write bitset
  sink->write(data_->as<char>(), numBytes_);
}

bool BlockSplitBloomFilter::findHash(uint64_t hash) const {
  const uint32_t bucketIndex = static_cast<uint32_t>(
      ((hash >> 32) * (numBytes_ / kBytesPerFilterBlock)) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  const uint32_t* bitset32 =
      reinterpret_cast<const uint32_t*>(data_->as<char>());

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    // Calculate mask for key in the given bitset.
    const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
    if (0 == (bitset32[kBitsSetPerBlock * bucketIndex + i] & mask)) {
      return false;
    }
  }
  return true;
}

void BlockSplitBloomFilter::insertHashImpl(uint64_t hash) {
  const uint32_t bucketIndex = static_cast<uint32_t>(
      ((hash >> 32) * (numBytes_ / kBytesPerFilterBlock)) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->asMutable<char>());

  for (int i = 0; i < kBitsSetPerBlock; i++) {
    // Calculate mask for key in the given bitset.
    const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
    bitset32[bucketIndex * kBitsSetPerBlock + i] |= mask;
  }
}

void BlockSplitBloomFilter::insertHash(uint64_t hash) {
  insertHashImpl(hash);
}

void BlockSplitBloomFilter::insertHashes(
    const uint64_t* hashes,
    int numValues) {
  for (int i = 0; i < numValues; ++i) {
    insertHashImpl(hashes[i]);
  }
}

} // namespace facebook::velox::parquet
