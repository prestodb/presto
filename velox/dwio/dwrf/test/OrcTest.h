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

#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/dwrf/common/Encryption.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"
#include "velox/dwio/dwrf/writer/IndexBuilder.h"
#include "velox/dwio/dwrf/writer/WriterBase.h"

#include <gmock/gmock.h>
#include <google/protobuf/wire_format_lite.h>

namespace facebook::velox::dwrf {

#define VELOX_ARRAY_SIZE(array) (sizeof(array) / sizeof(*array))

using MemoryPool = memory::MemoryPool;

inline std::string getExampleFilePath(const std::string& fileName) {
  return velox::test::getDataFilePath(
      "velox/dwio/dwrf/test", "examples/" + fileName);
}

class MockStripeStreams : public StripeStreams {
 public:
  MockStripeStreams() : scopedPool_{memory::getDefaultScopedMemoryPool()} {};
  ~MockStripeStreams() = default;

  std::unique_ptr<dwio::common::SeekableInputStream> getStream(
      const DwrfStreamIdentifier& si,
      bool throwIfNotFound) const override {
    return std::unique_ptr<dwio::common::SeekableInputStream>(getStreamProxy(
        si.encodingKey().node,
        static_cast<proto::Stream_Kind>(si.kind()),
        throwIfNotFound));
  }

  std::function<BufferPtr()> getIntDictionaryInitializerForNode(
      const EncodingKey& ek,
      uint64_t /* unused */,
      uint64_t /* unused */) override {
    return [this, nodeId = ek.node, sequenceId = ek.sequence]() {
      BufferPtr dictionaryData;
      genMockDictDataSetter(nodeId, sequenceId)(
          dictionaryData, &getMemoryPool());
      return dictionaryData;
    };
  }

  const proto::ColumnEncoding& getEncoding(
      const EncodingKey& ek) const override {
    return *getEncodingProxy(ek.node);
  }

  virtual dwio::common::FileFormat getFormat() const override {
    return dwio::common::FileFormat::DWRF;
  }

  MOCK_METHOD2(
      genMockDictDataSetter,
      std::function<void(BufferPtr&, MemoryPool*)>(uint32_t, uint32_t));
  MOCK_METHOD0(
      getStripeDictionaryCache,
      std::shared_ptr<StripeDictionaryCache>());
  MOCK_CONST_METHOD1(getEncodingProxy, proto::ColumnEncoding*(uint64_t));
  MOCK_CONST_METHOD2(
      visitStreamsOfNode,
      uint32_t(uint32_t, std::function<void(const StreamInformation&)>));
  MOCK_CONST_METHOD3(
      getStreamProxy,
      dwio::common::SeekableInputStream*(uint32_t, proto::Stream_Kind, bool));
  MOCK_CONST_METHOD0(getStrideIndexProviderProxy, StrideIndexProvider*());
  MOCK_CONST_METHOD0(getColumnSelectorProxy, dwio::common::ColumnSelector*());
  MOCK_CONST_METHOD0(
      getRowReaderOptionsProxy,
      dwio::common::RowReaderOptions*());

  const dwio::common::ColumnSelector& getColumnSelector() const override {
    return *getColumnSelectorProxy();
  }

  const dwio::common::RowReaderOptions& getRowReaderOptions() const override {
    auto ptr = getRowReaderOptionsProxy();
    return ptr ? *ptr : options_;
  }

  bool getUseVInts(const DwrfStreamIdentifier& /* streamId */) const override {
    return true; // current tests all expect results from using vints
  }

  MemoryPool& getMemoryPool() const override {
    return scopedPool_->getPool();
  }

  const StrideIndexProvider& getStrideIndexProvider() const override {
    return *getStrideIndexProviderProxy();
  }

  uint32_t rowsPerRowGroup() const override {
    // Disable efficient skipping using row index.
    return 1'000'000;
  }

 private:
  std::unique_ptr<memory::ScopedMemoryPool> scopedPool_;
  dwio::common::RowReaderOptions options_;
};

inline uint64_t zigZagEncode(int64_t val) {
  return google::protobuf::internal::WireFormatLite::ZigZagEncode64(val);
}

inline int64_t zigZagDecode(uint64_t val) {
  return google::protobuf::internal::WireFormatLite::ZigZagDecode64(val);
}

inline size_t writeVuLong(char* buffer, size_t pos, uint64_t val) {
  while (true) {
    if ((val & ~dwio::common::BASE_128_MASK) == 0) {
      buffer[pos++] = static_cast<char>(val);
      return pos;
    } else {
      buffer[pos++] =
          static_cast<char>((val & dwio::common::BASE_128_MASK) | 0x80);
      val >>= 7;
    }
  }
}

inline size_t writeVsLongs(char* buffer, const std::vector<int64_t>& vals) {
  size_t pos = 0;
  for (auto val : vals) {
    pos = writeVuLong(buffer, pos, zigZagEncode(val));
  }
  return pos;
}

inline size_t writeVuLongs(char* buffer, const std::vector<uint64_t>& vals) {
  size_t pos = 0;
  for (auto val : vals) {
    pos = writeVuLong(buffer, pos, val);
  }
  return pos;
}

inline size_t writeRange(char* buffer, size_t pos, int64_t begin, int64_t end) {
  while (begin < end) {
    pos = writeVuLong(buffer, pos, zigZagEncode(begin++));
  }
  return pos;
}

inline size_t writeRange(char* buffer, int64_t begin, int64_t end) {
  return writeRange(buffer, 0, begin, end);
}

// Fills 'buffer' with values from 'begin' to 'end'
// (exclusive). 'begin' and 'end' can be wider than T to bracket
// min/max of T.
template <typename T = int64_t>
inline void setSequence(BufferPtr& buffer, int64_t begin, int64_t end) {
  auto array = buffer->asMutable<T>();
  for (auto i = 0; i + begin < end; ++i) {
    array[i] = i + begin;
  }
}

template <typename T>
inline BufferPtr sequence(MemoryPool* pool, int64_t begin, int64_t end) {
  BufferPtr buffer = AlignedBuffer::allocate<T>(end - begin, pool);
  setSequence<T>(buffer, begin, end);
  return buffer;
}

class TestPositionRecorder : public PositionRecorder {
 public:
  explicit TestPositionRecorder() {
    addEntry();
  }

  void addEntry() {
    pos_.push_back({});
  }

  void add(uint64_t pos, int32_t strideIndex) override {
    pos_[strideIndex < 0 ? pos_.size() - 1 : strideIndex].push_back(pos);
  }

  const std::vector<uint64_t>& getPositions() const {
    return pos_.back();
  }

  const std::vector<uint64_t>& getPositions(size_t strideIndex) const {
    return pos_.at(strideIndex);
  }

 private:
  std::vector<std::vector<uint64_t>> pos_;
};

class MockIndexBuilder : public IndexBuilder {
 public:
  explicit MockIndexBuilder() : IndexBuilder{nullptr} {}

  MOCK_METHOD2(add, void(uint64_t, int));
  MOCK_METHOD1(addEntry, void(const StatisticsBuilder&));
  MOCK_CONST_METHOD0(getEntrySize, size_t());
  MOCK_METHOD0(flush, void());
};

class ProtoWriter : public WriterBase {
 public:
  ProtoWriter(memory::MemoryPool& pool)
      : WriterBase{std::make_unique<dwio::common::MemorySink>(pool, 1024)} {
    initContext(
        std::make_shared<Config>(), pool.addScopedChild("proto_writer"));
  }

  template <typename T>
  void writeProto(
      std::string& output,
      const T& t,
      const dwio::common::encryption::Encrypter& enc) {
    writeProtoAsString<T>(output, t, std::addressof(enc));
  }
};

} // namespace facebook::velox::dwrf
