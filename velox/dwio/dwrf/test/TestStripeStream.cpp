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

#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/common/encryption/TestProvider.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/dwio/dwrf/writer/WriterBase.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/type/Type.h"

using namespace testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwrf::encryption;
using namespace facebook::velox::memory;

using facebook::velox::RowType;
using facebook::velox::dwio::type::fbhive::HiveTypeParser;

class RecordingInputStream : public MemoryInputStream {
 public:
  RecordingInputStream() : MemoryInputStream{nullptr, 0} {}

  void read(
      void* /* unused */,
      uint64_t length,
      uint64_t offset,
      MetricsLog::MetricsType) override {
    reads_.push_back({offset, length});
  }

  const std::vector<Region>& getReads() const {
    return reads_;
  }

 private:
  std::vector<Region> reads_;
};

class TestProvider : public StrideIndexProvider {
 public:
  uint64_t getStrideIndex() const override {
    return 0;
  }
};

namespace {
void enqueueReads(
    const StripeReaderBase& reader,
    const ColumnSelector& selector,
    uint64_t stripeStart,
    uint32_t stripeIndex) {
  auto& input = reader.getStripeInput();
  auto& metadataCache = reader.getReader().getMetadataCache();
  uint64_t offset = stripeStart;
  uint64_t length = 0;
  uint32_t regions = 0;
  auto& footer = reader.getStripeFooter();
  for (const auto& stream : footer.streams()) {
    length = stream.length();
    // If index cache is available, there is no need to read it
    auto inMetaCache = metadataCache &&
        metadataCache->has(proto::StripeCacheMode::INDEX, stripeIndex) &&
        static_cast<StreamKind>(stream.kind()) ==
            StreamKind::StreamKind_ROW_INDEX;
    if (length > 0 &&
        selector.shouldReadStream(stream.node(), stream.sequence()) &&
        !inMetaCache) {
      input.enqueue({offset, length});
      regions++;
    }
    offset += length;
  }
}

StripeStreamsImpl createAndLoadStripeStreams(
    const StripeReaderBase& stripeReader,
    const ColumnSelector& selector) {
  TestProvider indexProvider;
  StripeStreamsImpl streams{
      stripeReader, selector, RowReaderOptions{}, 0, indexProvider, 0};
  enqueueReads(stripeReader, selector, 0, 0);
  streams.loadReadPlan();
  return streams;
}
} // namespace

TEST(StripeStream, planReads) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  google::protobuf::Arena arena;
  auto footer = google::protobuf::Arena::CreateMessage<proto::Footer>(&arena);
  footer->set_rowindexstride(100);
  auto type = HiveTypeParser().parse("struct<a:int,b:float>");
  ProtoUtils::writeType(*type, *footer);
  auto is = std::make_unique<RecordingInputStream>();
  auto isPtr = is.get();
  auto readerBase = std::make_shared<ReaderBase>(
      pool,
      std::move(is),
      std::make_unique<proto::PostScript>(),
      footer,
      nullptr);
  ColumnSelector cs{readerBase->getSchema(), std::vector<uint64_t>{2}, true};
  auto stripeFooter =
      google::protobuf::Arena::CreateMessage<proto::StripeFooter>(&arena);
  std::vector<std::tuple<uint64_t, StreamKind, uint64_t>> ss{
      std::make_tuple(1, StreamKind::StreamKind_ROW_INDEX, 100),
      std::make_tuple(2, StreamKind::StreamKind_ROW_INDEX, 100),
      std::make_tuple(1, StreamKind::StreamKind_PRESENT, 200),
      std::make_tuple(2, StreamKind::StreamKind_PRESENT, 200),
      std::make_tuple(1, StreamKind::StreamKind_DATA, 5000000),
      std::make_tuple(2, StreamKind::StreamKind_DATA, 1000000)};
  for (const auto& s : ss) {
    auto&& stream = stripeFooter->add_streams();
    stream->set_node(std::get<0>(s));
    stream->set_kind(static_cast<proto::Stream_Kind>(std::get<1>(s)));
    stream->set_length(std::get<2>(s));
  }
  std::vector<Region> expected{{100, 500}, {5000600, 1000000}};
  StripeReaderBase stripeReader{readerBase, stripeFooter};
  auto streams = createAndLoadStripeStreams(stripeReader, cs);
  auto const& actual = isPtr->getReads();
  EXPECT_EQ(actual.size(), expected.size());
  for (uint64_t i = 0; i < actual.size(); ++i) {
    EXPECT_EQ(actual[i].offset, expected[i].offset);
    EXPECT_EQ(actual[i].length, expected[i].length);
  }
}

TEST(StripeStream, filterSequences) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  google::protobuf::Arena arena;
  auto footer = google::protobuf::Arena::CreateMessage<proto::Footer>(&arena);
  footer->set_rowindexstride(100);
  auto type = HiveTypeParser().parse("struct<a:map<int,float>>");
  ProtoUtils::writeType(*type, *footer);
  auto is = std::make_unique<RecordingInputStream>();
  auto isPtr = is.get();
  auto readerBase = std::make_shared<ReaderBase>(
      pool,
      std::move(is),
      std::make_unique<proto::PostScript>(),
      footer,
      nullptr);

  // mock a filter that we only need one node and one sequence
  ColumnSelector cs{readerBase->getSchema(), std::vector<std::string>{"a#[1]"}};
  const auto& node = cs.getNode(1);
  auto seqFilter = std::make_shared<std::unordered_set<size_t>>();
  seqFilter->insert(1);
  node->setSequenceFilter(seqFilter);

  // mock the input stream data to verify our plan
  // only covered the filtered streams
  auto stripeFooter =
      google::protobuf::Arena::CreateMessage<proto::StripeFooter>(&arena);
  std::vector<std::tuple<uint64_t, StreamKind, uint64_t, uint64_t>> ss{
      std::make_tuple(1, StreamKind::StreamKind_ROW_INDEX, 100, 0),
      std::make_tuple(2, StreamKind::StreamKind_ROW_INDEX, 100, 0),
      std::make_tuple(1, StreamKind::StreamKind_PRESENT, 200, 0),
      std::make_tuple(1, StreamKind::StreamKind_PRESENT, 200, 0),
      std::make_tuple(1, StreamKind::StreamKind_DATA, 5000000, 1),
      std::make_tuple(1, StreamKind::StreamKind_DATA, 3000000, 2),
      std::make_tuple(3, StreamKind::StreamKind_DATA, 1000000, 1)};

  for (const auto& s : ss) {
    auto&& stream = stripeFooter->add_streams();
    stream->set_node(std::get<0>(s));
    stream->set_kind(static_cast<proto::Stream_Kind>(std::get<1>(s)));
    stream->set_length(std::get<2>(s));
    stream->set_sequence(std::get<3>(s));
  }

  // filter by sequence 1
  std::vector<Region> expected{{600, 5000000}, {8000600, 1000000}};
  StripeReaderBase stripeReader{readerBase, stripeFooter};
  auto streams = createAndLoadStripeStreams(stripeReader, cs);
  auto const& actual = isPtr->getReads();
  EXPECT_EQ(actual.size(), expected.size());
  for (uint64_t i = 0; i < actual.size(); ++i) {
    EXPECT_EQ(actual[i].offset, expected[i].offset);
    EXPECT_EQ(actual[i].length, expected[i].length);
  }
}

TEST(StripeStream, zeroLength) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  google::protobuf::Arena arena;
  auto footer = google::protobuf::Arena::CreateMessage<proto::Footer>(&arena);
  footer->set_rowindexstride(100);
  auto type = HiveTypeParser().parse("struct<a:int>");
  ProtoUtils::writeType(*type, *footer);
  auto ps = std::make_unique<proto::PostScript>();
  ps->set_compressionblocksize(1024);
  ps->set_compression(proto::CompressionKind::ZSTD);
  auto is = std::make_unique<RecordingInputStream>();
  auto isPtr = is.get();
  auto readerBase = std::make_shared<ReaderBase>(
      pool, std::move(is), std::move(ps), footer, nullptr);

  auto stripeFooter =
      google::protobuf::Arena::CreateMessage<proto::StripeFooter>(&arena);
  std::vector<std::tuple<uint64_t, StreamKind, uint64_t>> ss{
      std::make_tuple(0, StreamKind::StreamKind_ROW_INDEX, 0),
      std::make_tuple(1, StreamKind::StreamKind_ROW_INDEX, 0),
      std::make_tuple(1, StreamKind::StreamKind_DATA, 0)};
  for (const auto& s : ss) {
    auto&& stream = stripeFooter->add_streams();
    stream->set_node(std::get<0>(s));
    stream->set_kind(static_cast<proto::Stream_Kind>(std::get<1>(s)));
    stream->set_length(std::get<2>(s));
  }
  StripeReaderBase stripeReader{readerBase, stripeFooter};
  TestProvider indexProvider;
  ColumnSelector cs{std::dynamic_pointer_cast<const RowType>(type)};
  StripeStreamsImpl streams{
      stripeReader, cs, RowReaderOptions{}, 0, indexProvider, 0};
  streams.loadReadPlan();
  auto const& actual = isPtr->getReads();
  EXPECT_EQ(actual.size(), 0);

  for (const auto& s : ss) {
    auto id = EncodingKey(std::get<0>(s))
                  .forKind(static_cast<proto::Stream_Kind>(std::get<1>(s)));
    auto stream = streams.getStream(id, true);
    EXPECT_NE(stream, nullptr);
    const void* buf = nullptr;
    int32_t size = 1;
    EXPECT_FALSE(stream->Next(&buf, &size));
    proto::RowIndex rowIndex;
    EXPECT_EQ(stream->positionSize(), 2);
  }
}

TEST(StripeStream, planReadsIndex) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  google::protobuf::Arena arena;

  // build ps
  auto ps = std::make_unique<proto::PostScript>();
  ps->set_cachemode(proto::StripeCacheMode::INDEX);
  ps->set_compression(proto::CompressionKind::NONE);

  // build index
  proto::RowIndex index;
  index.add_entry()->add_positions(123);
  std::stringstream buffer;
  index.SerializeToOstream(&buffer);
  uint64_t length = buffer.tellp();
  index.SerializeToOstream(&buffer);

  // build footer
  auto footer = google::protobuf::Arena::CreateMessage<proto::Footer>(&arena);
  footer->set_rowindexstride(100);
  footer->add_stripecacheoffsets(0);
  footer->add_stripecacheoffsets(buffer.tellp());
  auto type = HiveTypeParser().parse("struct<a:int>");
  ProtoUtils::writeType(*type, *footer);

  // build cache
  std::string str(buffer.str());
  auto cacheBuffer = std::make_shared<DataBuffer<char>>(pool, str.size());
  memcpy(cacheBuffer->data(), str.data(), str.size());
  auto cache = std::make_unique<StripeMetadataCache>(
      *ps, *footer, std::move(cacheBuffer));

  auto is = std::make_unique<RecordingInputStream>();
  auto isPtr = is.get();
  auto readerBase = std::make_shared<ReaderBase>(
      pool, std::move(is), std::move(ps), footer, std::move(cache));

  auto stripeFooter =
      google::protobuf::Arena::CreateMessage<proto::StripeFooter>(&arena);
  std::vector<std::tuple<uint64_t, StreamKind, uint64_t>> ss{
      std::make_tuple(0, StreamKind::StreamKind_ROW_INDEX, length),
      std::make_tuple(1, StreamKind::StreamKind_ROW_INDEX, length),
      std::make_tuple(1, StreamKind::StreamKind_PRESENT, 200),
      std::make_tuple(1, StreamKind::StreamKind_DATA, 1000000)};
  for (const auto& s : ss) {
    auto&& stream = stripeFooter->add_streams();
    stream->set_node(std::get<0>(s));
    stream->set_kind(static_cast<proto::Stream_Kind>(std::get<1>(s)));
    stream->set_length(std::get<2>(s));
  }
  StripeReaderBase stripeReader{readerBase, stripeFooter};
  ColumnSelector cs{std::dynamic_pointer_cast<const RowType>(type)};
  auto streams = createAndLoadStripeStreams(stripeReader, cs);
  auto const& actual = isPtr->getReads();
  EXPECT_EQ(actual.size(), 1);
  EXPECT_EQ(actual[0].offset, length * 2);
  EXPECT_EQ(actual[0].length, 1000200);

  EXPECT_EQ(
      ProtoUtils::readProto<proto::RowIndex>(
          streams.getStream(
              EncodingKey(0).forKind(proto::Stream_Kind_ROW_INDEX), true))
          ->entry(0)
          .positions(0),
      123);
  EXPECT_EQ(
      ProtoUtils::readProto<proto::RowIndex>(
          streams.getStream(
              EncodingKey(1).forKind(proto::Stream_Kind_ROW_INDEX), true))
          ->entry(0)
          .positions(0),
      123);
}

void addEncryptionGroup(
    proto::Encryption& enc,
    const std::vector<uint32_t>& nodes) {
  auto group = enc.add_encryptiongroups();
  for (auto& n : nodes) {
    group->add_nodes(n);
    group->add_statistics();
  }
}

template <typename T>
void addNode(T& t, uint32_t node, uint32_t offset = 0) {
  auto enc = t.add_encoding();
  enc->set_kind(proto::ColumnEncoding_Kind_DIRECT);
  enc->set_node(node);
  enc->set_dictionarysize(node + 1);

  auto stream = t.add_streams();
  stream->set_kind(proto::Stream_Kind_DATA);
  stream->set_node(node);
  stream->set_length(node + 2);
  if (offset > 0) {
    stream->set_offset(offset);
  }
}

TEST(StripeStream, readEncryptedStreams) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  google::protobuf::Arena arena;
  auto ps = std::make_unique<proto::PostScript>();
  ps->set_compression(proto::CompressionKind::ZSTD);
  ps->set_compressionblocksize(256 * 1024);
  auto footer = google::protobuf::Arena::CreateMessage<proto::Footer>(&arena);
  // a: not encrypted, projected
  // encryption group 1: b, c. projected b.
  // group 2: d. projected d.
  // group 3: e. not projected
  auto type = HiveTypeParser().parse("struct<a:int,b:int,c:int,d:int,e:int>");
  ProtoUtils::writeType(*type, *footer);

  auto enc = footer->mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  addEncryptionGroup(*enc, {2, 3});
  addEncryptionGroup(*enc, {4});
  addEncryptionGroup(*enc, {5});

  auto stripe = footer->add_stripes();
  for (auto i = 0; i < 3; ++i) {
    *stripe->add_keymetadata() = folly::to<std::string>("key", i);
  }
  TestDecrypterFactory factory;
  auto handler = DecryptionHandler::create(*footer, &factory);
  TestEncrypter encrypter;

  ProtoWriter pw{pool};
  auto stripeFooter =
      google::protobuf::Arena::CreateMessage<proto::StripeFooter>(&arena);
  addNode(*stripeFooter, 1);
  proto::StripeEncryptionGroup group1;
  addNode(group1, 2, 100);
  addNode(group1, 3);
  encrypter.setKey("key0");
  pw.writeProto(*stripeFooter->add_encryptiongroups(), group1, encrypter);
  proto::StripeEncryptionGroup group2;
  addNode(group2, 4, 200);
  encrypter.setKey("key1");
  pw.writeProto(*stripeFooter->add_encryptiongroups(), group2, encrypter);
  // add empty string to group3, so decoding will fail if read
  *stripeFooter->add_encryptiongroups() = "";

  auto readerBase = std::make_shared<ReaderBase>(
      pool,
      std::make_unique<MemoryInputStream>(nullptr, 0),
      std::move(ps),
      footer,
      nullptr,
      std::move(handler));
  auto stripeReader =
      std::make_unique<StripeReaderBase>(readerBase, stripeFooter);
  ColumnSelector selector{readerBase->getSchema(), {1, 2, 4}, true};
  TestProvider provider;
  StripeStreamsImpl streams{
      *stripeReader, selector, RowReaderOptions{}, 0, provider, 0};

  // make sure projected columns exist
  std::unordered_set<uint32_t> existed{1, 2, 4};
  for (uint32_t node = 1; node < 6; ++node) {
    EncodingKey ek{node};
    auto stream = streams.getStream(
        StreamIdentifier{node, 0, 0, StreamKind::StreamKind_DATA}, false);
    if (existed.count(node)) {
      ASSERT_EQ(streams.getEncoding(ek).dictionarysize(), node + 1);
      ASSERT_NE(stream, nullptr);
    } else {
      ASSERT_THROW(streams.getEncoding(ek), exception::LoggedException);
      ASSERT_EQ(stream, nullptr);
    }
  }
}

TEST(StripeStream, schemaMismatch) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  google::protobuf::Arena arena;
  auto ps = std::make_unique<proto::PostScript>();
  ps->set_compression(proto::CompressionKind::ZSTD);
  ps->set_compressionblocksize(256 * 1024);
  auto footer = google::protobuf::Arena::CreateMessage<proto::Footer>(&arena);
  // a: not encrypted, has schema change
  // b: encrypted
  // c: not encrypted
  auto type = HiveTypeParser().parse("struct<a:struct<a:int>,b:int,c:int>");
  ProtoUtils::writeType(*type, *footer);

  auto enc = footer->mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  addEncryptionGroup(*enc, {3});

  auto stripe = footer->add_stripes();
  *stripe->add_keymetadata() = "key";
  TestDecrypterFactory factory;
  auto handler = DecryptionHandler::create(*footer, &factory);
  TestEncrypter encrypter;

  ProtoWriter pw{pool};
  auto stripeFooter =
      google::protobuf::Arena::CreateMessage<proto::StripeFooter>(&arena);
  addNode(*stripeFooter, 1);
  addNode(*stripeFooter, 2);
  addNode(*stripeFooter, 4);
  proto::StripeEncryptionGroup group;
  addNode(group, 3, 100);
  encrypter.setKey("key");
  pw.writeProto(*stripeFooter->add_encryptiongroups(), group, encrypter);

  auto readerBase = std::make_shared<ReaderBase>(
      pool,
      std::make_unique<MemoryInputStream>(nullptr, 0),
      std::move(ps),
      footer,
      nullptr,
      std::move(handler));
  auto stripeReader =
      std::make_unique<StripeReaderBase>(readerBase, stripeFooter);
  // now, we read the file as if schema has changed
  auto schema =
      HiveTypeParser().parse("struct<a:struct<a1:int,a2:int>,b:int,c:int>");
  // only project b and c. Node id of b and c in the new schema is 4, 5
  ColumnSelector selector{
      std::dynamic_pointer_cast<const RowType>(schema), {4, 5}, true};
  TestProvider provider;
  StripeStreamsImpl streams{
      *stripeReader, selector, RowReaderOptions{}, 0, provider, 0};

  // make sure all columns exist. Node id of b and c in the file is 3, 4
  for (uint32_t node = 3; node < 4; ++node) {
    EncodingKey ek{node};
    auto stream = streams.getStream(
        StreamIdentifier{node, 0, 0, StreamKind::StreamKind_DATA}, false);
    ASSERT_EQ(streams.getEncoding(ek).dictionarysize(), node + 1);
    ASSERT_NE(stream, nullptr);
  }
}

namespace {
// A class to allow testing StripeStreamsBase functionality with minimally
// needed methods implemented.
class TestStripeStreams : public StripeStreamsBase {
 public:
  explicit TestStripeStreams()
      : StripeStreamsBase{
            &facebook::velox::memory::getProcessDefaultMemoryManager()
                 .getRoot()} {}

  const proto::ColumnEncoding& getEncoding(
      const EncodingKey& ek) const override {
    return *getEncodingProxy(ek.node, ek.sequence);
  }

  std::unique_ptr<SeekableInputStream> getStream(
      const StreamIdentifier& si,
      bool throwIfNotFound) const override {
    return std::unique_ptr<SeekableInputStream>(getStreamProxy(
        si.node,
        si.sequence,
        static_cast<proto::Stream_Kind>(si.kind),
        throwIfNotFound));
  }

  const facebook::velox::dwio::common::ColumnSelector& getColumnSelector()
      const override {
    VELOX_UNSUPPORTED();
  }

  const facebook::velox::dwio::common::RowReaderOptions& getRowReaderOptions()
      const override {
    VELOX_UNSUPPORTED();
  }

  const StrideIndexProvider& getStrideIndexProvider() const override {
    VELOX_UNSUPPORTED();
  }

  bool getUseVInts(const StreamIdentifier& /* unused */) const override {
    return true; // current tests all expect results from using vints
  }

  uint32_t rowsPerRowGroup() const override {
    VELOX_UNSUPPORTED();
  }

  MOCK_CONST_METHOD2(
      getEncodingProxy,
      proto::ColumnEncoding*(uint32_t, uint32_t));
  MOCK_CONST_METHOD2(
      visitStreamsOfNode,
      uint32_t(uint32_t, std::function<void(const StreamInformation&)>));
  MOCK_CONST_METHOD4(
      getStreamProxy,
      SeekableInputStream*(uint32_t, uint32_t, proto::Stream_Kind, bool));
};

proto::ColumnEncoding genColumnEncoding(
    uint32_t node,
    uint32_t sequence,
    const proto::ColumnEncoding_Kind& kind,
    size_t dictionarySize) {
  proto::ColumnEncoding encoding;
  encoding.set_node(node);
  encoding.set_sequence(sequence);
  encoding.set_kind(kind);
  encoding.set_dictionarysize(dictionarySize);
  return encoding;
}
} // namespace

TEST(StripeStream, shareDictionary) {
  TestStripeStreams ss;

  auto nonSharedDictionaryEncoding =
      genColumnEncoding(1, 0, proto::ColumnEncoding_Kind_DICTIONARY, 100);
  EXPECT_CALL(ss, getEncodingProxy(1, 0))
      .WillOnce(Return(&nonSharedDictionaryEncoding));
  char nonSharedDictBuffer[1024];
  size_t nonSharedDictBufferSize = writeRange(nonSharedDictBuffer, 0, 100);
  EXPECT_CALL(ss, getStreamProxy(1, 0, proto::Stream_Kind_DICTIONARY_DATA, _))
      .WillOnce(InvokeWithoutArgs([&]() {
        return new SeekableArrayInputStream(
            nonSharedDictBuffer, nonSharedDictBufferSize);
      }));

  auto sharedDictionaryEncoding2_2 =
      genColumnEncoding(2, 2, proto::ColumnEncoding_Kind_DICTIONARY, 100);
  EXPECT_CALL(ss, getEncodingProxy(2, 2))
      .WillOnce(Return(&sharedDictionaryEncoding2_2));
  auto sharedDictionaryEncoding2_3 =
      genColumnEncoding(2, 3, proto::ColumnEncoding_Kind_DICTIONARY, 100);
  EXPECT_CALL(ss, getEncodingProxy(2, 3))
      .WillOnce(Return(&sharedDictionaryEncoding2_3));
  auto sharedDictionaryEncoding2_5 =
      genColumnEncoding(2, 5, proto::ColumnEncoding_Kind_DICTIONARY, 100);
  EXPECT_CALL(ss, getEncodingProxy(2, 5))
      .WillOnce(Return(&sharedDictionaryEncoding2_5));
  auto sharedDictionaryEncoding2_8 =
      genColumnEncoding(2, 8, proto::ColumnEncoding_Kind_DICTIONARY, 100);
  EXPECT_CALL(ss, getEncodingProxy(2, 8))
      .WillOnce(Return(&sharedDictionaryEncoding2_8));
  auto sharedDictionaryEncoding2_13 =
      genColumnEncoding(2, 13, proto::ColumnEncoding_Kind_DICTIONARY, 100);
  EXPECT_CALL(ss, getEncodingProxy(2, 13))
      .WillOnce(Return(&sharedDictionaryEncoding2_13));
  auto sharedDictionaryEncoding2_21 =
      genColumnEncoding(2, 21, proto::ColumnEncoding_Kind_DICTIONARY, 100);
  EXPECT_CALL(ss, getEncodingProxy(2, 21))
      .WillOnce(Return(&sharedDictionaryEncoding2_21));
  char sharedDictBuffer[2048];
  size_t sharedDictBufferSize = writeRange(sharedDictBuffer, 100, 200);
  EXPECT_CALL(ss, getStreamProxy(2, 0, proto::Stream_Kind_DICTIONARY_DATA, _))
      .WillRepeatedly(InvokeWithoutArgs([&]() {
        return new SeekableArrayInputStream(
            sharedDictBuffer, sharedDictBufferSize);
      }));
  EXPECT_CALL(
      ss, getStreamProxy(2, Not(0), proto::Stream_Kind_DICTIONARY_DATA, _))
      .WillRepeatedly(Return(nullptr));

  std::vector<std::function<facebook::velox::BufferPtr()>> dictInits{};
  dictInits.push_back(
      ss.getIntDictionaryInitializerForNode(EncodingKey{1, 0}, 8));
  dictInits.push_back(
      ss.getIntDictionaryInitializerForNode(EncodingKey{2, 2}, 16));
  dictInits.push_back(
      ss.getIntDictionaryInitializerForNode(EncodingKey{2, 3}, 4));
  dictInits.push_back(
      ss.getIntDictionaryInitializerForNode(EncodingKey{2, 5}, 16));
  dictInits.push_back(
      ss.getIntDictionaryInitializerForNode(EncodingKey{2, 8}, 8));
  dictInits.push_back(
      ss.getIntDictionaryInitializerForNode(EncodingKey{2, 13}, 4));
  dictInits.push_back(
      ss.getIntDictionaryInitializerForNode(EncodingKey{2, 21}, 16));

  auto dictCache = ss.getStripeDictionaryCache();
  // Maybe verify range is useful here.
  EXPECT_NO_THROW(dictCache->getIntDictionary({1, 0}));
  EXPECT_NO_THROW(dictCache->getIntDictionary({2, 0}));
  EXPECT_ANY_THROW(dictCache->getIntDictionary({2, 2}));
  EXPECT_ANY_THROW(dictCache->getIntDictionary({2, 3}));
  EXPECT_ANY_THROW(dictCache->getIntDictionary({2, 5}));
  EXPECT_ANY_THROW(dictCache->getIntDictionary({2, 8}));
  EXPECT_ANY_THROW(dictCache->getIntDictionary({2, 13}));
  EXPECT_ANY_THROW(dictCache->getIntDictionary({2, 21}));

  for (auto& dictInit : dictInits) {
    EXPECT_NO_THROW(dictInit());
  }
}
