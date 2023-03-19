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

#include <gtest/gtest.h>

#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/writer/WriterContext.h"

using namespace ::testing;

namespace facebook::velox::dwrf {
TEST(TestWriterContext, GetIntDictionaryEncoder) {
  auto config = std::make_shared<Config>();
  WriterContext context{
      config,
      memory::getProcessDefaultMemoryManager().getPool(
          "GetIntDictionaryEncoder")};

  EXPECT_EQ(0, context.dictEncoders_.size());
  auto& intEncoder_1_0 = context.getIntDictionaryEncoder<int32_t>(
      {1, 0}, *context.dictionaryPool_, *context.generalPool_);
  EXPECT_EQ(1, context.dictEncoders_.size());
  ASSERT_EQ(1, intEncoder_1_0.refCount_);

  auto& duplicateCallResult_1_0 = context.getIntDictionaryEncoder<int32_t>(
      {1, 0}, *context.dictionaryPool_, *context.generalPool_);
  EXPECT_EQ(1, context.dictEncoders_.size());
  EXPECT_EQ(&intEncoder_1_0, &duplicateCallResult_1_0);
  EXPECT_EQ(2, intEncoder_1_0.refCount_);

  for (size_t i = 0; i < 40; ++i) {
    context.getIntDictionaryEncoder<int32_t>(
        {1, 0}, *context.dictionaryPool_, *context.generalPool_);
  }
  EXPECT_EQ(42, intEncoder_1_0.refCount_);

  // Ignore the mismatched type request.
  context.getIntDictionaryEncoder<int64_t>(
      {1, 0}, *context.dictionaryPool_, *context.generalPool_);
  EXPECT_EQ(1, context.dictEncoders_.size());

  context.getIntDictionaryEncoder<int32_t>(
      {2, 0}, *context.dictionaryPool_, *context.generalPool_);
  EXPECT_EQ(2, context.dictEncoders_.size());
}

TEST(TestWriterContext, RemoveIntDictionaryEncoderForNode) {
  auto config = std::make_shared<Config>();
  config->set(Config::MAP_FLAT_DICT_SHARE, false);
  WriterContext context{
      config,
      memory::getProcessDefaultMemoryManager().getPool(
          "RemoveIntDictionaryEncoderForNode")};

  context.getIntDictionaryEncoder<int32_t>(
      {1, 1}, *context.dictionaryPool_, *context.generalPool_);
  context.getIntDictionaryEncoder<int32_t>(
      {1, 2}, *context.dictionaryPool_, *context.generalPool_);
  context.getIntDictionaryEncoder<int32_t>(
      {1, 4}, *context.dictionaryPool_, *context.generalPool_);
  context.getIntDictionaryEncoder<int32_t>(
      {1, 5}, *context.dictionaryPool_, *context.generalPool_);
  EXPECT_EQ(4, context.dictEncoders_.size());

  context.getIntDictionaryEncoder<int32_t>(
      {2, 0}, *context.dictionaryPool_, *context.generalPool_);
  context.getIntDictionaryEncoder<int32_t>(
      {3, 1}, *context.dictionaryPool_, *context.generalPool_);
  context.getIntDictionaryEncoder<int32_t>(
      {3, 3}, *context.dictionaryPool_, *context.generalPool_);
  EXPECT_EQ(7, context.dictEncoders_.size());

  context.removeAllIntDictionaryEncodersOnNode(
      [](uint32_t nodeId) { return nodeId == 1; });
  EXPECT_EQ(3, context.dictEncoders_.size());
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 1}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 2}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 4}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 5}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{2, 0}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{3, 1}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{3, 3}));

  context.removeAllIntDictionaryEncodersOnNode(
      [](uint32_t nodeId) { return nodeId == 3; });
  EXPECT_EQ(1, context.dictEncoders_.size());
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 1}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 2}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 4}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{1, 5}));
  EXPECT_EQ(1, context.dictEncoders_.count(EncodingKey{2, 0}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{3, 1}));
  EXPECT_EQ(0, context.dictEncoders_.count(EncodingKey{3, 3}));

  context.removeAllIntDictionaryEncodersOnNode(
      [](uint32_t nodeId) { return nodeId == 2; });
  EXPECT_EQ(0, context.dictEncoders_.size());
}

TEST(TestWriterContext, BuildPhysicalSizeAggregators) {
  auto config = std::make_shared<Config>();
  WriterContext context{
      config,
      memory::getProcessDefaultMemoryManager().getPool(
          "BuildPhysicalSizeAggregators")};
  auto type = ROW({
      {"array", ARRAY(REAL())},
      {"map", MAP(INTEGER(), DOUBLE())},
      {"row",
       ROW({
           {"a", REAL()},
           {"b", INTEGER()},
       })},
      {"nested",
       ARRAY(ROW({
           {"a", INTEGER()},
           {"b", MAP(REAL(), REAL())},
       }))},
  });
  auto typeWithId = velox::dwio::common::TypeWithId::create(type);
  context.buildPhysicalSizeAggregators(*typeWithId);
  std::vector<uint32_t> mapNodes{3, 12};
  for (size_t i = 0; i < 14; ++i) {
    EXPECT_NO_THROW(context.getPhysicalSizeAggregator(i));
  }
  for (const auto nodeId : mapNodes) {
    EXPECT_NO_THROW(dynamic_cast<MapPhysicalSizeAggregator&>(
        context.getPhysicalSizeAggregator(nodeId)));
  }
}
} // namespace facebook::velox::dwrf
