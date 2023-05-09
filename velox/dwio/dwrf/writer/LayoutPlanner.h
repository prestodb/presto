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

#include <folly/container/F14Map.h>
#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/common/Common.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/Encryption.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/writer/WriterContext.h"

namespace facebook::velox::dwrf {
using StreamList =
    std::vector<std::pair<const DwrfStreamIdentifier*, DataBufferHolder*>>;

StreamList getStreamList(WriterContext& context);

class EncodingIter {
 public:
  using value_type = const proto::ColumnEncoding;
  using reference = const proto::ColumnEncoding&;
  using pointer = const proto::ColumnEncoding*;
  using iterator_category = std::forward_iterator_tag;
  using difference_type = int64_t;

  static EncodingIter begin(
      const proto::StripeFooter& footer,
      const std::vector<proto::StripeEncryptionGroup>& encryptionGroups);

  static EncodingIter end(
      const proto::StripeFooter& footer,
      const std::vector<proto::StripeEncryptionGroup>& encryptionGroups);

  EncodingIter& operator++();
  EncodingIter operator++(int);
  bool operator==(const EncodingIter& other) const;
  bool operator!=(const EncodingIter& other) const;
  reference operator*() const;
  pointer operator->() const;

 private:
  EncodingIter(
      const proto::StripeFooter& footer,
      const std::vector<proto::StripeEncryptionGroup>& encryptionGroups,
      int32_t encryptionGroupIndex,
      google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
          current,
      google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
          currentEnd);

  void next();

  VELOX_FRIEND_TEST(TestEncodingIter, Ctor);
  VELOX_FRIEND_TEST(TestEncodingIter, EncodingIterBeginAndEnd);
  bool emptyEncryptionGroups() const;

  const proto::StripeFooter& footer_;
  const std::vector<proto::StripeEncryptionGroup>& encryptionGroups_;
  int32_t encryptionGroupIndex_{-1};
  google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
      current_;
  google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
      currentEnd_;
};

class EncodingContainer {
 public:
  virtual ~EncodingContainer() = default;
  virtual EncodingIter begin() const = 0;
  virtual EncodingIter end() const = 0;
};

class EncodingManager : public EncodingContainer {
 public:
  explicit EncodingManager(
      const encryption::EncryptionHandler& encryptionHandler);
  virtual ~EncodingManager() override = default;

  proto::ColumnEncoding& addEncodingToFooter(uint32_t nodeId);
  proto::Stream* addStreamToFooter(uint32_t nodeId, uint32_t& currentIndex);
  std::string* addEncryptionGroupToFooter();
  proto::StripeEncryptionGroup getEncryptionGroup(uint32_t i);
  const proto::StripeFooter& getFooter() const;

  EncodingIter begin() const override;
  EncodingIter end() const override;

 private:
  void initEncryptionGroups();

  const encryption::EncryptionHandler& encryptionHandler_;
  proto::StripeFooter footer_;
  std::vector<proto::StripeEncryptionGroup> encryptionGroups_;
};

class LayoutResult {
 public:
  LayoutResult(StreamList streams, size_t indexCount)
      : streams_{std::move(streams)}, indexCount_{indexCount} {}

  void iterateIndexStreams(
      const std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>&
          consumer) const;

  void iterateDataStreams(
      const std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>&
          consumer) const;

 private:
  StreamList streams_;
  size_t indexCount_;
};

class LayoutPlanner {
 public:
  explicit LayoutPlanner(const dwio::common::TypeWithId& schema);
  virtual ~LayoutPlanner() = default;

  virtual LayoutResult plan(
      const EncodingContainer& encoding,
      StreamList streamList) const;

 protected:
  static void sortBySize(
      StreamList::iterator begin,
      StreamList::iterator end,
      const folly::F14FastSet<uint32_t>& flatMapCols);

  // This method assumes flatmap can only be top level fields, which is enforced
  // through the way how flatmap is configured.
  static folly::F14FastSet<uint32_t> getFlatMapColumns(
      const EncodingContainer& encoding,
      const folly::F14FastMap<uint32_t, uint32_t>& nodeToColumnMap);

  folly::F14FastMap<uint32_t, uint32_t> nodeToColumnMap_;

  VELOX_FRIEND_TEST(LayoutPlannerTests, Basic);
};

} // namespace facebook::velox::dwrf
