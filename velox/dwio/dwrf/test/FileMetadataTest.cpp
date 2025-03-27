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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include "velox/dwio/dwrf/common/FileMetadata.h"

using namespace ::testing;
namespace facebook::velox::dwrf {

class FileMetadataTest : public ::testing::Test {
 protected:
  std::shared_ptr<proto::StripeFooter> dwrfStripeFooter_ =
      std::make_shared<proto::StripeFooter>();
  StripeFooterWrapper dwrfStripeFooterWrapper_ =
      StripeFooterWrapper(dwrfStripeFooter_);

  std::shared_ptr<proto::orc::StripeFooter> orcStripeFooter_ =
      std::make_shared<proto::orc::StripeFooter>();

  StripeFooterWrapper orcStripeFooterWrapper_ =
      StripeFooterWrapper(orcStripeFooter_);
};

TEST_F(FileMetadataTest, StripeFooterWrapper_GetFormat_CorrectFormat) {
  EXPECT_EQ(dwrfStripeFooterWrapper_.format(), DwrfFormat::kDwrf);
  EXPECT_EQ(orcStripeFooterWrapper_.format(), DwrfFormat::kOrc);
}

TEST_F(
    FileMetadataTest,
    StripeFooterWrapper_GetStripeFooter_ReturnsProtoTypes) {
  EXPECT_EQ(
      &dwrfStripeFooterWrapper_.getStripeFooterDwrf(), dwrfStripeFooter_.get());
  EXPECT_ANY_THROW(dwrfStripeFooterWrapper_.getStripeFooterOrc());

  EXPECT_EQ(
      &orcStripeFooterWrapper_.getStripeFooterOrc(), orcStripeFooter_.get());
  EXPECT_ANY_THROW(orcStripeFooterWrapper_.getStripeFooterDwrf());
}

TEST_F(
    FileMetadataTest,
    StripeFooterWrapper_GetStreamData_ReturnsCorrispondingProtoInfo) {
  // 2 streams with incrementing length for validation
  dwrfStripeFooter_->add_streams()->set_length(1);
  dwrfStripeFooter_->add_streams()->set_length(2);

  // 3 streams with incrementing length for validation
  orcStripeFooter_->add_streams()->set_length(3);
  orcStripeFooter_->add_streams()->set_length(4);
  orcStripeFooter_->add_streams()->set_length(5);

  // DWRF
  // get size
  EXPECT_EQ(dwrfStripeFooterWrapper_.streamsSize(), 2);

  // all streams
  EXPECT_EQ(dwrfStripeFooterWrapper_.streamsDwrf().size(), 2);

  // access stream by index
  EXPECT_EQ(dwrfStripeFooterWrapper_.streamDwrf(0).length(), 1);
  EXPECT_EQ(dwrfStripeFooterWrapper_.streamDwrf(1).length(), 2);

  // ORC
  // get size
  EXPECT_EQ(orcStripeFooterWrapper_.streamsSize(), 3);

  // all streams
  EXPECT_EQ(orcStripeFooterWrapper_.streamsOrc().size(), 3);

  // access stream by index
  EXPECT_EQ(orcStripeFooterWrapper_.streamOrc(0).length(), 3);
  EXPECT_EQ(orcStripeFooterWrapper_.streamOrc(1).length(), 4);
  EXPECT_EQ(orcStripeFooterWrapper_.streamOrc(2).length(), 5);
}

TEST_F(
    FileMetadataTest,
    StripeFooterWrapper_GetEncodings_ReturnsCorrispondingProtoInfo) {
  // 2 encoding with incrementing node for validation
  dwrfStripeFooter_->add_encoding()->set_kind(
      proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY);
  dwrfStripeFooter_->add_encoding()->set_kind(
      proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_V2);

  // 3 encoding with incrementing node for validation
  orcStripeFooter_->add_columns()->set_kind(
      proto::orc::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT);
  orcStripeFooter_->add_columns()->set_kind(
      proto::orc::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_V2);
  orcStripeFooter_->add_columns()->set_kind(
      proto::orc::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY);

  // DWRF
  // get size
  EXPECT_EQ(dwrfStripeFooterWrapper_.columnEncodingSize(), 2);

  // all encoding
  EXPECT_EQ(dwrfStripeFooterWrapper_.columnEncodingsDwrf().size(), 2);

  // access encoding by index
  EXPECT_EQ(
      dwrfStripeFooterWrapper_.columnEncodingDwrf(0).kind(),
      proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY);
  EXPECT_EQ(
      dwrfStripeFooterWrapper_.columnEncodingDwrf(1).kind(),
      proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_V2);

  // ORC
  // get size
  EXPECT_EQ(orcStripeFooterWrapper_.columnEncodingSize(), 3);

  // all encoding
  EXPECT_EQ(orcStripeFooterWrapper_.columnEncodingsOrc().size(), 3);

  // access encoding by index
  EXPECT_EQ(
      orcStripeFooterWrapper_.columnEncodingOrc(0).kind(),
      proto::orc::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT);
  EXPECT_EQ(
      orcStripeFooterWrapper_.columnEncodingOrc(1).kind(),
      proto::orc::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_V2);
  EXPECT_EQ(
      orcStripeFooterWrapper_.columnEncodingOrc(2).kind(),
      proto::orc::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY);
}

TEST_F(
    FileMetadataTest,
    StripeFooterWrapper_GetEncryptionGroups_ReturnsCorrispondingProtoInfo) {
  // 2 encryption groups with incrementing group for validation
  dwrfStripeFooter_->add_encryptiongroups()->append("test_encryption_group_1");
  dwrfStripeFooter_->add_encryptiongroups()->append("test_encryption_group_2");

  // DWRF
  // get size
  EXPECT_EQ(dwrfStripeFooterWrapper_.encryptiongroupsSize(), 2);

  // access encryption groups by index
  EXPECT_EQ(
      dwrfStripeFooterWrapper_.encryptiongroupsDwrf(0),
      "test_encryption_group_1");
  EXPECT_EQ(
      dwrfStripeFooterWrapper_.encryptiongroupsDwrf(1),
      "test_encryption_group_2");

  // ORC
  // orc encryption groups are not supported
  // get size always zero
  EXPECT_EQ(orcStripeFooterWrapper_.encryptiongroupsSize(), 0);
}

} // namespace facebook::velox::dwrf
