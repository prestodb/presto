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
#include <velox/dwio/common/MemoryInputStream.h>
#include "velox/dwio/common/encryption/TestProvider.h"
#include "velox/dwio/dwrf/reader/StripeReaderBase.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwrf::encryption;
using namespace facebook::velox::memory;
using namespace facebook::velox::dwio::type::fbhive;

namespace facebook::velox::dwrf {

class StripeLoadKeysTest : public Test {
 protected:
  void SetUp() override {
    HiveTypeParser parser;
    auto type = parser.parse("struct<a:int>");
    auto footer =
        google::protobuf::Arena::CreateMessage<proto::Footer>(&arena_);
    ProtoUtils::writeType(*type, *footer);
    auto enc = footer->mutable_encryption();
    enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
    auto group = enc->add_encryptiongroups();
    group->add_nodes(1);
    group->add_statistics();
    group->set_keymetadata("footer");

    auto stripe = footer->add_stripes();
    *stripe->add_keymetadata() = "stripe0";

    stripe = footer->add_stripes();
    stripe = footer->add_stripes();
    *stripe->add_keymetadata() = "stripe2";

    stripe = footer->add_stripes();
    *stripe->add_keymetadata() = "stripe3.1";
    *stripe->add_keymetadata() = "stripe3.2";

    auto stripeFooter =
        google::protobuf::Arena::CreateMessage<proto::StripeFooter>(&arena_);
    stripeFooter->add_encryptiongroups();

    TestDecrypterFactory factory;
    auto handler = DecryptionHandler::create(*footer, &factory);
    scopedPool_ = getDefaultScopedMemoryPool();

    reader_ = std::make_unique<ReaderBase>(
        scopedPool_->getPool(),
        std::make_unique<MemoryInputStream>(nullptr, 0),
        nullptr,
        footer,
        nullptr,
        std::move(handler));
    stripeReader_ =
        std::make_unique<StripeReaderBase>(reader_, std::move(stripeFooter));
    enc_ = const_cast<TestEncryption*>(
        std::addressof(dynamic_cast<const TestEncryption&>(
            stripeReader_->getDecryptionHandler().getEncryptionProviderByIndex(
                0))));
  }

  void runTest(std::optional<uint32_t> prevIndex, uint32_t newIndex) {
    stripeReader_->lastStripeIndex_ = prevIndex;
    stripeReader_->loadEncryptionKeys(newIndex);
  }

  google::protobuf::Arena arena_;
  std::shared_ptr<ReaderBase> reader_;
  std::unique_ptr<StripeReaderBase> stripeReader_;
  TestEncryption* enc_;
  std::unique_ptr<ScopedMemoryPool> scopedPool_;
};

} // namespace facebook::velox::dwrf

TEST_F(StripeLoadKeysTest, FirstStripeHasKey) {
  ASSERT_EQ(enc_->getKey(), "footer");

  runTest(std::optional<uint32_t>(), 0);
  ASSERT_EQ(enc_->getKey(), "stripe0");
}

TEST_F(StripeLoadKeysTest, FirstStripeNoKey) {
  ASSERT_EQ(enc_->getKey(), "footer");

  runTest(std::optional<uint32_t>(), 1);
  ASSERT_EQ(enc_->getKey(), "stripe0");
}

TEST_F(StripeLoadKeysTest, SeekToStripeNoKey) {
  ASSERT_EQ(enc_->getKey(), "footer");

  runTest(2, 1);
  ASSERT_EQ(enc_->getKey(), "stripe0");
}

TEST_F(StripeLoadKeysTest, ContinueSameKey) {
  ASSERT_EQ(enc_->getKey(), "footer");

  runTest(0, 1);
  ASSERT_EQ(enc_->getKey(), "footer");
}

TEST_F(StripeLoadKeysTest, ContinueDifferentKey) {
  ASSERT_EQ(enc_->getKey(), "footer");

  runTest(1, 2);
  ASSERT_EQ(enc_->getKey(), "stripe2");
}

TEST_F(StripeLoadKeysTest, KeyMismatch) {
  ASSERT_THROW(
      runTest(std::optional<uint32_t>(), 3), exception::LoggedException);
}
