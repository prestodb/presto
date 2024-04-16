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

#include "velox/dwio/common/encryption/TestProvider.h"
#include "velox/dwio/dwrf/reader/StripeReaderBase.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/type/fbhive/HiveTypeParser.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwrf::encryption;
using namespace facebook::velox::memory;
using namespace facebook::velox::type::fbhive;

namespace facebook::velox::dwrf {

class StripeLoadKeysTest : public Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    HiveTypeParser parser;
    auto type = parser.parse("struct<a:int>");
    footer_ = std::make_unique<proto::Footer>();
    ProtoUtils::writeType(*type, *footer_);
    auto enc = footer_->mutable_encryption();
    enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
    auto group = enc->add_encryptiongroups();
    group->add_nodes(1);
    group->add_statistics();
    group->set_keymetadata("footer");

    auto stripe = footer_->add_stripes();
    *stripe->add_keymetadata() = "stripe0";

    stripe = footer_->add_stripes();
    stripe = footer_->add_stripes();
    *stripe->add_keymetadata() = "stripe2";

    stripe = footer_->add_stripes();
    *stripe->add_keymetadata() = "stripe3.1";
    *stripe->add_keymetadata() = "stripe3.2";

    TestDecrypterFactory factory;
    auto handler =
        DecryptionHandler::create(FooterWrapper(footer_.get()), &factory);
    pool_ = memoryManager()->addLeafPool();

    reader_ = std::make_unique<ReaderBase>(
        *pool_,
        std::make_unique<BufferedInput>(
            std::make_shared<InMemoryReadFile>(std::string()), *pool_),
        nullptr,
        footer_.get(),
        nullptr,
        std::move(handler));

    stripeReader_ = std::make_unique<StripeReaderBase>(reader_);
  }

  void runTest(uint32_t index) {
    auto stripeFooter = std::make_unique<proto::StripeFooter>();
    stripeFooter->add_encryptiongroups();
    stripeFooter_ = std::move(stripeFooter);

    stripeInfo_ = std::make_unique<const StripeInformationWrapper>(
        FooterWrapper(footer_.get()).stripes(index));

    auto handler = std::make_unique<encryption::DecryptionHandler>(
        reader_->getDecryptionHandler());

    stripeReader_->loadEncryptionKeys(
        index, *stripeFooter_, *handler, *stripeInfo_);

    handler_ = std::move(handler);

    enc_ = const_cast<TestEncryption*>(
        std::addressof(dynamic_cast<const TestEncryption&>(
            handler_->getEncryptionProviderByIndex(0))));
  }

  std::unique_ptr<proto::Footer> footer_;
  std::shared_ptr<ReaderBase> reader_;
  std::unique_ptr<StripeReaderBase> stripeReader_;
  TestEncryption* enc_;
  std::shared_ptr<MemoryPool> pool_;
  std::unique_ptr<const proto::StripeFooter> stripeFooter_;
  std::unique_ptr<const encryption::DecryptionHandler> handler_;
  std::unique_ptr<const StripeInformationWrapper> stripeInfo_;
};

} // namespace facebook::velox::dwrf

TEST_F(StripeLoadKeysTest, FirstStripeHasKey) {
  runTest(0);
  ASSERT_EQ(enc_->getKey(), "stripe0");
}

TEST_F(StripeLoadKeysTest, SecondStripeNoKey) {
  runTest(1);
  ASSERT_EQ(enc_->getKey(), "stripe0");
}

TEST_F(StripeLoadKeysTest, ThirdStripeHasKey) {
  runTest(2);
  ASSERT_EQ(enc_->getKey(), "stripe2");
}

TEST_F(StripeLoadKeysTest, KeyMismatch) {
  EXPECT_THAT(
      [&]() { runTest(3); },
      Throws<facebook::velox::dwio::common::exception::LoggedException>(
          Property(
              &facebook::velox::dwio::common::exception::LoggedException::
                  failingExpression,
              HasSubstr("keys.size() == providers_.size()"))));
}
