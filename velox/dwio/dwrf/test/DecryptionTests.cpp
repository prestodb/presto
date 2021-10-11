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
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwrf::encryption;
using namespace facebook::velox::dwio::type::fbhive;

TEST(Decryption, NotEncrypted) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  TestDecrypterFactory factory;
  auto handler = DecryptionHandler::create(footer, &factory);
  ASSERT_FALSE(handler->isEncrypted());
}

TEST(Decryption, NoKeyProvider) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  TestDecrypterFactory factory;
  ASSERT_THROW(
      DecryptionHandler::create(footer, &factory), exception::LoggedException);
}

TEST(Decryption, EmptyGroup) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  TestDecrypterFactory factory;
  ASSERT_THROW(
      DecryptionHandler::create(footer, &factory), exception::LoggedException);
}

TEST(Decryption, EmptyNodes) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  auto group = enc->add_encryptiongroups();
  group->set_keymetadata("key");
  TestDecrypterFactory factory;
  ASSERT_THROW(
      DecryptionHandler::create(footer, &factory), exception::LoggedException);
}

TEST(Decryption, StatsMismatch) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  auto group = enc->add_encryptiongroups();
  group->set_keymetadata("key");
  group->add_nodes(1);
  group->add_nodes(2);
  group->add_statistics();
  TestDecrypterFactory factory;
  ASSERT_THROW(
      DecryptionHandler::create(footer, &factory), exception::LoggedException);
}

TEST(Decryption, KeyExistenceMismatch) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  for (size_t i = 0; i < 2; ++i) {
    auto group = enc->add_encryptiongroups();
    if (i == 0) {
      group->set_keymetadata("key");
    }
    group->add_nodes(i + 1);
    group->add_statistics();
  }
  TestDecrypterFactory factory;
  ASSERT_THROW(
      DecryptionHandler::create(footer, &factory), exception::LoggedException);
}

TEST(Decryption, ReuseStripeKey) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  auto group = enc->add_encryptiongroups();
  group->add_nodes(1);
  group->add_statistics();
  auto stripe = footer.add_stripes();
  *stripe->add_keymetadata() = "foobar";
  TestDecrypterFactory factory;
  auto handler = DecryptionHandler::create(footer, &factory);
  auto& td =
      dynamic_cast<const TestEncryption&>(handler->getEncryptionProvider(1));
  ASSERT_EQ(td.getKey(), "foobar");
}

TEST(Decryption, StripeKeyMismatch) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  auto group = enc->add_encryptiongroups();
  group->add_nodes(1);
  group->add_statistics();
  auto stripe = footer.add_stripes();
  *stripe->add_keymetadata() = "foobar";
  *stripe->add_keymetadata() = "foobar";
  TestDecrypterFactory factory;
  ASSERT_THROW(
      DecryptionHandler::create(footer, &factory), exception::LoggedException);
}

TEST(Decryption, Basic) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int,b:float,c:string,d:double>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  for (auto i = 0; i < 5; ++i) {
    if (i % 2 == 1) {
      auto group = enc->add_encryptiongroups();
      group->add_nodes(i);
      group->set_keymetadata(folly::to<std::string>("key", i));
      group->add_statistics();
    }
  }
  TestDecrypterFactory factory;
  auto handler = DecryptionHandler::create(footer, &factory);
  ASSERT_TRUE(handler->isEncrypted());
  for (auto i = 0; i < 5; ++i) {
    ASSERT_EQ(handler->isEncrypted(i), (i % 2 == 1));
    if (i % 2 == 1) {
      ASSERT_EQ(
          dynamic_cast<const TestEncryption&>(handler->getEncryptionProvider(i))
              .getKey(),
          folly::to<std::string>("key", i));
    }
  }
}

TEST(Decryption, NestedType) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<a:int,b:map<float,map<int,double>>,c:struct<a:string,b:int>,d:array<double>>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);

  uint32_t nodeCount = 12;
  std::vector<uint32_t> cols{1, 2, 7, 10, nodeCount};
  std::vector<uint32_t> encRoots{1, 2, 9, 10, nodeCount};
  std::unordered_map<uint32_t, uint32_t> nodeToRoot;
  for (auto i = 0; i < cols.size() - 1; ++i) {
    auto from = std::max(cols[i], encRoots[i]);
    auto to = std::min(cols[i + 1], encRoots[i + 1]);
    for (auto j = from; j < to; ++j) {
      nodeToRoot[j] = from;
    }
  }

  for (auto i = 0; i < encRoots.size() - 1; ++i) {
    auto group = enc->add_encryptiongroups();
    group->add_nodes(encRoots[i]);
    group->set_keymetadata(folly::to<std::string>("key", i));
    group->add_statistics();
  }
  TestDecrypterFactory factory;
  auto handler = DecryptionHandler::create(footer, &factory);
  ASSERT_TRUE(handler->isEncrypted());
  ASSERT_EQ(handler->getKeyProviderType(), EncryptionProvider::Unknown);
  ASSERT_EQ(handler->getEncryptionGroupCount(), 4);
  for (auto i = 0; i < nodeCount; ++i) {
    ASSERT_EQ(handler->isEncrypted(i), nodeToRoot.count(i) > 0);
    if (handler->isEncrypted(i)) {
      ASSERT_EQ(handler->getEncryptionRoot(i), nodeToRoot[i]);
    }
  }
}

TEST(Decryption, RootNode) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:int,b:int>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
  auto group = enc->add_encryptiongroups();
  group->add_nodes(0);
  group->set_keymetadata("key");
  group->add_statistics();
  TestDecrypterFactory factory;
  auto handler = DecryptionHandler::create(footer, &factory);
  ASSERT_EQ(handler->getEncryptionGroupCount(), 1);
}

TEST(Decryption, GroupOverlap) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<a:struct<a:float,b:double>>");
  proto::Footer footer;
  ProtoUtils::writeType(*type, footer);
  auto enc = footer.mutable_encryption();
  enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);

  auto group1 = enc->add_encryptiongroups();
  group1->add_nodes(1);
  group1->set_keymetadata("key");
  group1->add_statistics();

  auto group2 = enc->add_encryptiongroups();
  group2->add_nodes(3);
  group2->set_keymetadata("key");
  group2->add_statistics();

  TestDecrypterFactory factory;
  ASSERT_THROW(
      DecryptionHandler::create(footer, &factory), exception::LoggedException);
}
